// demo 在本地以可推进时钟演示实时采样与安全控制通道：
// 多路不同周期/优先级的采样、总线报文编解码、故障注入与安全降级、故障恢复。
package main

import (
	"fmt"

	"github.com/highcumontoa/realtime-sensor-control-go/clock"
	"github.com/highcumontoa/realtime-sensor-control-go/control"
	"github.com/highcumontoa/realtime-sensor-control-go/protocol"
	"github.com/highcumontoa/realtime-sensor-control-go/schedule"
)

// simDevice 模拟采样设备，可注入报文损坏故障。
type simDevice struct {
	seq     map[string]uint32
	corrupt map[string]bool
}

func newSimDevice() *simDevice {
	return &simDevice{seq: map[string]uint32{}, corrupt: map[string]bool{}}
}

func (d *simDevice) Sample(id string) ([]byte, error) {
	d.seq[id]++
	frame, err := protocol.Encode(protocol.Message{
		Version: 1,
		Kind:    1,
		Seq:     d.seq[id],
		Payload: []byte(fmt.Sprintf("%s=%d", id, 2000+d.seq[id]*7%500)),
	})
	if err != nil {
		return nil, err
	}
	if d.corrupt[id] {
		frame[len(frame)-1] ^= 0xFF // 模拟总线干扰：破坏 CRC
	}
	return frame, nil
}

func main() {
	clk := clock.NewManual(0)
	c := control.New(clk)
	// 两路采样：温度 100ms 高优先级，压力 200ms 低优先级。
	c.AddChannel("temp", 100, 1, schedule.MissMarkLate, 3)
	c.AddChannel("pressure", 200, 2, schedule.MissSkip, 2)
	dev := newSimDevice()

	step := func(label string) {
		outcomes := c.RunDue(dev)
		for _, o := range outcomes {
			status := "OK"
			switch {
			case o.Missed:
				status = "MISSED"
			case o.Late:
				status = "LATE"
			}
			if o.Err != nil {
				status = "ERR(" + o.Err.Error() + ")"
			}
			fmt.Printf("  t=%4d  %-8s slot@%4d  %s\n", o.At, o.TaskID, o.ScheduledAt, status)
		}
		snap := c.Snapshot()
		fmt.Printf("[%s] t=%d global=%s\n", label, snap.At, snap.State)
		for _, ch := range snap.Channels {
			fmt.Printf("    %-8s safety=%s stale=%v seq=%d payload=%q misses=%d\n",
				ch.ID, ch.Safety, ch.Stale, ch.LastSeq, ch.Payload, ch.Misses)
		}
	}

	fmt.Println("== 阶段 1：正常采样（每 100ms 运行一次调度）==")
	for i := 0; i < 4; i++ {
		clk.Advance(100)
		step(fmt.Sprintf("tick-%d", i+1))
	}

	fmt.Println("== 阶段 2：注入 temp 通道报文损坏 ==")
	dev.corrupt["temp"] = true
	for i := 0; i < 3; i++ {
		clk.Advance(100)
		step(fmt.Sprintf("fault-%d", i+1))
	}

	fmt.Println("== 阶段 3：故障恢复 ==")
	dev.corrupt["temp"] = false
	clk.Advance(100)
	step("recovered")

	fmt.Println("== 阶段 4：时钟大步推进导致错过截止时间 ==")
	clk.Advance(600)
	step("overrun")
}
