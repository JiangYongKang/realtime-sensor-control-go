// sensord 本地演示：模拟设备 + 手动时钟驱动采样与安全控制。
package main

import (
	"fmt"
	"time"

	"github.com/highcumontoa/realtime-sensor-control-go/internal/clock"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/protocol"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/scheduler"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/sensor"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/system"
)

func main() {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	dev := &sensor.SimDevice{Base: 20, Amp: 5, Period: time.Minute}
	sys := system.New(clk, dev, []system.ChannelConfig{
		{Task: scheduler.Task{ID: "fast", Period: 100 * time.Millisecond, Priority: 0, Deadline: 20 * time.Millisecond},
			SafeValue: 0, MaxMisses: 3, Timeout: 500 * time.Millisecond},
		{Task: scheduler.Task{ID: "slow", Period: 500 * time.Millisecond, Priority: 1, Deadline: 50 * time.Millisecond},
			SafeValue: -1, MaxMisses: 2, Timeout: 2 * time.Second},
	})

	// 正常推进 1 秒。
	t := start
	sys.Advance(t) // 处理起始时刻的到期采样
	for i := 0; i < 10; i++ {
		t = t.Add(100 * time.Millisecond)
		sys.Advance(t)
	}
	fmt.Println("== 正常运行 1s ==")
	printSnap(sys)

	// 模拟调度停顿约 2 秒：fast 通道连续错过截止时间，应进入安全状态。
	t = t.Add(1950 * time.Millisecond)
	sys.Advance(t)
	fmt.Println("== 停顿约 2s 后（fast/slow 应进入 SAFE）==")
	printSnap(sys)

	// 恢复正常推进（先对齐回采样网格）。
	t = t.Add(50 * time.Millisecond)
	sys.Advance(t)
	for i := 0; i < 5; i++ {
		t = t.Add(100 * time.Millisecond)
		sys.Advance(t)
	}
	fmt.Println("== 恢复推进 0.5s 后 ==")
	printSnap(sys)

	// 总线：一帧损坏 + 一帧正常，失败不影响后续报文。
	good := protocol.Encode(protocol.Message{
		Version: protocol.CurrentVersion, Type: protocol.TypeSample,
		Fields: map[uint8][]byte{
			protocol.TagSeq:   protocol.U64(1),
			protocol.TagValue: protocol.F64(21.5),
		},
	})
	corrupt := append([]byte(nil), good...)
	corrupt[len(corrupt)-1] ^= 0xFF // 破坏 CRC
	msgs, errs := sys.FeedBus(append(corrupt, good...))
	fmt.Printf("== 复用总线：损坏帧+无通道正常帧 ==\n解码成功 %d 帧，错误 %v，未归属 %d\n",
		len(msgs), errs, sys.Snapshot().BusUnrouted)

	// 通道级总线：带通道字段的复用帧按通道落地。
	chanFrame := func(id string, seq uint64, v float64) []byte {
		return protocol.Encode(protocol.Message{
			Version: protocol.CurrentVersion, Type: protocol.TypeControl,
			Fields: map[uint8][]byte{
				protocol.TagSeq:     protocol.U64(seq),
				protocol.TagValue:   protocol.F64(v),
				protocol.TagChannel: []byte(id),
			},
		})
	}
	sys.FeedBus(chanFrame("fast", 1, 42.0))
	fmt.Println("== 复用总线带通道字段的控制帧落到 fast ==")
	printSnap(sys)

	// fast 通道专用总线上连续坏帧：只让 fast 降级，slow 不受影响。
	bad := func(b []byte) []byte {
		c := append([]byte(nil), b...)
		c[len(c)-1] ^= 0xFF
		return c
	}
	for i := 0; i < 3; i++ {
		sys.FeedChannelBus("fast", bad(chanFrame("fast", uint64(10+i), 1)))
	}
	fmt.Println("== fast 连续 3 帧坏报文：fast 切 SAFE，slow 不受影响 ==")
	printSnap(sys)

	// 一帧恢复后的新报文：fast 回到 OK，输出只来自新报文。
	sys.FeedChannelBus("fast", chanFrame("fast", 100, 7.5))
	fmt.Println("== fast 收到一帧恢复后的新报文：回到 OK ==")
	printSnap(sys)
}

func printSnap(sys *system.System) {
	snap := sys.Snapshot()
	for _, id := range []string{"fast", "slow"} {
		st := snap.Channels[id]
		fmt.Printf("  %-4s state=%-4s bus=%-4s bfails=%d in=%d/%d last=%.3f seq=%d busSeq=%d out=%.3f\n",
			id, st.State, st.BusState, st.BusConsecFails, st.BusAccepted, st.BusRejected,
			st.LastValue, st.LastSeq, st.BusLastSeq, st.Output)
	}
}
