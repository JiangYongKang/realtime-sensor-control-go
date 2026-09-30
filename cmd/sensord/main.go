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

	// 总线：fast 通道连续 3 帧坏报文 -> 当批内进入 SAFE；随后一帧更新
	// 序号的新报文立即恢复。slow 通道全程不受影响。
	chFrame := func(id string, seq uint64, v float64) []byte {
		return protocol.Encode(protocol.Message{
			Version: protocol.CurrentVersion, Type: protocol.TypeSample,
			Fields: map[uint8][]byte{
				protocol.TagSeq:     protocol.U64(seq),
				protocol.TagValue:   protocol.F64(v),
				protocol.TagChannel: []byte(id),
			},
		})
	}
	var stream []byte
	for i := 0; i < 3; i++ {
		f := chFrame("fast", uint64(i+1), 21.5)
		f[len(f)-1] ^= 0xFF
		stream = append(stream, f...)
	}
	stream = append(stream, chFrame("slow", 1, 5.0)...)    // slow 正常报文
	stream = append(stream, chFrame("fast", 100, 22.0)...) // fast 恢复（新序号）
	msgs, errs := sys.FeedBus(stream)
	fmt.Printf("== 总线：fast 连续 3 坏帧 -> SAFE -> 新报文恢复；slow 不受影响 ==\n解码成功 %d 帧，错误 %d 个\n", len(msgs), len(errs))
	printSnap(sys)
	snap := sys.Snapshot()
	fmt.Printf("  不可归属帧累计=%d\n", snap.UnroutableTotal)
}

func printSnap(sys *system.System) {
	snap := sys.Snapshot()
	for _, id := range []string{"fast", "slow"} {
		st := snap.Channels[id]
		fmt.Printf("  %-4s state=%-4s misses=%d late=%d bad=%d(streak %d) reorder=%d last=%.3f seq=%d out=%.3f\n",
			id, st.State, st.ConsecMisses, st.LateTotal, st.BadTotal, st.BadStreak,
			st.ReorderTotal, st.LastValue, st.LastSeq, st.Output)
	}
}
