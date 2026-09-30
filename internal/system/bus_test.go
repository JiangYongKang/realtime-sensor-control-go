package system

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-sensor-control-go/internal/busguard"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/clock"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/protocol"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/scheduler"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/sensor"
)

// frame 构造一帧属于 ch、带 seq/value 的合法报文。
func frame(ch string, seq uint64, v float64) []byte {
	return protocol.Encode(protocol.Message{
		Version: protocol.CurrentVersion, Type: protocol.TypeSample,
		Fields: map[uint8][]byte{
			protocol.TagSeq:     protocol.U64(seq),
			protocol.TagValue:   protocol.F64(v),
			protocol.TagChannel: []byte(ch),
		},
	})
}

// corruptFrame 构造一帧属于 ch 的 CRC 损坏报文。
func corruptFrame(ch string, seq uint64) []byte {
	f := frame(ch, seq, 1)
	f[len(f)-1] ^= 0xFF
	return f
}

func newTwoChannelSystem() (*System, *clock.Manual) {
	clk := clock.NewManual(start)
	dev := &sensor.SimDevice{Base: 20, Amp: 5, Period: time.Minute}
	sys := New(clk, dev, []ChannelConfig{
		{Task: scheduler.Task{ID: "a", Period: 100 * time.Millisecond, Priority: 0, Deadline: 20 * time.Millisecond},
			SafeValue: -1, MaxMisses: 3, Timeout: time.Second,
			Bus: busguard.Config{MaxConsecutiveBad: 3, FaultWindow: 500 * time.Millisecond, ReorderWindow: 1}},
		{Task: scheduler.Task{ID: "b", Period: 100 * time.Millisecond, Priority: 1, Deadline: 20 * time.Millisecond},
			SafeValue: -2, MaxMisses: 3, Timeout: time.Second,
			Bus: busguard.Config{MaxConsecutiveBad: 3, FaultWindow: 500 * time.Millisecond, ReorderWindow: 1}},
	})
	return sys, clk
}

func TestConsecutiveBadBusFramesTripChannel(t *testing.T) {
	sys, _ := newTwoChannelSystem()
	// 2 帧坏帧：尚未降级。
	sys.FeedBus(append(corruptFrame("a", 1), corruptFrame("a", 2)...))
	if st := sys.Snapshot().Channels["a"]; st.BusSafe || st.State == "SAFE" {
		t.Fatalf("2 帧坏帧不应降级: %+v", st)
	}
	// 第 3 帧连续坏帧：立即 SAFE，输出切到安全值，无需等采样超时。
	sys.FeedBus(corruptFrame("a", 3))
	st := sys.Snapshot().Channels["a"]
	if st.State != "SAFE" || !st.BusSafe || st.Output != -1 {
		t.Fatalf("连续 3 帧坏帧应降级: %+v", st)
	}
	if st.LastSeq != 0 {
		t.Fatalf("降级后必须丢弃故障前状态, LastSeq=%d", st.LastSeq)
	}
}

func TestChannelRecoversOnlyWithFreshFrames(t *testing.T) {
	sys, _ := newTwoChannelSystem()
	sys.FeedBus(frame("a", 1, 10))
	sys.FeedBus(frame("a", 2, 11))
	// 触发降级，故障前序号峰值为 2。
	sys.FeedBus(append(append(corruptFrame("a", 3), corruptFrame("a", 4)...), corruptFrame("a", 5)...))
	if sys.Snapshot().Channels["a"].State != "SAFE" {
		t.Fatal("want SAFE")
	}
	// 序号 <=2 的“正常”报文是故障前旧报文，必须丢弃，不得恢复。
	sys.FeedBus(frame("a", 2, 99))
	if st := sys.Snapshot().Channels["a"]; st.State != "SAFE" || st.Output != -1 {
		t.Fatalf("故障前报文不得恢复通道: %+v", st)
	}
	// 序号更新的新报文：立即恢复且输出只来自恢复后。
	sys.FeedBus(frame("a", 3, 12))
	st := sys.Snapshot().Channels["a"]
	if st.State != "OK" || st.BusSafe || st.Output != 12 || st.LastSeq != 3 {
		t.Fatalf("新报文恢复错误: %+v", st)
	}
}

func TestBadAndGoodFramesInterleaved(t *testing.T) {
	sys, _ := newTwoChannelSystem()
	sys.FeedBus(frame("a", 1, 10))
	// 坏-好-坏-好：每帧正常报文都清零，永远到不了阈值。
	for i := uint64(2); i <= 5; i++ {
		sys.FeedBus(corruptFrame("a", i))
		sys.FeedBus(frame("a", i, float64(10+i)))
	}
	st := sys.Snapshot().Channels["a"]
	if st.State != "OK" || st.BadStreak != 0 || st.BadTotal != 4 {
		t.Fatalf("穿插场景状态错误: %+v", st)
	}
	if st.LastSeq != 5 {
		t.Fatalf("LastSeq=%d, want 5", st.LastSeq)
	}
}

func TestReorderWithinTolerance(t *testing.T) {
	sys, _ := newTwoChannelSystem()
	sys.FeedBus(frame("a", 10, 10))
	// 回退 1 在 ReorderWindow=1 内：乱序，不计数、不采用。
	sys.FeedBus(frame("a", 9, 99))
	st := sys.Snapshot().Channels["a"]
	if st.State != "OK" || st.BadStreak != 0 || st.ReorderTotal != 1 || st.Output != 10 {
		t.Fatalf("容忍窗口内乱序处理错误: %+v", st)
	}
	// 回退幅度 9（10->1）明显倒退：一次异常。
	sys.FeedBus(frame("a", 1, 1))
	if st := sys.Snapshot().Channels["a"]; st.BadStreak != 1 || st.BadTotal != 1 {
		t.Fatalf("明显倒退必须计异常: %+v", st)
	}
}

func TestDuplicateSeqCountedAsFault(t *testing.T) {
	sys, _ := newTwoChannelSystem()
	sys.FeedBus(frame("a", 5, 10))
	_, errs := sys.FeedBus(frame("a", 5, 11))
	if len(errs) != 1 || !errors.Is(errs[0], protocol.ErrInvalidField) {
		t.Fatalf("重复序号应返回语义错误: %v", errs)
	}
	if st := sys.Snapshot().Channels["a"]; st.BadStreak != 1 || st.Output != 10 {
		t.Fatalf("重复帧不得更新输出且要计数: %+v", st)
	}
}

func TestMultiChannelIsolation(t *testing.T) {
	sys, _ := newTwoChannelSystem()
	// 通道 a 持续坏帧直到降级；通道 b 全程正常。
	for i := 0; i < 5; i++ {
		sys.FeedBus(corruptFrame("a", uint64(i+1)))
	}
	for i := uint64(1); i <= 5; i++ {
		sys.FeedBus(frame("b", i, 20+float64(i)))
	}
	snap := sys.Snapshot()
	a, b := snap.Channels["a"], snap.Channels["b"]
	if a.State != "SAFE" || a.Output != -1 {
		t.Fatalf("a 应降级: %+v", a)
	}
	if b.State != "OK" || b.Output != 25 || b.LastSeq != 5 {
		t.Fatalf("b 不受 a 拖累: %+v", b)
	}
	// a 恢复，不影响 b。
	sys.FeedBus(frame("a", 100, 30))
	snap = sys.Snapshot()
	if snap.Channels["a"].State != "OK" || snap.Channels["b"].LastSeq != 5 {
		t.Fatalf("a 恢复后 b 状态被改变: a=%+v b=%+v", snap.Channels["a"], snap.Channels["b"])
	}
}

func TestUnroutableFramesDoNotTripAnyChannel(t *testing.T) {
	sys, _ := newTwoChannelSystem()
	// 无通道字段的坏帧、未知通道的坏帧，无论多少都不得影响已配置通道。
	for i := 0; i < 20; i++ {
		f := corruptFrame("ghost", 1) // CRC 坏但 TLV 可读出 ghost
		sys.FeedBus(f)
	}
	// 再发无通道字段的坏 magic 垃圾。
	sys.FeedBus(make([]byte, 4096))
	snap := sys.Snapshot()
	if snap.UnroutableTotal < 20 {
		t.Fatalf("UnroutableTotal=%d, want >=20", snap.UnroutableTotal)
	}
	for _, id := range []string{"a", "b"} {
		if st := snap.Channels[id]; st.BadTotal != 0 || st.State != "OK" {
			t.Fatalf("通道 %s 被不可归属帧拖累: %+v", id, st)
		}
	}
}

func TestFaultWindowTripsWithoutSampleTimeout(t *testing.T) {
	sys, _ := newTwoChannelSystem()
	// 采样正常推进（不到任何采样超时阈值），同时只来 1 帧坏报文，
	// 之后让时钟走过 FaultWindow=500ms：总线侧必须自行降级。
	sys.FeedBus(corruptFrame("a", 1))
	sys.Advance(start.Add(100 * time.Millisecond))
	sys.Advance(start.Add(200 * time.Millisecond))
	if st := sys.Snapshot().Channels["a"]; st.State == "SAFE" {
		t.Fatalf("未到 FaultWindow 不应降级: %+v", st)
	}
	sys.Advance(start.Add(600 * time.Millisecond))
	st := sys.Snapshot().Channels["a"]
	if st.State != "SAFE" || !st.BusSafe {
		t.Fatalf("异常持续到 FaultWindow 必须由总线侧自行降级: %+v", st)
	}
}

func TestConcurrentBusAndSnapshotsConsistent(t *testing.T) {
	sys, _ := newTwoChannelSystem()
	sys.Advance(start)
	stop := make(chan struct{})
	var writers, readers sync.WaitGroup
	run := func(wg *sync.WaitGroup, f func()) {
		wg.Add(1)
		go func() { defer wg.Done(); f() }()
	}
	// 通道 a：正常报文流。
	run(&writers, func() {
		for i := uint64(1); i <= 200; i++ {
			sys.FeedBus(frame("a", i, float64(i)))
		}
	})
	// 通道 b：坏帧突发后恢复，与 a 并发。
	run(&writers, func() {
		for i := 0; i < 3; i++ {
			sys.FeedBus(corruptFrame("b", uint64(i+1)))
		}
		for i := uint64(1); i <= 100; i++ {
			sys.FeedBus(frame("b", i+1000, float64(i)))
		}
	})
	// 并发采样推进。
	run(&writers, func() {
		now := start
		for i := 0; i < 100; i++ {
			now = now.Add(10 * time.Millisecond)
			sys.Advance(now)
		}
	})
	// 并发一致性快照读取：状态只能是 OK/SAFE，BadStreak 永远不超过阈值。
	for i := 0; i < 4; i++ {
		run(&readers, func() {
			for {
				select {
				case <-stop:
					return
				default:
					snap := sys.Snapshot()
					for _, id := range []string{"a", "b"} {
						st := snap.Channels[id]
						if st.State != "OK" && st.State != "SAFE" {
							t.Errorf("非法状态 %q", st.State)
						}
						if st.BadStreak > 3 {
							t.Errorf("BadStreak=%d 超过阈值", st.BadStreak)
						}
					}
				}
			}
		})
	}
	writers.Wait()
	close(stop)
	readers.Wait()
	// 最终两通道都应恢复 OK（b 的恢复报文序号 1001+ 全新）。
	final := sys.Snapshot()
	if final.Channels["a"].State != "OK" || final.Channels["b"].State != "OK" {
		t.Fatalf("最终状态: a=%+v b=%+v", final.Channels["a"], final.Channels["b"])
	}
}

func TestBusBacklogBounded(t *testing.T) {
	sys, _ := newTwoChannelSystem()
	// 喂入大段不构成完整帧的垃圾：解码器逐字节重新同步后缓冲立即清空
	// （协议层 MaxBufferedFrame 封顶），系统不得积压或失控。
	sys.FeedBus(make([]byte, 1<<20))
	msgs, errs := sys.FeedBus(frame("a", 1, 1))
	if len(msgs) != 1 || len(errs) != 0 {
		t.Fatalf("垃圾后正常帧必须仍被处理: msgs=%d errs=%v", len(msgs), errs)
	}
	if st := sys.Snapshot().Channels["a"]; st.Output != 1 || st.State != "OK" {
		t.Fatalf("垃圾不得影响通道: %+v", st)
	}
}
