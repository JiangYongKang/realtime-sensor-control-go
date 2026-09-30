package system

import (
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-sensor-control-go/internal/clock"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/protocol"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/scheduler"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/sensor"
)

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newSystem() (*System, *clock.Manual) {
	clk := clock.NewManual(start)
	dev := &sensor.SimDevice{Base: 20, Amp: 5, Period: time.Minute}
	sys := New(clk, dev, []ChannelConfig{
		{Task: scheduler.Task{ID: "ch", Period: 100 * time.Millisecond, Priority: 0, Deadline: 20 * time.Millisecond},
			SafeValue: -1, MaxMisses: 3, Timeout: time.Second},
	})
	return sys, clk
}

func step(sys *System, t time.Time, n int, d time.Duration) time.Time {
	for i := 0; i < n; i++ {
		t = t.Add(d)
		sys.Advance(t)
	}
	return t
}

func TestNormalSampling(t *testing.T) {
	sys, _ := newSystem()
	sys.Advance(start)
	step(sys, start, 10, 100*time.Millisecond)
	st := sys.Snapshot().Channels["ch"]
	if st.State != "OK" || st.ConsecMisses != 0 || st.LateTotal != 0 {
		t.Fatalf("unexpected status: %+v", st)
	}
	if st.LastSeq != 10 {
		t.Fatalf("LastSeq=%d, want 10", st.LastSeq)
	}
}

func TestMissedDeadlinesTripSafeState(t *testing.T) {
	sys, _ := newSystem()
	sys.Advance(start)
	now := step(sys, start, 5, 100*time.Millisecond)
	// 停顿约 1 秒（避开网格对齐，使最后处理时刻超过截止时间）：
	// 约 10 个连续迟到样本，远超 MaxMisses=3，
	// 必须在一次 Advance 内（远小于约定时限）进入 SAFE。
	now = now.Add(time.Second + 50*time.Millisecond)
	sys.Advance(now)
	st := sys.Snapshot().Channels["ch"]
	if st.State != "SAFE" {
		t.Fatalf("state=%s, want SAFE", st.State)
	}
	if st.Output != -1 {
		t.Fatalf("output=%v, want safe value -1", st.Output)
	}
	if st.LateTotal == 0 {
		t.Fatal("迟到样本必须被计数，不得静默丢弃")
	}
}

func TestRecoveryDoesNotReuseStaleState(t *testing.T) {
	sys, _ := newSystem()
	sys.Advance(start)
	now := step(sys, start, 5, 100*time.Millisecond)
	// 进入 SAFE。
	now = now.Add(time.Second + 50*time.Millisecond)
	sys.Advance(now)
	before := sys.Snapshot().Channels["ch"]
	if before.State != "SAFE" {
		t.Fatalf("state=%s, want SAFE", before.State)
	}
	// 故障期间状态已被清除。
	if before.LastSeq != 0 {
		t.Fatalf("SAFE 期间仍保留故障前 LastSeq=%d", before.LastSeq)
	}
	// 恢复：对齐回采样网格推进。
	now = now.Add(50 * time.Millisecond)
	sys.Advance(now)
	after := sys.Snapshot().Channels["ch"]
	if after.State != "OK" {
		t.Fatalf("state=%s, want OK after recovery", after.State)
	}
	// 恢复后的值必须来自恢复后的新样本，序号大于故障前。
	if after.LastSeq <= 5 {
		t.Fatalf("恢复后 LastSeq=%d 沿用了故障期间状态", after.LastSeq)
	}
}

func TestLateSampleNotConsumedForControl(t *testing.T) {
	sys, _ := newSystem()
	sys.Advance(start)
	now := step(sys, start, 3, 100*time.Millisecond)
	okSt := sys.Snapshot().Channels["ch"]
	// 推进 50ms：ch 无新到期样本，输出保持不变。
	now = now.Add(50 * time.Millisecond)
	sys.Advance(now)
	// 推进到下一个网格点之后 30ms（超过 20ms 截止时间）：
	// 该样本迟到，不得更新控制输出。
	now = now.Add(80 * time.Millisecond) // 到达 grid+30ms
	sys.Advance(now)
	st := sys.Snapshot().Channels["ch"]
	if st.LateTotal != 1 {
		t.Fatalf("LateTotal=%d, want 1", st.LateTotal)
	}
	if st.LastSeq != okSt.LastSeq {
		t.Fatalf("迟到样本被消费: LastSeq %d -> %d", okSt.LastSeq, st.LastSeq)
	}
	if st.Output != okSt.Output {
		t.Fatalf("迟到样本改变了控制输出: %v -> %v", okSt.Output, st.Output)
	}
}

func TestBusFailureDoesNotAffectSubsequent(t *testing.T) {
	sys, _ := newSystem()
	good := protocol.Encode(protocol.Message{
		Version: protocol.CurrentVersion, Type: protocol.TypeSample,
		Fields: map[uint8][]byte{
			protocol.TagSeq:     protocol.U64(7),
			protocol.TagValue:   protocol.F64(21.5),
			protocol.TagChannel: []byte("ch"),
		},
	})
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 0xFF
	msgs, errs := sys.FeedBus(append(bad, good...))
	if len(msgs) != 1 || len(errs) != 1 {
		t.Fatalf("msgs=%d errs=%v", len(msgs), errs)
	}
	// 坏帧尽力归属到 ch，好帧紧随其后被接受：连续异常清零，通道不降级。
	st := sys.Snapshot().Channels["ch"]
	if st.State != "OK" || st.BadStreak != 0 || st.BadTotal != 1 {
		t.Fatalf("坏帧+好帧后状态错误: %+v", st)
	}
}

func TestConcurrentAccess(t *testing.T) {
	sys, _ := newSystem()
	sys.Advance(start)
	frame := protocol.Encode(protocol.Message{
		Version: protocol.CurrentVersion, Type: protocol.TypeControl,
		Fields: map[uint8][]byte{
			protocol.TagSeq:     protocol.U64(1),
			protocol.TagValue:   protocol.F64(1),
			protocol.TagChannel: []byte("ch"),
		},
	})
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 采样推进 goroutine。
	wg.Add(1)
	go func() {
		defer wg.Done()
		now := start
		for i := 0; i < 200; i++ {
			now = now.Add(10 * time.Millisecond)
			sys.Advance(now)
		}
		close(stop)
	}()
	// 并发状态读取。
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snap := sys.Snapshot()
					_ = snap.Channels["ch"]
				}
			}
		}()
	}
	// 并发总线输入。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			sys.FeedBus(frame)
		}
	}()
	wg.Wait()
}
