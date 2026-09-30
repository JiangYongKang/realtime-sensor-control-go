package system

import (
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/realtime-sensor-control-go/internal/busfault"
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
		Fields: map[uint8][]byte{protocol.TagSeq: protocol.U64(7)},
	})
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 0xFF
	msgs, errs := sys.FeedBus(append(bad, good...))
	if len(msgs) != 1 || len(errs) != 1 {
		t.Fatalf("msgs=%d errs=%v", len(msgs), errs)
	}
}

func TestConcurrentAccess(t *testing.T) {
	sys, _ := newSystem()
	sys.Advance(start)
	frame := protocol.Encode(protocol.Message{
		Version: protocol.CurrentVersion, Type: protocol.TypeControl,
		Fields: map[uint8][]byte{protocol.TagSeq: protocol.U64(1)},
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

// ---- 总线接入与通道级安全 ----

func twoChannelSystem(busCfg busfault.Config) (*System, *clock.Manual) {
	clk := clock.NewManual(start)
	dev := &sensor.SimDevice{Base: 20, Amp: 5, Period: time.Minute}
	sys := New(clk, dev, []ChannelConfig{
		{Task: scheduler.Task{ID: "a", Period: time.Hour, Priority: 0, Deadline: 20 * time.Millisecond},
			SafeValue: -1, MaxMisses: 5, Timeout: time.Hour, Bus: busCfg},
		{Task: scheduler.Task{ID: "b", Period: time.Hour, Priority: 1, Deadline: 20 * time.Millisecond},
			SafeValue: -2, MaxMisses: 5, Timeout: time.Hour, Bus: busCfg},
	})
	return sys, clk
}

func ctrlFrame(channel string, seq uint64, v float64) []byte {
	f := map[uint8][]byte{
		protocol.TagSeq:   protocol.U64(seq),
		protocol.TagValue: protocol.F64(v),
	}
	if channel != "" {
		f[protocol.TagChannel] = []byte(channel)
	}
	return protocol.Encode(protocol.Message{
		Version: protocol.CurrentVersion, Type: protocol.TypeControl, Fields: f,
	})
}

func badCRC(frame []byte) []byte {
	b := append([]byte(nil), frame...)
	b[len(b)-1] ^= 0xFF
	return b
}

func TestBusBadFramesTripChannelSafe(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	// 通道 a 连续 3 帧坏 CRC（专用总线，错误可确定归因到 a）。
	for i := 0; i < 3; i++ {
		sys.FeedChannelBus("a", badCRC(ctrlFrame("a", uint64(i+1), 10)))
	}
	snap := sys.Snapshot()
	if snap.Channels["a"].BusState != "SAFE" || snap.Channels["a"].State != "SAFE" {
		t.Fatalf("a 应进入 SAFE: %+v", snap.Channels["a"])
	}
	if snap.Channels["a"].Output != -1 {
		t.Fatalf("a 输出应为安全值 -1, got %v", snap.Channels["a"].Output)
	}
	// 其他通道不受影响。
	if snap.Channels["b"].State != "OK" || snap.Channels["b"].Output != -2 {
		t.Fatalf("b 应保持 OK 与自身安全初值: %+v", snap.Channels["b"])
	}
	if snap.Channels["a"].BusRejected != 3 {
		t.Fatalf("BusRejected=%d, want 3", snap.Channels["a"].BusRejected)
	}
}

func TestBusRecoveryUsesFreshFrames(t *testing.T) {
	sys, clk := twoChannelSystem(busfault.DefaultConfig())
	sys.Advance(start)
	// 先发一帧有效控制，输出切到 10.0。
	sys.FeedChannelBus("a", ctrlFrame("a", 1, 10))
	if st := sys.Snapshot().Channels["a"]; st.Output != 10 || st.State != "OK" {
		t.Fatalf("有效控制帧未生效: %+v", st)
	}
	// 连续 3 帧坏数据，进入 SAFE，故障前输出与序号被清除。
	for i := 0; i < 3; i++ {
		sys.FeedChannelBus("a", badCRC(ctrlFrame("a", uint64(2+i), 11)))
	}
	st := sys.Snapshot().Channels["a"]
	if st.State != "SAFE" || st.Output != -1 || st.BusLastSeq != 0 {
		t.Fatalf("SAFE 语义错误: %+v", st)
	}
	// 恢复：时间稍后推进一步（推进总线时间窗），再发恢复后的新报文。
	clk.Advance(time.Second)
	sys.FeedChannelBus("a", ctrlFrame("a", 50, 42))
	st = sys.Snapshot().Channels["a"]
	if st.State != "OK" {
		t.Fatalf("一帧有效报文后应恢复 OK, got %s", st.State)
	}
	if st.Output != 42 || st.BusLastSeq != 50 {
		t.Fatalf("恢复后输出/序号应只来自新报文: %+v", st)
	}
}

func TestBusInterleavedGoodResetsCounter(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	sys.FeedChannelBus("a", badCRC(ctrlFrame("a", 1, 1)))
	sys.FeedChannelBus("a", badCRC(ctrlFrame("a", 2, 1)))
	// 夹杂一帧正常报文：连续异常清零。
	sys.FeedChannelBus("a", ctrlFrame("a", 3, 3))
	if sys.Snapshot().Channels["a"].BusConsecFails != 0 {
		t.Fatal("正常帧应清零连续异常")
	}
	// 再 2 帧坏数据不应触发（阈值 3）。
	sys.FeedChannelBus("a", badCRC(ctrlFrame("a", 4, 1)))
	sys.FeedChannelBus("a", badCRC(ctrlFrame("a", 5, 1)))
	if sys.Snapshot().Channels["a"].State != "OK" {
		t.Fatal("清零后两次坏帧不应触发 SAFE")
	}
	// 连续第 3 帧坏数据才触发。
	sys.FeedChannelBus("a", badCRC(ctrlFrame("a", 6, 1)))
	if sys.Snapshot().Channels["a"].State != "SAFE" {
		t.Fatal("连续 3 帧坏数据必须触发 SAFE")
	}
}

func TestBusDuplicateAndRegressionCountAsFaults(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig()) // 默认乱序容忍 0
	sys.FeedChannelBus("a", ctrlFrame("a", 10, 1))
	// 重复序号：异常。
	sys.FeedChannelBus("a", ctrlFrame("a", 10, 1))
	if c := sys.Snapshot().Channels["a"].BusConsecFails; c != 1 {
		t.Fatalf("重复序号后 consec=%d, want 1", c)
	}
	// 倒退序号：异常。
	sys.FeedChannelBus("a", ctrlFrame("a", 9, 1))
	sys.FeedChannelBus("a", ctrlFrame("a", 8, 1))
	if sys.Snapshot().Channels["a"].State != "SAFE" {
		t.Fatal("重复+倒退累计 3 次应触发 SAFE")
	}
}

func TestBusReorderToleranceConfigurable(t *testing.T) {
	cfg := busfault.Config{ReorderTolerance: 5}
	sys, _ := twoChannelSystem(cfg)
	sys.FeedChannelBus("a", ctrlFrame("a", 100, 1))
	// 回退 3，在容忍内：接受但不覆盖输出。
	sys.FeedChannelBus("a", ctrlFrame("a", 97, 99))
	st := sys.Snapshot().Channels["a"]
	if st.State != "OK" || st.BusConsecFails != 0 {
		t.Fatalf("容忍范围内乱序应被接受: %+v", st)
	}
	if st.Output != 1 {
		t.Fatalf("乱序旧帧不应覆盖输出, got %v", st.Output)
	}
	// 回退 10，超出容忍：异常，且不应用载荷。
	sys.FeedChannelBus("a", ctrlFrame("a", 90, 99))
	if st := sys.Snapshot().Channels["a"]; st.BusConsecFails != 1 || st.Output != 1 {
		t.Fatalf("超容忍倒退应计异常且不应用载荷: %+v", st)
	}
}

func TestBusMuxRoutingByChannelField(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	// 带合法通道字段的帧按通道落到 a。
	msgs, errs := sys.FeedBus(ctrlFrame("a", 1, 7.5))
	if len(msgs) != 1 || len(errs) != 0 {
		t.Fatalf("msgs=%d errs=%v", len(msgs), errs)
	}
	if st := sys.Snapshot().Channels["a"]; st.Output != 7.5 || st.BusLastSeq != 1 {
		t.Fatalf("复用总线帧未落到通道 a: %+v", st)
	}
	if st := sys.Snapshot().Channels["b"]; st.Output != -2 {
		t.Fatalf("b 不应受影响: %+v", st)
	}
	// 无通道字段：未归属，不影响任何通道。
	noChan := protocol.Encode(protocol.Message{
		Version: protocol.CurrentVersion, Type: protocol.TypeControl,
		Fields: map[uint8][]byte{protocol.TagSeq: protocol.U64(1)},
	})
	// 未知通道名 + 坏 CRC：均计入未归属。
	sys.FeedBus(append(append(noChan, ctrlFrame("zzz", 1, 1)...), badCRC(ctrlFrame("a", 1, 1))...))
	snap := sys.Snapshot()
	if snap.BusUnrouted != 3 {
		t.Fatalf("BusUnrouted=%d, want 3（无通道/未知通道/坏CRC）", snap.BusUnrouted)
	}
	// 复用总线上的坏 CRC 无法可靠归因，a 不降级。
	if snap.Channels["a"].State != "OK" {
		t.Fatal("复用总线坏帧不应让 a 降级")
	}
}

func TestBusMuxThreeBadRoutableFramesTrip(t *testing.T) {
	// 结构完整、校验通过、带通道字段，但序号连续重复：
	// 这种“可归属的坏帧”在复用总线上也必须让对应通道降级。
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	sys.FeedBus(ctrlFrame("a", 5, 1))
	for i := 0; i < 3; i++ {
		sys.FeedBus(ctrlFrame("a", 5, 1)) // 重复序号
	}
	if sys.Snapshot().Channels["a"].State != "SAFE" {
		t.Fatal("可归属的连续异常报文必须在阈值内让 a 降级")
	}
	if sys.Snapshot().Channels["b"].State != "OK" {
		t.Fatal("b 不受影响")
	}
}

func TestBusSafeSuppressesPayload(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.Config{MaxConsecutive: 2, RecoveryFrames: 2})
	for i := 0; i < 2; i++ {
		sys.FeedChannelBus("a", badCRC(ctrlFrame("a", uint64(i+1), 1)))
	}
	// SAFE 期间到达的有效帧载荷不生效（恢复需连续 2 帧）。
	sys.FeedChannelBus("a", ctrlFrame("a", 10, 55))
	if st := sys.Snapshot().Channels["a"]; st.Output != -1 {
		t.Fatalf("恢复帧数未集齐时载荷不得生效: %+v", st)
	}
	// 第二帧有效报文后才恢复并应用。
	sys.FeedChannelBus("a", ctrlFrame("a", 11, 56))
	if st := sys.Snapshot().Channels["a"]; st.State != "OK" || st.Output != 56 {
		t.Fatalf("恢复后应应用新载荷: %+v", st)
	}
}

func TestBusChannelIsolationUnderBurst(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 通道 a：大量坏帧 + 巨长垃圾流（超过解码器缓冲上限）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		garbage := make([]byte, 300*1024)
		for i := range garbage {
			garbage[i] = byte(i)
		}
		for i := 0; i < 100; i++ {
			sys.FeedChannelBus("a", append(badCRC(ctrlFrame("a", 1, 1)), garbage...))
		}
		close(stop)
	}()
	// 通道 b：持续发送有效控制帧，必须全部生效，不被 a 拖停。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; ; i++ {
			sys.FeedChannelBus("b", ctrlFrame("b", uint64(i), float64(i)))
			if i >= 200 {
				return
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	wg.Wait()
	st := sys.Snapshot().Channels["b"]
	if st.State != "OK" {
		t.Fatalf("b 应始终健康, got %s", st.State)
	}
	if st.Output != 200 || st.BusLastSeq != 200 {
		t.Fatalf("b 的有效报文被 a 的坏帧突发拖累: %+v", st)
	}
}

func TestBusBoundedBacklog(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	// 远大于缓冲上限的无 magic 垃圾：报告截断、计数有界、不 panic。
	huge := make([]byte, 10*protocol.DefaultMaxBuffered)
	_, errs := sys.FeedChannelBus("a", huge)
	if len(errs) == 0 {
		t.Fatal("超长积压应报告 ErrTruncated")
	}
	// 之后的正常帧仍可解码、通道可恢复。
	sys.FeedChannelBus("a", ctrlFrame("a", 1, 3.5))
	if st := sys.Snapshot().Channels["a"]; st.Output != 3.5 {
		t.Fatalf("积压后正常报文应照常生效: %+v", st)
	}
}

func TestConcurrentBusSnapshotConsistency(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	sys.Advance(start)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	frameA, frameB := ctrlFrame("a", 1, 5), ctrlFrame("b", 1, 6)
	// 两路通道并发喂入（好帧坏帧交替）。
	for id, frame := range map[string][]byte{"a": frameA, "b": frameB} {
		id, frame := id, frame
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if i%3 == 0 {
					sys.FeedChannelBus(id, badCRC(frame))
				} else {
					sys.FeedChannelBus(id, frame)
				}
			}
		}()
	}
	// 时钟推进与采样。
	wg.Add(1)
	go func() {
		defer wg.Done()
		now := start
		for i := 0; i < 100; i++ {
			now = now.Add(time.Millisecond)
			sys.Advance(now)
		}
		close(stop)
	}()
	// 并发快照读取：任何时刻都必须是一致状态，无半更新。
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				snap := sys.Snapshot()
				for _, id := range []string{"a", "b"} {
					st := snap.Channels[id]
					if st.State != "OK" && st.State != "SAFE" {
						t.Errorf("非法状态 %q", st.State)
						return
					}
					if st.State == "SAFE" && st.Output != snap.Channels[id].Output {
						t.Error("快照不一致")
						return
					}
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	wg.Wait()
}

func TestFeedChannelBusUnknownID(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	msgs, errs := sys.FeedChannelBus("nope", ctrlFrame("nope", 1, 1))
	if msgs != nil || errs != nil {
		t.Fatalf("未知通道应返回空, got msgs=%d errs=%d", len(msgs), len(errs))
	}
	if err := sys.FinishChannelBus("nope"); err != nil {
		t.Fatalf("未知通道 Finish 应为 nil, got %v", err)
	}
}

func TestFinishChannelBusTruncationCharged(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.Config{MaxConsecutive: 1})
	sys.FeedChannelBus("a", ctrlFrame("a", 1, 1)[:5]) // 残留不完整帧
	if err := sys.FinishChannelBus("a"); err == nil {
		t.Fatal("残留截断帧应返回错误")
	}
	if sys.Snapshot().Channels["a"].State != "SAFE" {
		t.Fatal("截断帧应计为该通道一次异常（阈值 1 即降级）")
	}
}
