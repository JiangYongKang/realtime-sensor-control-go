package system

// 通道从总线安全状态恢复的判定用例。
//
// 覆盖业务：
//  1. 故障后用故障前旧序号（相同/更旧）报文不能恢复、不能改输出；
//  2. 只有序号严格大于故障前最新序号的报文才允许恢复并采用其值；
//  3. 旧报文与真正更新报文穿插时，恢复进度按“连续新报文”确定地重算；
//  4. 一路通道恢复判定不影响其他通道；
//  5. 并发喂入与状态读取下快照始终一致（配合 -race）；
//  6. 默认配置下结果确定可复现；
//  7. 反复进入/退出 SAFE 的长时间行为稳定、计数有界。

import (
	"runtime"
	"sync"
	"testing"

	"github.com/highcumontoa/realtime-sensor-control-go/internal/busfault"
)

// faultA 通过 3 帧坏 CRC 让通道 a 进入总线 SAFE（默认阈值 3）。
func faultA(sys *System) {
	for i := 0; i < 3; i++ {
		sys.FeedChannelBus("a", badCRC(ctrlFrame("a", uint64(100+i), 99)))
	}
}

// TestRecoveryStaleFramesDoNotRecover：故障后喂入与故障前序号相同、
// 甚至更旧的报文，通道必须保持 SAFE、输出保持安全值、旧值不得落地。
func TestRecoveryStaleFramesDoNotRecover(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())

	// 故障前：序号 10 的控制帧生效，输出 10.0。
	sys.FeedChannelBus("a", ctrlFrame("a", 10, 10))
	st := sys.Snapshot().Channels["a"]
	if st.State != "OK" || st.Output != 10 || st.BusLastSeq != 10 {
		t.Fatalf("前置状态错误: %+v", st)
	}
	t.Logf("业务=故障前基线: state=%s seq=%d output=%.1f", st.State, st.BusLastSeq, st.Output)

	faultA(sys)
	st = sys.Snapshot().Channels["a"]
	if st.State != "SAFE" || st.Output != -1 || st.BusLastSeq != 0 {
		t.Fatalf("应进入 SAFE 且输出切安全值: %+v", st)
	}
	t.Log("业务=连续3帧坏CRC: a 进入 SAFE，输出=-1，故障前序号/值已清除")

	cases := []struct {
		name string
		seq  uint64
		desc string
	}{
		{"与故障前序号相同(10)", 10, "重复故障前最新序号，不能算恢复"},
		{"比故障前更旧(9)", 9, "倒退报文，不能算恢复"},
		{"故障前更早出现过的序号(1)", 1, "故障前已用过的序号，不能算恢复"},
		{"再次重放序号10", 10, "重放旧报文同样不能恢复"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sys.FeedChannelBus("a", ctrlFrame("a", c.seq, 77.0))
			got := sys.Snapshot().Channels["a"]
			t.Logf("业务=%s: state=%s output=%.1f busSeq=%d rejected=%d -> 期望 SAFE/-1/不改输出",
				c.desc, got.State, got.Output, got.BusLastSeq, got.BusRejected)
			if got.State != "SAFE" {
				t.Fatalf("旧序号报文不得触发恢复: %+v", got)
			}
			if got.Output != -1 {
				t.Fatalf("旧报文载荷不得写进控制输出: output=%v", got.Output)
			}
			if got.BusLastSeq != 0 || got.BusLastValue != 0 {
				t.Fatalf("旧报文不得更新最近序号/值: seq=%d value=%v", got.BusLastSeq, got.BusLastValue)
			}
			if got.BusRecovery != 0 {
				t.Fatalf("旧报文不得推进恢复进度: %d", got.BusRecovery)
			}
		})
	}

	// 真正故障之后产生的报文（序号 11 > 故障前 10）才允许恢复。
	sys.FeedChannelBus("a", ctrlFrame("a", 11, 42))
	st = sys.Snapshot().Channels["a"]
	t.Logf("业务=真正更新报文(seq=11): state=%s output=%.1f seq=%d -> 期望 OK/42/11",
		st.State, st.Output, st.BusLastSeq)
	if st.State != "OK" || st.Output != 42 || st.BusLastSeq != 11 {
		t.Fatalf("只有真正更新的报文才能恢复并驱动输出: %+v", st)
	}
}

// TestRecoveryOnlyFreshSeqRecovers：更细粒度地确认恢复点恰好落在
// “序号超过故障前最新序号”的那一帧；故障门槛逐次抬高。
func TestRecoveryOnlyFreshSeqRecovers(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	sys.FeedChannelBus("a", ctrlFrame("a", 100, 1))
	faultA(sys) // 坏帧携带序号 100..102，但解码失败不计序号；门槛仍为 100

	// seq=100 仍旧；seq=101 才是故障后新报文。
	sys.FeedChannelBus("a", ctrlFrame("a", 100, 50))
	if st := sys.Snapshot().Channels["a"]; st.State != "SAFE" || st.Output != -1 {
		t.Fatalf("门槛序号 100 不得恢复: %+v", st)
	}
	t.Log("业务=恢复门槛: seq=100 与故障前最新序号相同，保持 SAFE/-1")
	sys.FeedChannelBus("a", ctrlFrame("a", 101, 51))
	if st := sys.Snapshot().Channels["a"]; st.State != "OK" || st.Output != 51 {
		t.Fatalf("seq=101 必须恢复并采用其值: %+v", st)
	}
	t.Log("业务=跨过门槛: seq=101 > 100，恢复 OK，输出采用 51（旧帧的 50 从未生效）")

	// 再次故障：新门槛为 101；恢复后必须 >101。
	faultA(sys)
	if st := sys.Snapshot().Channels["a"]; st.State != "SAFE" {
		t.Fatalf("二次故障应进入 SAFE: %+v", st)
	}
	sys.FeedChannelBus("a", ctrlFrame("a", 101, 60))
	if st := sys.Snapshot().Channels["a"]; st.State != "SAFE" || st.Output != -1 {
		t.Fatalf("重复新一轮门槛序号 101 不得恢复: %+v", st)
	}
	sys.FeedChannelBus("a", ctrlFrame("a", 102, 61))
	if st := sys.Snapshot().Channels["a"]; st.State != "OK" || st.Output != 61 {
		t.Fatalf("seq=102 才恢复且只能采用新值: %+v", st)
	}
	t.Log("业务=二次故障门槛抬高: seq=101 被拒，seq=102 恢复并输出 61")
}

// TestRecoveryProgressInterleaved：RecoveryFrames=3 时旧报文穿插，
// 恢复进度按确定规则（旧帧清零、只有连续新帧累计）重新计算。
func TestRecoveryProgressInterleaved(t *testing.T) {
	cfg := busfault.Config{MaxConsecutive: 1, RecoveryFrames: 3}
	sys, _ := twoChannelSystem(cfg)
	sys.FeedChannelBus("a", ctrlFrame("a", 10, 100))
	sys.FeedChannelBus("a", badCRC(ctrlFrame("a", 11, 0))) // 1 帧坏即 SAFE
	if st := sys.Snapshot().Channels["a"]; st.State != "SAFE" {
		t.Fatalf("前置: 应 SAFE, %+v", st)
	}

	steps := []struct {
		seq      uint64
		bad      bool
		wantProg int
		wantSafe bool
		wantOut  float64
		why      string
	}{
		{11, false, 1, true, -1, "新报文 进度1/3"},
		{11, false, 0, true, -1, "重复序号 进度清零重算"},
		{12, false, 1, true, -1, "被旧帧打断后只算1/3"},
		{5, false, 0, true, -1, "倒退旧帧 进度再次清零"},
		{13, false, 1, true, -1, "重新累计 1/3"},
		{0, true, 0, true, -1, "坏帧异常打断 进度0"},
		{14, false, 1, true, -1, "异常后从头开始 1/3"},
		{15, false, 2, true, -1, "连续新报文 2/3 仍不输出"},
		{16, false, 0, false, 88, "第3帧新报文 恢复并采用其值"},
	}
	for i, st := range steps {
		var frame []byte
		if st.bad {
			frame = badCRC(ctrlFrame("a", st.seq, 0))
		} else {
			frame = ctrlFrame("a", st.seq, 88)
		}
		sys.FeedChannelBus("a", frame)
		got := sys.Snapshot().Channels["a"]
		stateWant := "OK"
		if st.wantSafe {
			stateWant = "SAFE"
		}
		t.Logf("步骤%02d 业务=%-28s seq=%-3d -> state=%s progress=%d output=%.1f（期望 %s/%d/%.1f）",
			i+1, st.why, st.seq, got.State, got.BusRecovery, got.Output, stateWant, st.wantProg, st.wantOut)
		if got.BusRecovery != st.wantProg {
			t.Fatalf("步骤%d 恢复进度=%d, want %d（%s）", i+1, got.BusRecovery, st.wantProg, st.why)
		}
		if (got.State == "SAFE") != st.wantSafe {
			t.Fatalf("步骤%d 状态=%s, want safe=%v（%s）", i+1, got.State, st.wantSafe, st.why)
		}
		if got.Output != st.wantOut {
			t.Fatalf("步骤%d 输出=%v, want %v（%s）", i+1, got.Output, st.wantOut, st.why)
		}
	}
}

// TestRecoveryChannelIsolation：a 通道反复收到旧报文无法恢复期间，
// b 通道始终正常、输出持续推进；a 恢复不影响 b。
func TestRecoveryChannelIsolation(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	// a 故障前基线 seq=1。
	sys.FeedChannelBus("a", ctrlFrame("a", 1, 10))
	faultA(sys)
	if sys.Snapshot().Channels["a"].State != "SAFE" {
		t.Fatal("a 应 SAFE")
	}
	// a 反复被旧报文喂入，始终无法恢复；同时 b 一路递增正常工作。
	for i := 1; i <= 50; i++ {
		sys.FeedChannelBus("a", ctrlFrame("a", 1, float64(i))) // 恒为旧序号 1
		sys.FeedChannelBus("b", ctrlFrame("b", uint64(i), float64(100+i)))
		a := sys.Snapshot().Channels["a"]
		b := sys.Snapshot().Channels["b"]
		if a.State != "SAFE" || a.Output != -1 {
			t.Fatalf("第%d轮: a 旧报文不得恢复: %+v", i, a)
		}
		if b.State != "OK" || b.Output != float64(100+i) || b.BusLastSeq != uint64(i) {
			t.Fatalf("第%d轮: b 必须不受 a 影响: %+v", i, b)
		}
	}
	t.Log("业务=50轮: a 持续旧报文保持 SAFE/-1；b 序号1..50 全部正常生效，互不影响")
	// a 收到真正更新报文后恢复，b 依然不变。
	sys.FeedChannelBus("a", ctrlFrame("a", 2, 7.5))
	a := sys.Snapshot().Channels["a"]
	b := sys.Snapshot().Channels["b"]
	if a.State != "OK" || a.Output != 7.5 {
		t.Fatalf("a 新报文恢复失败: %+v", a)
	}
	if b.State != "OK" || b.Output != 150 || b.BusLastSeq != 50 {
		t.Fatalf("a 恢复不得影响 b: %+v", b)
	}
	t.Log("业务=a 恢复 OK/7.5；b 仍为 OK/150/seq50")
}

// TestRecoveryDeterministicDefaultConfig：同一份喂入序列跑两遍，
// 每一步快照必须完全一致——默认配置下恢复进度与异常计数确定可复现。
func TestRecoveryDeterministicDefaultConfig(t *testing.T) {
	script := func(sys *System) {
		sys.FeedChannelBus("a", ctrlFrame("a", 10, 1))
		faultA(sys)
		sys.FeedChannelBus("a", ctrlFrame("a", 10, 2))
		sys.FeedChannelBus("a", ctrlFrame("a", 9, 2))
		sys.FeedChannelBus("a", ctrlFrame("a", 11, 3))
		faultA(sys)
		sys.FeedChannelBus("a", ctrlFrame("a", 11, 4))
		sys.FeedChannelBus("a", ctrlFrame("a", 12, 5))
		for i := 0; i < 3; i++ {
			sys.FeedChannelBus("a", badCRC(ctrlFrame("a", 20, 0)))
		}
		sys.FeedChannelBus("a", ctrlFrame("a", 13, 6))
	}
	s1, _ := twoChannelSystem(busfault.DefaultConfig())
	s2, _ := twoChannelSystem(busfault.DefaultConfig())
	for step := 0; step < 12; step++ {
		script(s1)
		script(s2)
		if a1, a2 := s1.Snapshot().Channels["a"], s2.Snapshot().Channels["a"]; a1 != a2 {
			t.Fatalf("第%d遍快照不一致:\n%+v\n%+v", step+1, a1, a2)
		}
	}
	t.Log("业务=同序列重复12轮，逐步快照完全一致：恢复进度/异常计数/输出确定可复现")
}

// TestConcurrentRecoverySnapshotConsistency：通道在 SAFE/恢复之间反复
// 横跳时，并发喂入与快照读取，任何时刻快照必须自洽，无半更新、无数据竞争。
func TestConcurrentRecoverySnapshotConsistency(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.Config{MaxConsecutive: 2, RecoveryFrames: 2})
	safeVal := map[string]float64{"a": -1, "b": -2}

	var feedWG sync.WaitGroup
	feed := func(id string) {
		defer feedWG.Done()
		var seq uint64 = 1
		for i := 0; ; i++ {
			// 5 步循环：坏帧->坏帧(降级) -> 旧序号(不得恢复) ->
			// 新帧(进度1/2) -> 新帧(进度2/2 恢复)，通道反复横跳 SAFE/OK。
			var frame []byte
			switch i % 5 {
			case 0, 1:
				frame = badCRC(ctrlFrame(id, seq, 0))
			case 2:
				frame = ctrlFrame(id, seq, 0) // 重复旧序号
			case 3:
				frame = ctrlFrame(id, seq, float64(seq))
				seq++
			default:
				frame = ctrlFrame(id, seq, float64(seq))
				seq++
			}
			sys.FeedChannelBus(id, frame)
			if i > 400 {
				return
			}
		}
	}
	feedWG.Add(2)
	go feed("a")
	go feed("b")

	// 快照读取：SAFE 必然对应安全输出，OK 下字段自洽，绝无半更新。
	// reader 使用独立 WaitGroup：先等喂入结束，再关闭 readerDone 让其退出。
	readerDone := make(chan struct{})
	var failOnce sync.Once
	failReader := func(format string, args ...any) {
		t.Errorf(format, args...)
		failOnce.Do(func() { close(readerDone) })
	}
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-readerDone:
				return
			default:
			}
			snap := sys.Snapshot()
			if snap.Now.IsZero() {
				failReader("快照时间异常")
				return
			}
			for _, id := range []string{"a", "b"} {
				st := snap.Channels[id]
				switch st.State {
				case "OK", "SAFE":
				default:
					failReader("非法状态 %q", st.State)
					return
				}
				if st.BusState == "SAFE" && st.Output != safeVal[id] {
					failReader("通道 %s SAFE 半更新: output=%v want %v", id, st.Output, safeVal[id])
					return
				}
				if st.State == "SAFE" && st.Output != safeVal[id] {
					failReader("通道 %s SAFE 输出不是安全值: %v", id, st.Output)
					return
				}
				if st.BusRecovery < 0 || st.BusRecovery > 2 {
					failReader("通道 %s 恢复进度越界: %d", id, st.BusRecovery)
					return
				}
			}
		}
	}()
	feedWG.Wait()
	close(readerDone)
	readerWG.Wait()
	if t.Failed() {
		return
	}
	t.Log("业务=两通道并发坏帧/旧帧/新帧 400+ 轮，读取方未见任何半更新快照（-race 下数据竞争为零）")
}

// TestRecoveryLongRunBounded：长时间反复进入/退出 SAFE，
// 行为稳定、连续异常计数与恢复进度始终有界。
func TestRecoveryLongRunBounded(t *testing.T) {
	sys, _ := twoChannelSystem(busfault.DefaultConfig())
	// 建立故障前序号基线，门槛为 1000。
	sys.FeedChannelBus("a", ctrlFrame("a", 1000, 0))

	var heapMid, heapEnd uint64
	for cycle := 0; cycle < 2000; cycle++ {
		faultA(sys)
		if st := sys.Snapshot().Channels["a"]; st.State != "SAFE" {
			t.Fatalf("cycle %d: 应 SAFE", cycle)
		}
		// 先旧后新：旧帧序号恒 < 门槛，永远恢复不了；严格更新的新帧恢复。
		sys.FeedChannelBus("a", ctrlFrame("a", uint64(cycle), 1)) // 旧于门槛
		st := sys.Snapshot().Channels["a"]
		if st.State != "SAFE" || st.Output != -1 {
			t.Fatalf("cycle %d: 旧序号不得恢复: %+v", cycle, st)
		}
		sys.FeedChannelBus("a", ctrlFrame("a", uint64(2000+cycle), 2))
		st = sys.Snapshot().Channels["a"]
		if st.State != "OK" || st.Output != 2 {
			t.Fatalf("cycle %d: 新序号应恢复: %+v", cycle, st)
		}
		if st.BusConsecFails < 0 || st.BusConsecFails > 3 || st.BusRecovery != 0 {
			t.Fatalf("cycle %d: 计数越界: %+v", cycle, st)
		}
		if cycle == 999 {
			runtime.GC()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			heapMid = ms.HeapAlloc
		}
	}
	// 计数有界：连续异常不可能超过阈值，恢复进度恢复后归零。
	st := sys.Snapshot().Channels["a"]
	if st.BusConsecFails != 0 || st.BusRecovery != 0 {
		t.Fatalf("长跑结束计数应归零: consec=%d recovery=%d", st.BusConsecFails, st.BusRecovery)
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heapEnd = ms.HeapAlloc
	// 再跑 1000 轮，堆不应继续随周期数增长（允许 GC 抖动，取宽松上限 10%）。
	for cycle := 2000; cycle < 3000; cycle++ {
		faultA(sys)
		sys.FeedChannelBus("a", ctrlFrame("a", uint64(cycle), 1))
		sys.FeedChannelBus("a", ctrlFrame("a", uint64(4000+cycle), 2))
	}
	runtime.GC()
	runtime.ReadMemStats(&ms)
	if float64(ms.HeapAlloc) > float64(heapEnd)*1.1+4096 {
		t.Fatalf("堆随周期增长: %dKiB(1000轮) -> %dKiB(2000轮) -> %dKiB(3000轮)",
			heapMid/1024, heapEnd/1024, ms.HeapAlloc/1024)
	}
	t.Logf("业务=3000 次 SAFE 进出循环稳定、计数有界；堆平台 %dKiB -> %dKiB -> %dKiB，无积压增长",
		heapMid/1024, heapEnd/1024, ms.HeapAlloc/1024)
}
