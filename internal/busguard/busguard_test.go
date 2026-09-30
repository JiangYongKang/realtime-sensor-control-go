package busguard

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func cfg() Config {
	return Config{MaxConsecutiveBad: 3, FaultWindow: time.Second, ReorderWindow: 1}
}

func TestTripsAfterConsecutiveBadFrames(t *testing.T) {
	g := New(cfg())
	if g.Observe(t0, 1) != DispApplied {
		t.Fatal("首帧应被接受")
	}
	g.Fault(t0.Add(10 * time.Millisecond))
	if g.Status().Safe {
		t.Fatal("1 帧异常不应触发 SAFE")
	}
	g.Fault(t0.Add(20 * time.Millisecond))
	if g.Status().Safe {
		t.Fatal("2 帧异常不应触发 SAFE")
	}
	g.Fault(t0.Add(30 * time.Millisecond))
	if !g.Status().Safe {
		t.Fatal("连续 3 帧异常必须触发 SAFE")
	}
	if st := g.Status(); st.BadStreak != 3 || st.BadTotal != 3 {
		t.Fatalf("status=%+v", st)
	}
}

func TestOneGoodFrameClearsStreak(t *testing.T) {
	g := New(cfg())
	g.Observe(t0, 1)
	g.Fault(t0.Add(10 * time.Millisecond))
	g.Fault(t0.Add(20 * time.Millisecond))
	// 夹杂一帧正常报文：清零。
	if d := g.Observe(t0.Add(30*time.Millisecond), 2); d != DispApplied {
		t.Fatalf("disp=%v, want Applied", d)
	}
	if g.Status().BadStreak != 0 {
		t.Fatalf("正常帧后 BadStreak=%d, want 0", g.Status().BadStreak)
	}
	g.Fault(t0.Add(40 * time.Millisecond))
	g.Fault(t0.Add(50 * time.Millisecond))
	if g.Status().Safe {
		t.Fatal("清零后再来 2 帧异常不应触发 SAFE")
	}
}

func TestReorderWithinWindowNotCounted(t *testing.T) {
	g := New(Config{MaxConsecutiveBad: 2, FaultWindow: time.Second, ReorderWindow: 2})
	g.Observe(t0, 10)
	// 回退 1、2 在容忍窗口内：乱序，不计数、不清零、不采用。
	for _, seq := range []uint64{9, 8} {
		if d := g.Observe(t0.Add(10*time.Millisecond), seq); d != DispReordered {
			t.Fatalf("seq=%d disp=%v, want Reordered", seq, d)
		}
	}
	st := g.Status()
	if st.Safe || st.BadStreak != 0 || st.ReorderTotal != 2 {
		t.Fatalf("status=%+v", st)
	}
	if st.LastSeq != 10 {
		t.Fatalf("乱序帧不得更新 LastSeq: %d", st.LastSeq)
	}
	// 回退 3 超出窗口：一次异常。
	if d := g.Observe(t0.Add(20*time.Millisecond), 7); d != DispBad {
		t.Fatalf("disp=%v, want Bad", d)
	}
}

func TestDuplicateSeqIsFault(t *testing.T) {
	g := New(cfg())
	g.Observe(t0, 5)
	if d := g.Observe(t0.Add(10*time.Millisecond), 5); d != DispBad {
		t.Fatalf("重复序号 disp=%v, want Bad", d)
	}
	if g.Status().BadTotal != 1 || g.Status().BadStreak != 1 {
		t.Fatalf("重复序号必须计一次异常: %+v", g.Status())
	}
}

func TestFaultWindowTripsOnTick(t *testing.T) {
	g := New(cfg()) // FaultWindow=1s, MaxConsecutiveBad=3
	g.Observe(t0, 1)
	g.Fault(t0.Add(100 * time.Millisecond))
	// 不足 1s：Tick 不触发。
	g.Tick(t0.Add(900 * time.Millisecond))
	if g.Status().Safe {
		t.Fatal("未满 FaultWindow 不应触发 SAFE")
	}
	// 到达 1s（即使帧数只有 1，也不依赖采样超时兜底）。
	g.Tick(t0.Add(1100 * time.Millisecond))
	if !g.Status().Safe {
		t.Fatal("异常持续达到 FaultWindow 必须触发 SAFE")
	}
}

func TestRecoveryOnlyAfterFreshSeq(t *testing.T) {
	g := New(cfg())
	g.Observe(t0, 1)
	g.Observe(t0.Add(10*time.Millisecond), 2)
	g.Observe(t0.Add(20*time.Millisecond), 3)
	// 触发 SAFE，故障前峰值 floor=3。
	g.Fault(t0.Add(30 * time.Millisecond))
	g.Fault(t0.Add(40 * time.Millisecond))
	g.Fault(t0.Add(50 * time.Millisecond))
	if !g.Status().Safe {
		t.Fatal("want SAFE")
	}
	// 故障前/同序号的报文一律 Stale，不得恢复。
	for _, seq := range []uint64{1, 2, 3} {
		if d := g.Observe(t0.Add(60*time.Millisecond), seq); d != DispStale {
			t.Fatalf("seq=%d disp=%v, want Stale", seq, d)
		}
	}
	if !g.Status().Safe {
		t.Fatal("陈旧报文不得恢复通道")
	}
	// 新序号报文立即恢复并被采用。
	if d := g.Observe(t0.Add(70*time.Millisecond), 4); d != DispApplied {
		t.Fatalf("disp=%v, want Applied", d)
	}
	st := g.Status()
	if st.Safe || st.BadStreak != 0 || st.LastSeq != 4 {
		t.Fatalf("恢复后状态错误: %+v", st)
	}
}

func TestInvalidateArmsRecoveryGate(t *testing.T) {
	g := New(cfg())
	g.Observe(t0, 7)
	g.Invalidate()
	if !g.Status().Safe {
		t.Fatal("Invalidate 后应 SAFE")
	}
	if d := g.Observe(t0.Add(10*time.Millisecond), 7); d != DispStale {
		t.Fatalf("disp=%v, want Stale", d)
	}
	if g.Observe(t0.Add(20*time.Millisecond), 8) != DispApplied {
		t.Fatal("新序号报文应恢复")
	}
}

func TestDefaultsAreStable(t *testing.T) {
	g := New(Config{})
	st0 := g.Status()
	_ = st0
	// 零值配置填充默认值后，3 帧异常必触发。
	g.Fault(t0)
	g.Fault(t0)
	g.Fault(t0)
	if !g.Status().Safe {
		t.Fatal("默认 MaxConsecutiveBad=3 必须触发 SAFE")
	}
}
