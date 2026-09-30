package busfault

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestConsecutiveFailuresTrip(t *testing.T) {
	m := New(DefaultConfig()) // 阈值 3
	m.Fail(t0)
	m.Fail(t0.Add(10 * time.Millisecond))
	if m.Safe() {
		t.Fatal("2 次异常不应触发 SAFE")
	}
	m.Fail(t0.Add(20 * time.Millisecond))
	if !m.Safe() {
		t.Fatal("连续 3 次异常必须触发 SAFE")
	}
}

func TestValidFrameClearsConsecutive(t *testing.T) {
	m := New(DefaultConfig())
	m.Fail(t0)
	m.Fail(t0.Add(10 * time.Millisecond))
	if r := m.Frame(t0.Add(20*time.Millisecond), 1); r != Accepted {
		t.Fatalf("result=%v", r)
	}
	if m.Consecutive() != 0 {
		t.Fatalf("正常帧夹杂后异常计数应清零, got %d", m.Consecutive())
	}
	m.Fail(t0.Add(30 * time.Millisecond))
	m.Fail(t0.Add(40 * time.Millisecond))
	if m.Safe() {
		t.Fatal("清零后再 2 次异常不应触发 SAFE")
	}
}

func TestRecoveryUsesFreshFramesOnly(t *testing.T) {
	m := New(DefaultConfig())
	m.Frame(t0, 100)
	m.Fail(t0.Add(10 * time.Millisecond))
	m.Fail(t0.Add(20 * time.Millisecond))
	m.Fail(t0.Add(30 * time.Millisecond))
	if !m.Safe() {
		t.Fatal("应进入 SAFE")
	}
	// 故障前序号 100 的旧帧重发：基线已被丢弃，它会被当作恢复后首帧接受，
	// 建立新基线 100（而不是被判为倒退）——恢复只看恢复之后的报文。
	if r := m.Frame(t0.Add(40*time.Millisecond), 100); r != Accepted {
		t.Fatalf("result=%v, want Accepted as fresh baseline", r)
	}
	if m.Safe() {
		t.Fatal("默认 1 帧恢复：应回到 OK")
	}
	// 恢复后下一个更大的序号被接受。
	if r := m.Frame(t0.Add(50*time.Millisecond), 101); r != Accepted || m.Safe() {
		t.Fatalf("result=%v safe=%v", r, m.Safe())
	}
}

func TestRecoveryFramesRequirement(t *testing.T) {
	m := New(Config{MaxConsecutive: 1, RecoveryFrames: 3})
	m.Fail(t0)
	if !m.Safe() {
		t.Fatal("阈值 1 应立即 SAFE")
	}
	m.Frame(t0.Add(10*time.Millisecond), 1)
	m.Frame(t0.Add(20*time.Millisecond), 2)
	if !m.Safe() {
		t.Fatal("2 帧有效报文不应恢复，需要 3 帧")
	}
	m.Fail(t0.Add(25 * time.Millisecond)) // 异常打断恢复进度
	m.Frame(t0.Add(30*time.Millisecond), 3)
	m.Frame(t0.Add(40*time.Millisecond), 4)
	if !m.Safe() {
		t.Fatal("恢复进度被异常打断后需重新计数")
	}
	m.Frame(t0.Add(50*time.Millisecond), 5)
	if m.Safe() {
		t.Fatal("连续 3 帧有效报文后应恢复")
	}
}

func TestReorderTolerance(t *testing.T) {
	m := New(Config{ReorderTolerance: 5})
	m.Frame(t0, 10)
	// 回退 3，在容忍范围内：接受但不推进基线。
	if r := m.Frame(t0.Add(10*time.Millisecond), 7); r != AcceptedReordered {
		t.Fatalf("result=%v", r)
	}
	// 基线仍是 10：来一帧 9 仍然在容忍范围内。
	if r := m.Frame(t0.Add(20*time.Millisecond), 9); r != AcceptedReordered {
		t.Fatalf("result=%v", r)
	}
	// 回退超过容忍量：真正倒退，计异常。
	if r := m.Frame(t0.Add(30*time.Millisecond), 4); r != Rejected || m.Consecutive() != 1 {
		t.Fatalf("result=%v consec=%d", r, m.Consecutive())
	}
	// 乱序容忍帧像正常帧一样清零异常计数。
	if r := m.Frame(t0.Add(40*time.Millisecond), 8); r != AcceptedReordered || m.Consecutive() != 0 {
		t.Fatalf("result=%v consec=%d", r, m.Consecutive())
	}
	// 容忍范围内的旧帧不能推进基线：10 仍是重复。
	if r := m.Frame(t0.Add(50*time.Millisecond), 10); r != Rejected {
		t.Fatalf("重复序号应判异常, got %v", r)
	}
}

func TestDefaultZeroToleranceRejectsAnyBackward(t *testing.T) {
	m := New(DefaultConfig())
	m.Frame(t0, 10)
	if r := m.Frame(t0.Add(10*time.Millisecond), 9); r != Rejected {
		t.Fatalf("默认容忍 0，任何倒退都应判异常, got %v", r)
	}
	if r := m.Frame(t0.Add(20*time.Millisecond), 10); r != Rejected {
		t.Fatalf("重复序号应判异常, got %v", r)
	}
}

func TestWindowModeTripsAcrossInterleavedGood(t *testing.T) {
	m := New(Config{MaxConsecutive: 3, Window: 100 * time.Millisecond})
	m.Fail(t0)
	m.Frame(t0.Add(10*time.Millisecond), 1) // 中间夹杂正常帧
	m.Fail(t0.Add(20 * time.Millisecond))
	m.Fail(t0.Add(30 * time.Millisecond))
	if !m.Safe() {
		t.Fatal("时间窗内 3 次异常（非连续）也应触发 SAFE")
	}
}

func TestWindowFailuresExpire(t *testing.T) {
	m := New(Config{MaxConsecutive: 3, Window: 100 * time.Millisecond})
	m.Fail(t0)
	m.Fail(t0.Add(20 * time.Millisecond))
	// 超过窗口后旧异常已滑出。
	m.Tick(t0.Add(200 * time.Millisecond))
	m.Fail(t0.Add(210 * time.Millisecond))
	if m.Safe() {
		t.Fatal("窗口外旧异常不应计入，不应 SAFE")
	}
}

func TestLivenessResetsCounter(t *testing.T) {
	m := New(DefaultConfig())
	m.Fail(t0)
	m.Alive(t0.Add(10 * time.Millisecond))
	if m.Consecutive() != 0 {
		t.Fatalf("活性帧应清零异常计数, got %d", m.Consecutive())
	}
}

func TestCounters(t *testing.T) {
	m := New(DefaultConfig())
	m.Frame(t0, 1)
	m.Fail(t0.Add(10 * time.Millisecond))
	m.Frame(t0.Add(20*time.Millisecond), 2)
	if m.AcceptedTotal() != 2 || m.RejectedTotal() != 1 {
		t.Fatalf("accepted=%d rejected=%d", m.AcceptedTotal(), m.RejectedTotal())
	}
}
