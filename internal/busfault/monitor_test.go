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
	// 故障前序号 100 的旧帧重发：与故障前最新序号相同，不算恢复，
	// 计一次异常，通道仍处于 SAFE。
	if r := m.Frame(t0.Add(40*time.Millisecond), 100); r != Rejected {
		t.Fatalf("重复故障前序号 result=%v, want Rejected", r)
	}
	if !m.Safe() {
		t.Fatal("故障前旧序号不能触发恢复")
	}
	// 比故障前更旧的序号同样不算恢复。
	if r := m.Frame(t0.Add(45*time.Millisecond), 99); r != Rejected {
		t.Fatalf("倒退序号 result=%v, want Rejected", r)
	}
	if !m.Safe() {
		t.Fatal("比故障前更旧的序号不能触发恢复")
	}
	// 严格大于故障前最新序号（100）的报文才是故障后新报文，默认 1 帧恢复。
	if r := m.Frame(t0.Add(50*time.Millisecond), 101); r != Accepted || m.Safe() {
		t.Fatalf("真正更新的报文应恢复: result=%v safe=%v", r, m.Safe())
	}
	// 恢复后下一个更大的序号被接受。
	if r := m.Frame(t0.Add(60*time.Millisecond), 102); r != Accepted || m.Safe() {
		t.Fatalf("result=%v safe=%v", r, m.Safe())
	}
}

func TestRecoveryProgressResetByStaleFrames(t *testing.T) {
	// RecoveryFrames=3：旧报文穿插时恢复进度按确定规则重新计算。
	m := New(Config{MaxConsecutive: 1, RecoveryFrames: 3})
	m.Frame(t0, 10)
	m.Fail(t0.Add(5 * time.Millisecond))
	if !m.Safe() {
		t.Fatal("阈值 1 应立即 SAFE")
	}
	// 第一帧真正更新的报文：进度 1/3。
	m.Frame(t0.Add(10*time.Millisecond), 11)
	if p := m.RecoveryProgress(); p != 1 || !m.Safe() {
		t.Fatalf("进度=%d safe=%v, want 1/SAFE", p, m.Safe())
	}
	// 旧报文（重复当前已接受最大序号）：不算恢复，进度清零。
	if r := m.Frame(t0.Add(20*time.Millisecond), 11); r != Rejected {
		t.Fatalf("重复序号 result=%v", r)
	}
	if p := m.RecoveryProgress(); p != 0 {
		t.Fatalf("旧报文后进度应清零, got %d", p)
	}
	// 再来一帧真正更新：只算 1/3，而不是接着之前的进度。
	m.Frame(t0.Add(30*time.Millisecond), 12)
	if p := m.RecoveryProgress(); p != 1 || !m.Safe() {
		t.Fatalf("旧报文后恢复进度应重新计算: %d safe=%v", p, m.Safe())
	}
	// 容忍范围内乱序帧也不推进恢复，并清零已累计进度。
	m2 := New(Config{MaxConsecutive: 1, RecoveryFrames: 2, ReorderTolerance: 5})
	m2.Frame(t0, 20)
	m2.Fail(t0.Add(5 * time.Millisecond))
	m2.Frame(t0.Add(10*time.Millisecond), 21) // 进度 1/2
	if r := m2.Frame(t0.Add(20*time.Millisecond), 18); r != AcceptedReordered {
		t.Fatalf("容忍内乱序 result=%v", r)
	}
	if p := m2.RecoveryProgress(); p != 0 || !m2.Safe() {
		t.Fatalf("乱序旧帧应清零恢复进度: p=%d safe=%v", p, m2.Safe())
	}
	// 连续两帧真正更新后恢复。
	m2.Frame(t0.Add(30*time.Millisecond), 22)
	m2.Frame(t0.Add(40*time.Millisecond), 23)
	if m2.Safe() {
		t.Fatal("连续两帧故障后新报文后应恢复")
	}
}

func TestRecoveryLivenessDoesNotDriveRecovery(t *testing.T) {
	m := New(Config{MaxConsecutive: 1, RecoveryFrames: 2})
	m.Frame(t0, 5)
	m.Fail(t0.Add(5 * time.Millisecond))
	// SAFE 期间活性帧既不推进恢复，也不清零已有进度。
	m.Alive(t0.Add(10 * time.Millisecond))
	if p := m.RecoveryProgress(); p != 0 || !m.Safe() {
		t.Fatalf("活性帧不应推进恢复: p=%d safe=%v", p, m.Safe())
	}
	m.Frame(t0.Add(20*time.Millisecond), 6) // 1/2
	m.Alive(t0.Add(25 * time.Millisecond))
	if p := m.RecoveryProgress(); p != 1 {
		t.Fatalf("活性帧不应改变恢复进度, got %d", p)
	}
	m.Frame(t0.Add(30*time.Millisecond), 7)
	if m.Safe() {
		t.Fatal("两帧故障后新报文后应恢复")
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
