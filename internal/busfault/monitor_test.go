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
	// 故障前序号 100 的旧帧重发：不算有效恢复，按异常处理。
	if r := m.Frame(t0.Add(40*time.Millisecond), 100); r != Rejected {
		t.Fatalf("故障前同序旧帧应判异常, result=%v", r)
	}
	if !m.Safe() {
		t.Fatal("旧帧不得让通道恢复")
	}
	// 故障前更旧的序号同样不算恢复。
	if r := m.Frame(t0.Add(50*time.Millisecond), 99); r != Rejected {
		t.Fatalf("故障前更旧序号应判异常, result=%v", r)
	}
	if !m.Safe() {
		t.Fatal("更旧序号不得让通道恢复")
	}
	// 只有严格大于故障前最大序号的新报文才算有效恢复。
	if r := m.Frame(t0.Add(60*time.Millisecond), 101); r != Accepted {
		t.Fatalf("故障后新报文应被接受, result=%v", r)
	}
	if m.Safe() {
		t.Fatal("默认 1 帧恢复：真正的新报文应让通道回到 OK")
	}
	// 恢复后下一个更大的序号被接受。
	if r := m.Frame(t0.Add(70*time.Millisecond), 102); r != Accepted || m.Safe() {
		t.Fatalf("result=%v safe=%v", r, m.Safe())
	}
}

// 恢复进度只由真正的新报文推进：旧报文夹杂其间时按确定规则清零重计。
func TestRecoveryProgressRestartedByStaleFrames(t *testing.T) {
	m := New(Config{MaxConsecutive: 1, RecoveryFrames: 2})
	m.Frame(t0, 10)
	m.Fail(t0.Add(10 * time.Millisecond))
	if !m.Safe() {
		t.Fatal("阈值 1 应立即 SAFE")
	}
	// 第 1 帧真正的新报文：恢复进度 1/2，未恢复。
	if r := m.Frame(t0.Add(20*time.Millisecond), 11); r != Accepted || !m.Safe() {
		t.Fatalf("result=%v safe=%v", r, m.Safe())
	}
	// 夹杂故障前旧序号：计异常且清零恢复进度。
	if r := m.Frame(t0.Add(30*time.Millisecond), 10); r != Rejected {
		t.Fatalf("旧序号应判异常, result=%v", r)
	}
	// 进度被清零：之前那帧 11 不得被旧报文“带过去”，需重新集齐 2 帧。
	if r := m.Frame(t0.Add(40*time.Millisecond), 12); r != Accepted || !m.Safe() {
		t.Fatalf("进度重计后第 1 帧不应恢复, result=%v safe=%v", r, m.Safe())
	}
	if r := m.Frame(t0.Add(50*time.Millisecond), 13); r != Accepted || m.Safe() {
		t.Fatalf("连续 2 帧新报文后应恢复, result=%v safe=%v", r, m.Safe())
	}
}

// 恢复期间建立的新基线成为新的故障水位：再次故障后，恢复期旧序号同样失效。
func TestFaultWatermarkAdvancesAcrossEpisodes(t *testing.T) {
	m := New(Config{MaxConsecutive: 1, RecoveryFrames: 1})
	m.Frame(t0, 10)
	m.Fail(t0.Add(10 * time.Millisecond)) // SAFE，故障水位 10
	m.Frame(t0.Add(20*time.Millisecond), 20)
	if m.Safe() {
		t.Fatal("新报文应恢复")
	}
	m.Fail(t0.Add(30 * time.Millisecond)) // 再次 SAFE，故障水位抬升为 20
	if !m.Safe() {
		t.Fatal("应再次进入 SAFE")
	}
	// 上一恢复期的序号 20 与故障前的 10 都已是旧报文。
	for _, seq := range []uint64{10, 20} {
		if r := m.Frame(t0.Add(40*time.Millisecond), seq); r != Rejected {
			t.Fatalf("seq=%d 应判异常, result=%v", seq, r)
		}
	}
	if !m.Safe() {
		t.Fatal("旧报文不得恢复")
	}
	if r := m.Frame(t0.Add(50*time.Millisecond), 21); r != Accepted || m.Safe() {
		t.Fatalf("只有大于新水位的报文才能恢复, result=%v safe=%v", r, m.Safe())
	}
}

// 外部 Reset（看门狗触发安全时调用）同样记录故障水位。
func TestResetRecordsFaultWatermark(t *testing.T) {
	m := New(DefaultConfig())
	m.Frame(t0, 42)
	m.Reset()
	if r := m.Frame(t0.Add(10*time.Millisecond), 42); r != Accepted {
		// 注意：Reset 本身不置 SAFE，此处验证的是水位对 SAFE 恢复的影响，
		// 非 SAFE 下首帧仍建立新基线。
		t.Fatalf("result=%v", r)
	}
	// 进入 SAFE 后水位生效。
	m2 := New(DefaultConfig())
	m2.Frame(t0, 42)
	m2.Fail(t0.Add(10 * time.Millisecond))
	m2.Fail(t0.Add(20 * time.Millisecond))
	m2.Fail(t0.Add(30 * time.Millisecond))
	if !m2.Safe() {
		t.Fatal("应进入 SAFE")
	}
	if r := m2.Frame(t0.Add(40*time.Millisecond), 42); r != Rejected {
		t.Fatalf("SAFE 期间故障前序号应判异常, result=%v", r)
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
