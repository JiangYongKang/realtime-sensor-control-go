// Package busfault 实现通道级总线报文异常监控。
//
// 每路通道独立持有一个 Monitor：连续（或可选时间窗内）出现无法接受的
// 报文即按确定性阈值进入安全状态；恢复只依据恢复之后的新报文，不沿用
// 故障前的序号基线与累计异常。
//
// 判定规则（确定且可配置，默认值稳定可复现）：
//
//   - 一次异常：结构不完整、完整性校验失败、版本不接受、字段非法，或
//     序号重复/倒退超过容忍量，各计一次异常（Fail）。
//   - 降级：连续异常达到 MaxConsecutive（默认 3）；若设置 Window>0，
//     则窗口内异常数达到 MaxConsecutive 即降级（不要求严格连续）。
//   - 夹杂正常帧：任一被接受的报文（含容忍范围内的乱序帧与活性帧）
//     立即清零连续异常计数。
//   - 乱序：序号回退量不超过 ReorderTolerance（默认 0）的帧被接受，
//     但不推进序号基线；回退超过容忍量或序号重复按异常处理。
//   - 恢复：SAFE 后需要连续 RecoveryFrames（默认 1）帧“故障后新报文”才回
//     OK。故障后新报文指序号严格大于故障前已接受最大序号（恢复门槛）的帧；
//     与门槛相同（重复故障前序号）、比门槛更旧（倒退）、容忍范围内乱序帧
//     都不算恢复：不推进恢复进度，并把已累计的恢复进度清零重算；只有真正
//     更新的报文才推进恢复进度并最终驱动输出。不带序号的活性帧无法证明其
//     产生于故障之后，SAFE 期间既不推进也不清零恢复进度。
//   - 进入 SAFE 时丢弃连续异常计数、时间窗历史与恢复进度，但保留故障前已
//     接受的最大序号作为恢复门槛；外部 Reset（如看门狗联动要求全清）才丢弃
//     门槛，此后首帧建立全新基线。
package busfault

import "time"

// Result 是一次报文判定结果。
type Result int

const (
	// Accepted 报文有效，已更新序号基线。
	Accepted Result = iota
	// AcceptedReordered 报文在乱序容忍范围内，已接受但不推进基线。
	AcceptedReordered
	// Liveness 报文结构合法但不带序号，仅作为活性信号（清零异常计数）。
	Liveness
	// Rejected 报文无法接受，计一次异常。
	Rejected
)

// 默认配置，保证零配置下行为稳定可复现。
const (
	DefaultMaxConsecutive = 3
	DefaultRecoveryFrames = 1
)

// Config 是监控器配置，零值经 Normalize 后取稳定默认值。
type Config struct {
	// MaxConsecutive 连续无法接受报文的降级阈值（默认 3）。
	MaxConsecutive int
	// Window 为异常时间窗；>0 时窗口内异常数达到 MaxConsecutive 即降级，
	// 不要求异常严格连续；0 表示仅按连续异常计数（默认）。
	Window time.Duration
	// ReorderTolerance 允许的序号回退量；回退超过该值视为真正倒退。
	// 0 表示任何倒退都按异常处理（默认）。
	ReorderTolerance uint64
	// RecoveryFrames 恢复所需连续有效报文数（默认 1）。
	RecoveryFrames int
}

// DefaultConfig 返回稳定默认配置。
func DefaultConfig() Config {
	return Config{
		MaxConsecutive:   DefaultMaxConsecutive,
		RecoveryFrames:   DefaultRecoveryFrames,
		ReorderTolerance: 0,
	}
}

// Normalize 补全默认值并返回规范化配置。
func (c Config) Normalize() Config {
	if c.MaxConsecutive <= 0 {
		c.MaxConsecutive = DefaultMaxConsecutive
	}
	if c.RecoveryFrames <= 0 {
		c.RecoveryFrames = DefaultRecoveryFrames
	}
	if c.Window < 0 {
		c.Window = 0
	}
	return c
}

// Monitor 监控单路通道的总线报文质量。非并发安全：由上层串行化访问。
type Monitor struct {
	cfg Config

	safe     bool
	consec   int // 当前连续异常数
	recovery int // SAFE 后连续“故障后新报文”数
	// hasBaseline/baseline 为已接受的最大序号（高水位）。进入 SAFE 时
	// 不丢弃，作为恢复门槛：恢复报文必须严格大于 baseline。
	hasBaseline bool
	baseline    uint64
	failTimes   []time.Time // Window>0 时记录的异常时刻（有界）
	accepted    uint64
	rejected    uint64
}

// New 创建监控器。
func New(cfg Config) *Monitor {
	return &Monitor{cfg: cfg.Normalize()}
}

// Frame 处理一帧带序号报文，at 为到达时刻，返回判定结果。
func (m *Monitor) Frame(at time.Time, seq uint64) Result {
	m.prune(at)
	if !m.hasBaseline {
		// 启动后或外部 Reset 后的首帧：建立全新序号基线。SAFE 期间它
		// 也被视为故障后新报文（此前没有任何已接受序号可作为门槛）。
		m.hasBaseline = true
		m.baseline = seq
		return m.noteFresh(at, Accepted)
	}
	switch {
	case seq > m.baseline:
		m.baseline = seq
		return m.noteFresh(at, Accepted)
	case seq == m.baseline:
		m.Fail(at) // 序号重复（含故障前最后序号）：计一次异常
		return Rejected
	default:
		gap := m.baseline - seq
		if gap <= m.cfg.ReorderTolerance {
			// 容忍范围内的迟到乱序帧：接受但不推进基线，避免旧帧拉低基线。
			return m.noteTolerated(at, AcceptedReordered)
		}
		m.Fail(at) // 真正倒退（比故障前已接受序号更旧）：计一次异常
		return Rejected
	}
}

// Alive 处理一帧结构合法但不带序号的报文：正常状态下作为活性信号清零
// 连续异常计数。SAFE 期间它无法证明产生于故障之后（没有序号），因此
// 不推进恢复进度，也不清零已经累计的恢复进度；不建立或推进序号基线。
func (m *Monitor) Alive(at time.Time) Result {
	m.prune(at)
	m.accepted++
	m.consec = 0
	return Liveness
}

// Fail 记录一次无法接受的报文（结构不完整/校验失败/版本/字段非法等）。
func (m *Monitor) Fail(at time.Time) {
	m.prune(at)
	m.consec++
	m.rejected++
	m.recovery = 0 // SAFE 期间的恢复进度被一次异常打断
	if m.cfg.Window > 0 {
		m.failTimes = append(m.failTimes, at)
	}
	if m.consec >= m.cfg.MaxConsecutive ||
		(m.cfg.Window > 0 && len(m.failTimes) >= m.cfg.MaxConsecutive) {
		m.trip()
	}
}

// Tick 按当前时刻重评时间窗；窗口异常数仍达阈值时保持/进入安全状态。
func (m *Monitor) Tick(now time.Time) {
	m.prune(now)
	if !m.safe && m.cfg.Window > 0 && len(m.failTimes) >= m.cfg.MaxConsecutive {
		m.trip()
	}
}

// noteFresh 处理“故障后新报文”（序号严格大于高水位，或无基线后的首帧）：
// 清零连续异常计数；SAFE 期间推进恢复进度，集齐 RecoveryFrames 帧后回 OK。
// 注意：连续异常计数清零，但时间窗内的失败历史保留——窗口模式的意义
// 正是让夹杂正常帧的坏帧突发在窗口累计达标时仍能触发降级。
func (m *Monitor) noteFresh(at time.Time, r Result) Result {
	m.accepted++
	m.consec = 0
	if m.safe {
		m.recovery++
		if m.recovery >= m.cfg.RecoveryFrames {
			m.safe = false
			m.recovery = 0
		}
	}
	return r
}

// noteTolerated 处理容忍范围内的乱序帧：清零连续异常计数，但它携带的是
// 旧序号，不是故障后新报文——SAFE 期间不得推进恢复进度，并把此前累计的
// 恢复进度清零，恢复进度只由连续的真正更新报文重新建立。
func (m *Monitor) noteTolerated(at time.Time, r Result) Result {
	m.accepted++
	m.consec = 0
	if m.safe {
		m.recovery = 0
	}
	return r
}

// trip 进入安全状态：丢弃连续异常、时间窗历史与恢复进度，但保留序号高水
// 位作为恢复门槛，保证故障前用过的序号无法触发恢复、无法驱动输出。
func (m *Monitor) trip() {
	m.safe = true
	m.consec = 0
	m.recovery = 0
	m.failTimes = m.failTimes[:0]
}

// Reset 丢弃序号基线、连续异常、时间窗与恢复进度，但不改变 SAFE 标志。
// 供看门狗联动等外部场景使用：外部已通过其他故障源决定通道安全性，这里
// 只丢弃故障前累计；此后首帧建立全新基线。总线自身降级走 trip，保留门槛。
func (m *Monitor) Reset() {
	m.hasBaseline = false
	m.baseline = 0
	m.consec = 0
	m.recovery = 0
	m.failTimes = m.failTimes[:0]
}

// RecoveryProgress 返回 SAFE 后已连续收到的故障后新报文数（0..RecoveryFrames-1）。
func (m *Monitor) RecoveryProgress() int { return m.recovery }

// prune 丢弃已滑出时间窗的异常时刻（at-t >= Window 视为过期）。
func (m *Monitor) prune(at time.Time) {
	if m.cfg.Window <= 0 || len(m.failTimes) == 0 {
		return
	}
	cut := 0
	for _, ft := range m.failTimes {
		if at.Sub(ft) < m.cfg.Window {
			m.failTimes[cut] = ft
			cut++
		}
	}
	m.failTimes = m.failTimes[:cut]
	// 窗口内失败已过期：它们不再构成连续异常，连续计数随之遗忘。
	if m.consec > cut {
		m.consec = cut
	}
}

// Safe 报告是否处于安全降级状态。
func (m *Monitor) Safe() bool { return m.safe }

// Consecutive 返回当前连续异常数。
func (m *Monitor) Consecutive() int { return m.consec }

// AcceptedTotal 返回累计接受报文数（含乱序容忍帧与活性帧）。
func (m *Monitor) AcceptedTotal() uint64 { return m.accepted }

// RejectedTotal 返回累计异常报文数。
func (m *Monitor) RejectedTotal() uint64 { return m.rejected }
