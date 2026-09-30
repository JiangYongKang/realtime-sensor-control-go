// Package busguard 实现单路通道的总线报文安全判定。
//
// 它把“无法接受的总线报文”（结构不完整、完整性校验不过、版本不接受、
// 字段非法、序号重复或明显倒退）按连续异常帧数与异常持续时间计数，
// 达到阈值即让该通道进入 SAFE；夹杂一帧可接受报文即清零；乱序在
// 容忍窗口内不计数；恢复后只能使用序号严格新于故障前峰值的报文。
//
// 判定规则（确定、可配置，零值配置填充为默认值）：
//
//   - 一次异常：解码/校验/版本/字段失败，或序号与上一接受序号重复，
//     或序号倒退幅度超过 ReorderWindow。异常连续累计，达到
//     MaxConsecutiveBad 帧，或从首个异常起经过 FaultWindow，即触发 SAFE。
//   - 清零：只有序号严格前进的可接受报文才把连续异常计数清零；
//     容忍窗口内的乱序帧不计数也不清零。
//   - 乱序：序号回退幅度在 1..ReorderWindow 内（含）判定为乱序，
//     该帧不用于控制输出但不计异常；超过窗口判定为明显倒退（一次异常）。
//   - 恢复门：进入 SAFE 时记录故障前接受序号峰值 floor；SAFE 期间
//     序号 <= floor 的可接受报文一律视为陈旧（Stale）而丢弃，只有
//     seq > floor 的新报文才恢复 OK 并被采用。
package busguard

import "time"

// 触发降级的默认配置，零值 Config 填充后使用，保证可复现。
const (
	// DefaultMaxConsecutiveBad 是连续不可接受报文帧数的默认阈值。
	DefaultMaxConsecutiveBad = 3
	// DefaultFaultWindow 是连续异常持续时间的默认阈值。
	DefaultFaultWindow = time.Second
	// DefaultReorderWindow 是默认可容忍的序号回退幅度。
	DefaultReorderWindow uint64 = 1
)

// Config 配置一路通道的总线安全判定。零值字段填充为默认值。
type Config struct {
	MaxConsecutiveBad int           // 连续不可接受报文帧数阈值；<=0 用默认值
	FaultWindow       time.Duration // 连续异常持续时间阈值；<=0 用默认值
	ReorderWindow     uint64        // 可容忍的序号回退幅度；0 用默认值
}

// Status 是总线安全判定的状态快照。
type Status struct {
	Safe         bool
	BadStreak    int
	BadTotal     uint64
	ReorderTotal uint64
	LastSeq      uint64
	AppliedTotal uint64
}

// Disposition 描述一帧报文的判定结果。
type Disposition int

const (
	// DispBad 表示不可接受，计入连续异常。
	DispBad Disposition = iota
	// DispReordered 表示乱序但在容忍窗口内，不计数也不采用。
	DispReordered
	// DispStale 表示序号不新于故障前峰值，被恢复门丢弃。
	DispStale
	// DispApplied 表示接受，报文可用于控制输出。
	DispApplied
)

// Guard 监控单路通道的总线报文。零值不可用，须经 New 创建。
type Guard struct {
	cfg Config

	safe       bool
	badStreak  int
	badTotal   uint64
	reorderTot uint64
	appliedTot uint64
	lastSeq    uint64 // 已接受报文的序号峰值
	hasSeq     bool
	badStart   time.Time // 当前连续异常区间首帧到达时刻
	hasFloor   bool
	floor      uint64 // 恢复门序号：SAFE 期间 seq<=floor 一律丢弃
}

// New 创建总线安全判定器。零值字段填充为稳定默认值。
func New(cfg Config) *Guard {
	if cfg.MaxConsecutiveBad <= 0 {
		cfg.MaxConsecutiveBad = DefaultMaxConsecutiveBad
	}
	if cfg.FaultWindow <= 0 {
		cfg.FaultWindow = DefaultFaultWindow
	}
	if cfg.ReorderWindow == 0 {
		cfg.ReorderWindow = DefaultReorderWindow
	}
	return &Guard{cfg: cfg}
}

// Observe 处理一帧已通过结构与字段校验、属于本通道的报文，
// at 为到达时刻，seq 为报文序号，返回判定结果。
func (g *Guard) Observe(at time.Time, seq uint64) Disposition {
	if g.safe {
		// 恢复门：只接受序号严格新于故障前峰值的报文。
		if g.hasFloor && seq <= g.floor {
			return DispStale
		}
		g.recover(seq)
		return DispApplied
	}
	if !g.hasSeq {
		// 首帧：直接接受，建立序号基线，并清零此前的异常计数
		// （通道尚无数号基线，坏帧无从归属到有效序列，不累计）。
		g.hasSeq = true
		g.lastSeq = seq
		g.badStreak = 0
		g.badStart = time.Time{}
		g.appliedTot++
		return DispApplied
	}
	switch {
	case seq > g.lastSeq:
		// 正常前进：采用并清零连续异常。
		g.badStreak = 0
		g.badStart = time.Time{}
		g.lastSeq = seq
		g.appliedTot++
		return DispApplied
	case seq == g.lastSeq:
		// 重复序号算一次异常。
		return g.countBad(at)
	default:
		gap := g.lastSeq - seq
		if gap <= g.cfg.ReorderWindow {
			// 乱序但在容忍窗口内：不计数、不清零、不采用。
			g.reorderTot++
			return DispReordered
		}
		// 明显倒退：一次异常。
		return g.countBad(at)
	}
}

// Fault 记录一帧不可接受报文（结构不完整、校验失败、版本不接受、
// 字段非法）。属于本通道的坏帧由调用方据此归属。
func (g *Guard) Fault(at time.Time) {
	g.countBad(at)
}

// Invalidate 使总线侧安全失效（例如采样看门狗已进入 SAFE）。
// 恢复门同步生效：之后只有序号更新的报文才能让总线侧恢复。
func (g *Guard) Invalidate() {
	g.trip()
}

// Tick 在时间推进时检查异常持续时间窗口；到达 FaultWindow 即触发 SAFE。
func (g *Guard) Tick(now time.Time) {
	if g.safe || g.badStreak == 0 || g.cfg.FaultWindow <= 0 {
		return
	}
	if now.Sub(g.badStart) >= g.cfg.FaultWindow {
		g.trip()
	}
}

// Status 返回一致性快照。
func (g *Guard) Status() Status {
	return Status{
		Safe:         g.safe,
		BadStreak:    g.badStreak,
		BadTotal:     g.badTotal,
		ReorderTotal: g.reorderTot,
		LastSeq:      g.lastSeq,
		AppliedTotal: g.appliedTot,
	}
}

// countBad 累计一次异常并检查帧数/时间阈值（调用方保证非 SAFE 语义）。
func (g *Guard) countBad(at time.Time) Disposition {
	g.badTotal++
	g.badStreak++
	if g.badStreak == 1 {
		g.badStart = at
	}
	if g.cfg.MaxConsecutiveBad > 0 && g.badStreak >= g.cfg.MaxConsecutiveBad {
		g.trip()
	} else if g.cfg.FaultWindow > 0 && at.Sub(g.badStart) >= g.cfg.FaultWindow {
		g.trip()
	}
	return DispBad
}

// trip 进入 SAFE 并建立恢复门，记录故障前序号峰值。
func (g *Guard) trip() {
	if g.safe {
		return
	}
	g.safe = true
	g.hasFloor = g.hasSeq
	g.floor = g.lastSeq
}

// recover 用一帧更新序号的报文恢复 OK。
func (g *Guard) recover(seq uint64) {
	g.safe = false
	g.badStreak = 0
	g.badStart = time.Time{}
	g.hasFloor = false
	g.hasSeq = true
	g.lastSeq = seq
	g.appliedTot++
}
