// Package safety 实现看门狗与安全降级。
package safety

import "time"

// State 表示通道安全状态。
type State int

const (
	StateOK State = iota
	// StateSafe 表示已进入安全降级状态。
	StateSafe
)

func (s State) String() string {
	if s == StateSafe {
		return "SAFE"
	}
	return "OK"
}

// Watchdog 监控单路采样：连续错过截止时间达到 MaxMisses，
// 或超过 Timeout 没有任何采样事件，即进入安全状态。
// 收到按时采样后恢复为 OK（恢复判定只依据恢复后的新事件）。
type Watchdog struct {
	MaxMisses int
	Timeout   time.Duration

	state     State
	misses    int
	lastEvent time.Time
	hasEvent  bool
}

// Event 是看门狗输入事件。
type Event struct {
	At     time.Time
	OnTime bool // 本次采样是否在截止时间内完成
}

// Update 处理一次采样事件，返回当前状态。
func (w *Watchdog) Update(ev Event) State {
	if ev.OnTime {
		w.misses = 0
	} else {
		w.misses++
	}
	w.lastEvent, w.hasEvent = ev.At, true
	switch {
	case w.misses >= w.MaxMisses:
		w.state = StateSafe
	case ev.OnTime:
		w.state = StateOK
	}
	return w.state
}

// Tick 在无采样事件时按时间推进检查静默超时。
func (w *Watchdog) Tick(now time.Time) State {
	if w.hasEvent && w.Timeout > 0 && now.Sub(w.lastEvent) > w.Timeout {
		w.state = StateSafe
	}
	return w.state
}

// State 返回当前状态。
func (w *Watchdog) State() State {
	return w.state
}

// Misses 返回当前连续错过次数。
func (w *Watchdog) Misses() int {
	return w.misses
}
