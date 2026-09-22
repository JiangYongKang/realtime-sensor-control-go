// Package safety 实现看门狗与安全降级。
//
// 语义：
//   - 某一路连续错过/异常达到阈值即判定故障，进入安全状态；
//     因采样周期已知，检测时限上界为 maxMisses * Period，满足约定时限。
//   - 恢复（收到一次正常事件）后立即回到正常状态，并重置全部计数，
//     故障期间累积的状态不被沿用。
//   - 全部方法可并发调用，状态读写不会出现半更新。
package safety

import "sync"

// State 表示系统安全状态。
type State int

const (
	// StateNormal 正常运行。
	StateNormal State = iota
	// StateSafe 已进入安全降级状态。
	StateSafe
)

func (s State) String() string {
	if s == StateSafe {
		return "SAFE"
	}
	return "NORMAL"
}

// Watchdog 监控某一路采样/报文的连续性。
type Watchdog struct {
	mu         sync.Mutex
	maxMisses  int
	consecMiss int
	state      State
}

// NewWatchdog 创建看门狗，maxMisses 为触发安全状态所需的连续异常次数。
func NewWatchdog(maxMisses int) *Watchdog {
	if maxMisses < 1 {
		maxMisses = 1
	}
	return &Watchdog{maxMisses: maxMisses}
}

// Feed 报告一次正常事件。若此前处于安全状态，则恢复正常并重置计数，
// 故障期间的状态（连续错过计数）不会被沿用。
func (w *Watchdog) Feed() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.consecMiss = 0
	w.state = StateNormal
}

// Miss 报告一次错过/异常事件，返回本次调用后是否处于安全状态。
func (w *Watchdog) Miss() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.consecMiss++
	if w.consecMiss >= w.maxMisses {
		w.state = StateSafe
	}
	return w.state == StateSafe
}

// State 返回当前安全状态。
func (w *Watchdog) State() State {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state
}

// ConsecutiveMisses 返回当前连续异常计数（主要用于测试与观测）。
func (w *Watchdog) ConsecutiveMisses() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.consecMiss
}
