// Package schedule 实现确定性的多周期采样调度。
//
// 时间语义：
//   - 每路任务的第 n 个时隙 scheduledAt = base + n*Period，截止时刻即计划时刻本身：
//     未能在 scheduledAt 处理即为迟到（Late）或错过（Missed）。
//     计划时刻按绝对倍数计算，长时间运行不产生累积漂移。
//   - 同一时刻到期的多路任务按 (scheduledAt, Priority, ID) 排序派发，顺序完全确定。
//   - 每个时隙都有确定去向：准时执行、迟到执行（Late）或记录为 Missed，
//     不会被静默丢弃，也不会在到期前被提前消费。
package schedule

import (
	"sort"

	"github.com/highcumontoa/realtime-sensor-control-go/clock"
)

// MissPolicy 描述采样错过截止时间时的处理方式。
type MissPolicy int

const (
	// MissMarkLate 迟到采样仍被执行，但标记为 Late。
	MissMarkLate MissPolicy = iota
	// MissSkip 本周期跳过，记录为 Missed，等待下一周期。
	MissSkip
)

// Task 描述一路周期采样。
type Task struct {
	ID       string
	Period   int64 // 采样周期（纳秒），必须为正
	Priority int   // 数值越小优先级越高
	Policy   MissPolicy
}

// Outcome 是一次采样时隙的处理结果。
type Outcome struct {
	TaskID      string
	ScheduledAt int64 // 该时隙的计划时刻
	Deadline    int64 // 该时隙的截止时间
	At          int64 // 实际处理时刻
	Late        bool  // 超过截止时间仍被执行
	Missed      bool  // 被跳过（未执行）
	Err         error // 执行回调返回的错误
}

// taskState 是单路任务的内部调度状态。
type taskState struct {
	task Task
	next int64 // 下一个待处理时隙的 scheduledAt
}

// Scheduler 按周期与优先级确定性地派发采样任务。
type Scheduler struct {
	clk   clock.Clock
	tasks []*taskState
}

// New 创建调度器。
func New(clk clock.Clock) *Scheduler {
	return &Scheduler{clk: clk}
}

// Add 注册一路采样任务，首个时隙为当前时刻之后一个周期。
func (s *Scheduler) Add(t Task) {
	if t.Period <= 0 {
		panic("schedule: period must be positive")
	}
	s.tasks = append(s.tasks, &taskState{task: t, next: s.clk.Now() + t.Period})
}

// RunDue 处理当前时刻所有到期（含已错过）的时隙，返回确定顺序的结果。
// exec 为采样执行回调；返回的错误会记录在对应 Outcome.Err 中，不影响其他时隙。
func (s *Scheduler) RunDue(exec func(taskID string) error) []Outcome {
	now := s.clk.Now()
	var out []Outcome
	for {
		st, ok := s.earliest(now)
		if !ok {
			break
		}
		oc := Outcome{
			TaskID:      st.task.ID,
			ScheduledAt: st.next,
			Deadline:    st.next, // 截止时刻 = 计划时刻
			At:          now,
		}
		pastDeadline := now > oc.Deadline
		// 是否还有更晚的待处理时隙（用于 MarkLate 策略的合并）。
		morePending := st.next+st.task.Period <= now
		switch {
		case !pastDeadline:
			oc.Err = exec(st.task.ID)
		case st.task.Policy == MissMarkLate && !morePending:
			// 最新一个已逾期的时隙仍执行，但标记迟到。
			oc.Late = true
			oc.Err = exec(st.task.ID)
		default:
			// MissSkip 策略，或 MarkLate 策略下被合并掉的中间时隙：
			// 明确记录为 Missed，不静默丢弃。
			oc.Missed = true
		}
		out = append(out, oc)
		st.next += st.task.Period
	}
	return out
}

// earliest 返回当前已到期（scheduledAt <= now）且 scheduledAt 最小的任务。
// 并列时按 (Priority, ID) 决定，保证顺序确定。
func (s *Scheduler) earliest(now int64) (*taskState, bool) {
	var best *taskState
	for _, st := range s.tasks {
		if st.next > now {
			continue
		}
		if best == nil || less(st, best) {
			best = st
		}
	}
	return best, best != nil
}

func less(a, b *taskState) bool {
	if a.next != b.next {
		return a.next < b.next
	}
	if a.task.Priority != b.task.Priority {
		return a.task.Priority < b.task.Priority
	}
	return a.task.ID < b.task.ID
}

// NextDue 返回下一次有任何任务到期的时刻，ok 为 false 表示无任务。
func (s *Scheduler) NextDue() (int64, bool) {
	if len(s.tasks) == 0 {
		return 0, false
	}
	nexts := make([]int64, 0, len(s.tasks))
	for _, st := range s.tasks {
		nexts = append(nexts, st.next)
	}
	sort.Slice(nexts, func(i, j int) bool { return nexts[i] < nexts[j] })
	return nexts[0], true
}
