// Package scheduler 实现多路周期采样调度，保证确定性与零累积漂移。
package scheduler

import (
	"sort"
	"time"
)

// Task 描述一路周期采样任务。
type Task struct {
	ID       string
	Period   time.Duration // 采样周期
	Priority int           // 数值小者优先级高
	Deadline time.Duration // 相对计划时刻的允许延迟
}

// Due 表示一次到期采样。
type Due struct {
	Task      Task
	Seq       uint64
	Scheduled time.Time
}

// Scheduler 按周期与优先级调度采样任务。
// 到期时刻始终以 start + k*Period 的整数倍计算，长时间运行无累积漂移。
type Scheduler struct {
	start time.Time
	tasks []taskState
}

type taskState struct {
	task Task
	next time.Time
	seq  uint64
}

// New 创建调度器，所有任务从 start 时刻开始首次到期。
func New(start time.Time, tasks []Task) *Scheduler {
	s := &Scheduler{start: start}
	for _, t := range tasks {
		s.tasks = append(s.tasks, taskState{task: t, next: start})
	}
	return s
}

// DueAt 返回时刻 t 到期（含此前漏处理）的任务集合，
// 按 (Scheduled, Priority, ID) 确定性排序。
// 若时间跨度跨过多个周期，每个被跨过的周期都会各产生一条 Due，
// 不会被静默吞掉；调用方据此判断迟到并做可区分处理。
func (s *Scheduler) DueAt(t time.Time) []Due {
	var out []Due
	for i := range s.tasks {
		ts := &s.tasks[i]
		for !ts.next.After(t) {
			out = append(out, Due{Task: ts.task, Seq: ts.seq, Scheduled: ts.next})
			ts.seq++
			// 逐周期推进：next 始终是 start + k*Period 的网格点，
			// 既不累积漂移，也不吞掉任何被跨过的周期。
			ts.next = ts.next.Add(ts.task.Period)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Scheduled.Equal(out[j].Scheduled) {
			return out[i].Scheduled.Before(out[j].Scheduled)
		}
		if out[i].Task.Priority != out[j].Task.Priority {
			return out[i].Task.Priority < out[j].Task.Priority
		}
		return out[i].Task.ID < out[j].Task.ID
	})
	return out
}

// NextDue 返回下一次有任务到期的时刻。
func (s *Scheduler) NextDue() time.Time {
	var min time.Time
	for i := range s.tasks {
		if min.IsZero() || s.tasks[i].next.Before(min) {
			min = s.tasks[i].next
		}
	}
	return min
}
