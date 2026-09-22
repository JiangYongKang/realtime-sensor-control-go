package scheduler

import (
	"testing"
	"time"
)

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func tasks() []Task {
	return []Task{
		{ID: "b", Period: 200 * time.Millisecond, Priority: 1, Deadline: 20 * time.Millisecond},
		{ID: "a", Period: 100 * time.Millisecond, Priority: 0, Deadline: 10 * time.Millisecond},
	}
}

func TestDeterministicOrder(t *testing.T) {
	s := New(start, tasks())
	dues := s.DueAt(start.Add(200 * time.Millisecond))
	// t=0: a,b；t=100ms: a；t=200ms: a,b —— 同时刻按优先级再按 ID 排序。
	type key struct {
		id  string
		off time.Duration
	}
	want := []key{{"a", 0}, {"b", 0}, {"a", 100 * time.Millisecond}, {"a", 200 * time.Millisecond}, {"b", 200 * time.Millisecond}}
	if len(dues) != len(want) {
		t.Fatalf("got %d dues, want %d", len(dues), len(want))
	}
	for i, w := range want {
		if dues[i].Task.ID != w.id || !dues[i].Scheduled.Equal(start.Add(w.off)) {
			t.Fatalf("due[%d] = %s@%v, want %s@%v", i, dues[i].Task.ID, dues[i].Scheduled, w.id, start.Add(w.off))
		}
	}
}

func TestNoCumulativeDrift(t *testing.T) {
	s := New(start, []Task{{ID: "x", Period: 3 * time.Millisecond, Priority: 0, Deadline: time.Millisecond}})
	// 以非网格对齐的步长推进 1000 次，网格点必须始终是 start + k*3ms。
	step := 7 * time.Millisecond
	now := start
	for i := 0; i < 1000; i++ {
		now = now.Add(step)
		for _, d := range s.DueAt(now) {
			off := d.Scheduled.Sub(start)
			if off%(3*time.Millisecond) != 0 {
				t.Fatalf("scheduled %v off grid by %v", d.Scheduled, off%(3*time.Millisecond))
			}
		}
	}
	// 最终 next 必须精确落在网格上。
	if off := s.NextDue().Sub(start); off%(3*time.Millisecond) != 0 {
		t.Fatalf("next due off grid: %v", off)
	}
}

func TestBacklogNotDropped(t *testing.T) {
	s := New(start, []Task{{ID: "x", Period: 100 * time.Millisecond, Priority: 0, Deadline: 10 * time.Millisecond}})
	// 一次性推进 1 秒，跨过的 10 个周期都必须产生 Due。
	dues := s.DueAt(start.Add(time.Second))
	if len(dues) != 11 { // t=0 .. t=1s
		t.Fatalf("got %d dues, want 11", len(dues))
	}
	for i, d := range dues {
		if d.Seq != uint64(i) {
			t.Fatalf("seq = %d, want %d", d.Seq, i)
		}
	}
}

func TestSeqMonotonicAcrossCalls(t *testing.T) {
	s := New(start, []Task{{ID: "x", Period: 100 * time.Millisecond, Priority: 0, Deadline: 10 * time.Millisecond}})
	s.DueAt(start) // 消费 t=0 的首次到期
	var last uint64
	for i := 0; i < 10; i++ {
		dues := s.DueAt(start.Add(time.Duration(i+1) * 100 * time.Millisecond))
		if len(dues) != 1 {
			t.Fatalf("got %d dues, want 1", len(dues))
		}
		if dues[0].Seq != last+1 {
			t.Fatalf("seq jump: %d -> %d", last, dues[0].Seq)
		}
		last = dues[0].Seq
	}
}
