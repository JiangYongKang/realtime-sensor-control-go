package schedule

import (
	"fmt"
	"testing"

	"github.com/highcumontoa/realtime-sensor-control-go/clock"
)

func runIDs(out []Outcome) []string {
	ids := make([]string, len(out))
	for i, o := range out {
		ids[i] = o.TaskID
	}
	return ids
}

func TestDeterministicOrderByPriorityAndTime(t *testing.T) {
	clk := clock.NewManual(0)
	s := New(clk)
	s.Add(Task{ID: "slow", Period: 100, Priority: 2})
	s.Add(Task{ID: "fast", Period: 50, Priority: 1})
	s.Add(Task{ID: "mid", Period: 100, Priority: 1})

	clk.Advance(100)
	out := s.RunDue(func(id string) error { return nil })
	// fast 的 t=50 时隙已逾期且非最新，记为 Missed（不静默丢弃）。
	var missed int
	var executed []string
	for _, o := range out {
		if o.Missed {
			missed++
			continue
		}
		executed = append(executed, o.TaskID)
	}
	if missed != 1 {
		t.Fatalf("missed = %d, want 1: %+v", missed, out)
	}
	// t=100 同时刻到期按 (Priority, ID) 排序：fast(P1) < mid(P1) < slow(P2)。
	if fmt.Sprint(executed) != fmt.Sprint([]string{"fast", "mid", "slow"}) {
		t.Fatalf("executed order = %v: %+v", executed, out)
	}
	// 再次运行确认顺序确定：先推进到共同到期点。
	clk2 := clock.NewManual(0)
	s2 := New(clk2)
	s2.Add(Task{ID: "b", Period: 100, Priority: 2})
	s2.Add(Task{ID: "a", Period: 100, Priority: 1})
	s2.Add(Task{ID: "c", Period: 100, Priority: 1})
	clk2.Advance(100)
	got := runIDs(s2.RunDue(func(string) error { return nil }))
	// 首个时隙在 t=100，三任务同时到期。
	want := []string{"a", "c", "b"} // 同时刻：P1(a,c 按 ID) 先于 P2(b)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestNoCumulativeDrift(t *testing.T) {
	clk := clock.NewManual(0)
	s := New(clk)
	s.Add(Task{ID: "x", Period: 7, Priority: 0})
	const n = 10000
	for i := 1; i <= n; i++ {
		clk.Advance(7)
		out := s.RunDue(func(string) error { return nil })
		if len(out) != 1 {
			t.Fatalf("tick %d: got %d outcomes", i, len(out))
		}
		if out[0].ScheduledAt != int64(i)*7 {
			t.Fatalf("tick %d: scheduledAt = %d, want %d", i, out[0].ScheduledAt, i*7)
		}
		if out[0].Late || out[0].Missed {
			t.Fatalf("tick %d: unexpected late/missed", i)
		}
	}
}

func TestNotConsumedEarly(t *testing.T) {
	clk := clock.NewManual(0)
	s := New(clk)
	s.Add(Task{ID: "x", Period: 100, Priority: 0})
	clk.Advance(99)
	if out := s.RunDue(func(string) error { return nil }); len(out) != 0 {
		t.Fatalf("consumed early: %+v", out)
	}
}

func TestMissSkipPolicy(t *testing.T) {
	clk := clock.NewManual(0)
	s := New(clk)
	s.Add(Task{ID: "x", Period: 100, Priority: 0, Policy: MissSkip})
	clk.Advance(350) // t=100,200,300 三个时隙均已错过
	out := s.RunDue(func(string) error { return nil })
	var missed, ran int
	for _, o := range out {
		if o.Missed {
			missed++
		} else {
			ran++
		}
	}
	if missed != 3 || ran != 0 {
		t.Fatalf("missed=%d ran=%d, want 3/0: %+v", missed, ran, out)
	}
	// 下一周期（t=400）准时运行则恢复正常执行。
	clk.Advance(50)
	out = s.RunDue(func(string) error { return nil })
	if len(out) != 1 || out[0].Missed || out[0].Late {
		t.Fatalf("expected clean run after stall, got %+v", out)
	}
}

func TestMarkLatePolicyCoalesces(t *testing.T) {
	clk := clock.NewManual(0)
	s := New(clk)
	s.Add(Task{ID: "x", Period: 100, Priority: 0, Policy: MissMarkLate})
	clk.Advance(250) // t=100 逾期被合并，t=200 逾期但为最新时隙
	executed := 0
	out := s.RunDue(func(string) error { executed++; return nil })
	var late, missed int
	for _, o := range out {
		if o.Late {
			late++
		}
		if o.Missed {
			missed++
		}
	}
	if late != 1 || missed != 1 || executed != 1 {
		t.Fatalf("late=%d missed=%d executed=%d: %+v", late, missed, executed, out)
	}
}

func TestExecErrorRecorded(t *testing.T) {
	clk := clock.NewManual(0)
	s := New(clk)
	s.Add(Task{ID: "x", Period: 100, Priority: 0})
	clk.Advance(100)
	want := fmt.Errorf("boom")
	out := s.RunDue(func(string) error { return want })
	if len(out) != 1 { // 首个时隙 t=100，准时执行
		t.Fatalf("got %d outcomes", len(out))
	}
	if out[0].Err != want {
		t.Fatalf("Err = %v, want %v", out[0].Err, want)
	}
}
