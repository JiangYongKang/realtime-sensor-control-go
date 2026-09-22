package safety

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestTripAfterMaxMisses(t *testing.T) {
	w := &Watchdog{MaxMisses: 3, Timeout: time.Second}
	for i := 1; i <= 2; i++ {
		if st := w.Update(Event{At: t0.Add(time.Duration(i) * 100 * time.Millisecond)}); st != StateOK {
			t.Fatalf("miss %d: state=%v, want OK", i, st)
		}
	}
	if st := w.Update(Event{At: t0.Add(300 * time.Millisecond)}); st != StateSafe {
		t.Fatalf("miss 3: state=%v, want SAFE", st)
	}
}

func TestOnTimeResetsMisses(t *testing.T) {
	w := &Watchdog{MaxMisses: 2, Timeout: time.Second}
	w.Update(Event{At: t0})
	if st := w.Update(Event{At: t0.Add(100 * time.Millisecond), OnTime: true}); st != StateOK {
		t.Fatalf("state=%v", st)
	}
	if w.Misses() != 0 {
		t.Fatalf("misses=%d, want 0", w.Misses())
	}
	w.Update(Event{At: t0.Add(200 * time.Millisecond)})
	if st := w.Update(Event{At: t0.Add(300 * time.Millisecond)}); st != StateSafe {
		t.Fatalf("state=%v, want SAFE", st)
	}
}

func TestSilenceTimeout(t *testing.T) {
	w := &Watchdog{MaxMisses: 5, Timeout: 500 * time.Millisecond}
	w.Update(Event{At: t0, OnTime: true})
	if st := w.Tick(t0.Add(400 * time.Millisecond)); st != StateOK {
		t.Fatalf("state=%v, want OK", st)
	}
	if st := w.Tick(t0.Add(600 * time.Millisecond)); st != StateSafe {
		t.Fatalf("state=%v, want SAFE", st)
	}
}

func TestRecoveryOnFreshOnTimeEvent(t *testing.T) {
	w := &Watchdog{MaxMisses: 1, Timeout: time.Second}
	w.Update(Event{At: t0})
	if w.State() != StateSafe {
		t.Fatal("should be SAFE")
	}
	if st := w.Update(Event{At: t0.Add(time.Second), OnTime: true}); st != StateOK {
		t.Fatalf("state=%v, want OK after fresh on-time event", st)
	}
}
