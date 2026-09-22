package safety

import (
	"sync"
	"testing"
)

func TestTripAfterConsecutiveMisses(t *testing.T) {
	w := NewWatchdog(3)
	if w.Miss() || w.Miss() {
		t.Fatal("tripped before threshold")
	}
	if !w.Miss() {
		t.Fatal("not tripped at threshold")
	}
	if w.State() != StateSafe {
		t.Fatal("state should be SAFE")
	}
}

func TestFeedResetsConsecutiveCount(t *testing.T) {
	w := NewWatchdog(2)
	w.Miss()
	w.Feed() // 打断连续计数
	if w.Miss() {
		t.Fatal("should not trip: counter was reset by Feed")
	}
	if !w.Miss() {
		t.Fatal("should trip after two consecutive misses")
	}
}

func TestRecoveryDoesNotReuseFaultState(t *testing.T) {
	w := NewWatchdog(2)
	w.Miss()
	w.Miss()
	if w.State() != StateSafe {
		t.Fatal("expected SAFE")
	}
	w.Feed()
	if w.State() != StateNormal {
		t.Fatal("expected recovery to NORMAL")
	}
	if w.ConsecutiveMisses() != 0 {
		t.Fatalf("fault-period count leaked: %d", w.ConsecutiveMisses())
	}
	// 恢复后需重新累计满阈值才再次触发。
	if w.Miss() {
		t.Fatal("tripped immediately after recovery: fault state reused")
	}
}

func TestConcurrentAccess(t *testing.T) {
	w := NewWatchdog(5)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if i%2 == 0 {
					w.Miss()
				} else {
					w.Feed()
				}
				_ = w.State()
			}
		}(i)
	}
	wg.Wait()
}
