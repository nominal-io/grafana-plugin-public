package plugin

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
)

// expectedRecoveredPanics tallies the panics tests deliberately trigger via
// withExpectedRecoveredPanics. TestMain fails the run when recoveredPanics
// drifts from it.
var expectedRecoveredPanics atomic.Int64

// withExpectedRecoveredPanics runs fn, asserts it contained exactly n panics,
// and registers them with the TestMain guard. Deriving the expectation from
// the measured delta keeps the comparison exact under -count>1 and -run
// filters; a wrong n fails this test instead of leaking slack into the guard.
func withExpectedRecoveredPanics(t *testing.T, n int64, fn func()) {
	t.Helper()
	before := recoveredPanics.Load()
	fn()
	if got := recoveredPanics.Load() - before; got != n {
		t.Fatalf("expected fn to contain %d panics, got %d", n, got)
	}
	expectedRecoveredPanics.Add(n)
}

// A contained panic produces an error response instead of crashing the test
// binary, so a test with weak assertions can pass straight through one. This
// guard fails the package whenever a panic was recovered without a test
// declaring it via withExpectedRecoveredPanics.
func TestMain(m *testing.M) {
	code := m.Run()
	if got, want := recoveredPanics.Load(), expectedRecoveredPanics.Load(); got != want {
		fmt.Fprintf(os.Stderr,
			"panic containment guard: %d panics recovered, %d expected; "+
				"a test swallowed a contained panic. Search this output for "+
				"%q, the logged stack names the offending test.\n",
			got, want, "Recovered panic while")
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}
