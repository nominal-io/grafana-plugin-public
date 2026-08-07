package plugin

import "sync/atomic"

// recoveredPanics counts panics converted into error responses by the
// containment recover blocks. Containment hides panics from the test binary,
// which would otherwise crash and fail CI; TestMain compares this counter
// against declared expectations so a contained panic still fails the suite.
var recoveredPanics atomic.Int64

// noteRecoveredPanic records one contained panic. Every recover block that
// converts a panic into an error response must call it.
func noteRecoveredPanic() {
	recoveredPanics.Add(1)
}
