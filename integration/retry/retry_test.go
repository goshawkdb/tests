package retry

import (
	"goshawkdb.io/tests/harness"
	"testing"
)

func TestLoop(t *testing.T) {
	for idx := 0; idx < 100; idx++ {
		TestSimpleRetry(t)
		TestDisjointRetry(t)
	}
}

// Test that one write wakes up many retriers
func TestSimpleRetry(t *testing.T) {
	SimpleRetry(harness.NewTestHelper(t))
}

// Test that a retry on several objs gets restarted by one write.
func TestDisjointRetry(t *testing.T) {
	DisjointRetry(harness.NewTestHelper(t))
}
