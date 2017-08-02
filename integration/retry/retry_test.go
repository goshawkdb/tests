package retry

import (
	"goshawkdb.io/tests"
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
	SimpleRetry(tests.NewTestHelper(t))
}

// Test that a retry on several objs gets restarted by one write.
func TestDisjointRetry(t *testing.T) {
	DisjointRetry(tests.NewTestHelper(t))
}
