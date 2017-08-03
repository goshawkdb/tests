package pqueue

import (
	"goshawkdb.io/tests/harness"
	"testing"
)

func TestPQueue(t *testing.T) {
	PQueue(harness.NewTestHelper(t))
}
