package simpleconflict

import (
	"goshawkdb.io/tests/harness"
	"testing"
)

func TestSimpleConflict(t *testing.T) {
	SimpleConflict(harness.NewTestHelper(t))
}
