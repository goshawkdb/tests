package simpleconflict

import (
	"goshawkdb.io/tests"
	"testing"
)

func TestSimpleConflict(t *testing.T) {
	SimpleConflict(tests.NewTestHelper(t))
}
