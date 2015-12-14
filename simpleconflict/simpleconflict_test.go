package simpleconflict

import (
	"goshawkdb.io/tests"
	"testing"
)

func TestSimpleConflict(t *testing.T) {
	hosts := []string{"localhost:10001", "localhost:10002", "localhost:10003"}
	SimpleConflict(tests.NewTestHelper(t, hosts...))
}
