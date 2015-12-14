package parcount

import (
	"goshawkdb.io/tests"
	"testing"
)

func TestParCount(t *testing.T) {
	hosts := []string{"localhost:10001", "localhost:10002", "localhost:10003"}
	ParCount(tests.NewTestHelper(t, hosts...))
}
