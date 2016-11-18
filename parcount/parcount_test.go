package parcount

import (
	"goshawkdb.io/tests"
	"testing"
)

func TestParCount(t *testing.T) {
	ParCount(tests.NewTestHelper(t))
}
