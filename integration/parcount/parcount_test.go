package parcount

import (
	"goshawkdb.io/tests/harness"
	"testing"
)

func TestParCount(t *testing.T) {
	ParCount(harness.NewTestHelper(t))
}
