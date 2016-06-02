package solocount

import (
	"goshawkdb.io/tests"
	"testing"
)

// We have one client, and it counts from 0 to 1000
func TestSoloCount(t *testing.T) {
	SoloCount(tests.NewTestHelper(t))
}
