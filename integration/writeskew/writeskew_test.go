package writeskew

import (
	"goshawkdb.io/tests/harness"
	"testing"
)

// This tests for the A5B write skew anomaly.
func TestWriteSkew(t *testing.T) {
	WriteSkew(harness.NewTestHelper(t))
}
