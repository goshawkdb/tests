package writeskew

import (
	"goshawkdb.io/tests"
	"testing"
)

// This tests for the A5B write skew anomaly.
func TestWriteSkew(t *testing.T) {
	WriteSkew(tests.NewTestHelper(t))
}
