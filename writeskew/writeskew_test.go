package writeskew

import (
	"goshawkdb.io/tests"
	"testing"
)

var hosts = []string{"localhost:10001", "localhost:10002", "localhost:10003"}

// This tests for the A5B write skew anomaly.
func TestWriteSkew(t *testing.T) {
	WriteSkew(tests.NewTestHelper(t, hosts...))
}
