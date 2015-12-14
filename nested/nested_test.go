package nested

import (
	"goshawkdb.io/tests"
	"testing"
)

var hosts = []string{"localhost:10001", "localhost:10002", "localhost:10003"}

func TestNestedRead(t *testing.T) {
	NestedRead(tests.NewTestHelper(t, hosts...))
}

func TestNestedWrite(t *testing.T) {
	NestedWrite(tests.NewTestHelper(t, hosts...))
}

func TestNestedInnerAbort(t *testing.T) {
	NestedInnerAbort(tests.NewTestHelper(t, hosts...))
}

func TestNestedInnerRetry(t *testing.T) {
	NestedInnerRetry(tests.NewTestHelper(t, hosts...))
}

func TestNestedInnerCreate(t *testing.T) {
	NestedInnerCreate(tests.NewTestHelper(t, hosts...))
}
