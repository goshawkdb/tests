package nested

import (
	"goshawkdb.io/tests"
	"testing"
)

func TestNestedRead(t *testing.T) {
	NestedRead(tests.NewTestHelper(t))
}

func TestNestedWrite(t *testing.T) {
	NestedWrite(tests.NewTestHelper(t))
}

func TestNestedInnerAbort(t *testing.T) {
	NestedInnerAbort(tests.NewTestHelper(t))
}

func TestNestedInnerRetry(t *testing.T) {
	NestedInnerRetry(tests.NewTestHelper(t))
}

func TestNestedInnerCreate(t *testing.T) {
	NestedInnerCreate(tests.NewTestHelper(t))
}
