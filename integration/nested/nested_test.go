package nested

import (
	"goshawkdb.io/tests/harness"
	"testing"
)

func TestNestedRead(t *testing.T) {
	NestedRead(harness.NewTestHelper(t))
}

func TestNestedWrite(t *testing.T) {
	NestedWrite(harness.NewTestHelper(t))
}

func TestNestedInnerAbort(t *testing.T) {
	NestedInnerAbort(harness.NewTestHelper(t))
}

func TestNestedInnerRetry(t *testing.T) {
	NestedInnerRetry(harness.NewTestHelper(t))
}

func TestNestedInnerCreate(t *testing.T) {
	NestedInnerCreate(harness.NewTestHelper(t))
}
