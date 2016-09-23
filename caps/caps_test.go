package caps

import (
	"goshawkdb.io/tests"
	"testing"
)

func TestNone(t *testing.T) {
	none(tests.NewTestHelper(t))
}

func TestReadOnly(t *testing.T) {
	readOnly(tests.NewTestHelper(t))
}

func TestWriteOnly(t *testing.T) {
	writeOnly(tests.NewTestHelper(t))
}

func TestReadWrite(t *testing.T) {
	readWrite(tests.NewTestHelper(t))
}
