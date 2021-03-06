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

func TestFakeRead(t *testing.T) {
	fakeRead(tests.NewTestHelper(t))
}

func TestFakeWrite(t *testing.T) {
	fakeWrite(tests.NewTestHelper(t))
}

func TestCapabilitiesCanGrowSingleTxn(t *testing.T) {
	capabilitiesCanGrowSingleTxn(tests.NewTestHelper(t))
}

func TestCapabilitiesCanGrowMultiTxn(t *testing.T) {
	capabilitiesCanGrowMultiTxn(tests.NewTestHelper(t))
}
