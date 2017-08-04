package caps

import (
	"goshawkdb.io/tests/harness"
	"testing"
)

func TestNone(t *testing.T) {
	none(harness.NewTestHelper(t))
}

func TestReadOnly(t *testing.T) {
	readOnly(harness.NewTestHelper(t))
}

func TestWriteOnly(t *testing.T) {
	writeOnly(harness.NewTestHelper(t))
}

func TestReadWrite(t *testing.T) {
	readWrite(harness.NewTestHelper(t))
}

func TestFakeRead(t *testing.T) {
	fakeRead(harness.NewTestHelper(t))
}

func TestFakeWrite(t *testing.T) {
	fakeWrite(harness.NewTestHelper(t))
}

func TestCapabilitiesCanGrowSingleTxn(t *testing.T) {
	capabilitiesCanGrowSingleTxn(harness.NewTestHelper(t))
}

func TestCapabilitiesCanGrowMultiTxn(t *testing.T) {
	capabilitiesCanGrowMultiTxn(harness.NewTestHelper(t))
}
