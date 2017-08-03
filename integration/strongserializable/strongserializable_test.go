package strongserializable

import (
	"goshawkdb.io/tests/harness"
	"testing"
)

// Careful, this one is quite timing sensitive - you want the number
// of proposers/acceptors to stay very close to 0 (<10).
func TestStrongSerializable(t *testing.T) {
	StrongSerializable(harness.NewTestHelper(t))
}
