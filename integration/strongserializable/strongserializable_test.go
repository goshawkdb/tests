package strongserializable

import (
	"goshawkdb.io/tests"
	"testing"
)

// Careful, this one is quite timing sensitive - you want the number
// of proposers/acceptors to stay very close to 0 (<10).
func TestStrongSerializable(t *testing.T) {
	StrongSerializable(tests.NewTestHelper(t))
}
