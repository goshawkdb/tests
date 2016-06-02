package atomicrw

import (
	"goshawkdb.io/tests"
	"testing"
)

// This is a variant of the write skew test, but this version doesn't
// rely on retry. Basically, the two txns in use are:
// t1: if x%2 == 0 then {x = x+1; y = x} else {x = x+1}
// t2: if x%2 == 0 then {y = x+2} else {x = x+1}
// Thus the only way that x goes odd is the first branch of t1. So if
// we observe an odd x, then we must have x == y
func TestAtomicRW(t *testing.T) {
	AtomicRW(tests.NewTestHelper(t))
}
