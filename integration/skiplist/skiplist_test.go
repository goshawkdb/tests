package skiplist

import (
	"goshawkdb.io/tests/harness"
	"testing"
)

func TestCreate(t *testing.T) {
	Create(harness.NewTestHelper(t))
}

func TestInsert(t *testing.T) {
	Insert(harness.NewTestHelper(t))
}

func TestInsertAndGet(t *testing.T) {
	InsertAndGet(harness.NewTestHelper(t))
}

func TestInsertAndGetManyOrdered(t *testing.T) {
	InsertAndGetManyOrdered(harness.NewTestHelper(t))
}

func TestInsertAndGetManyPermutation(t *testing.T) {
	InsertAndGetManyPermutation(harness.NewTestHelper(t))
}

func TestInsertAndGetManyPar(t *testing.T) {
	InsertAndGetManyPar(harness.NewTestHelper(t))
}

func TestInsertAndGetManyParPermutation(t *testing.T) {
	InsertAndGetManyParPermutation(harness.NewTestHelper(t))
}
