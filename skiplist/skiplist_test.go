package skiplist

import (
	"goshawkdb.io/tests"
	"testing"
)

func TestCreate(t *testing.T) {
	Create(tests.NewTestHelper(t))
}

func TestInsert(t *testing.T) {
	Insert(tests.NewTestHelper(t))
}

func TestInsertAndGet(t *testing.T) {
	InsertAndGet(tests.NewTestHelper(t))
}

func TestInsertAndGetManyOrdered(t *testing.T) {
	InsertAndGetManyOrdered(tests.NewTestHelper(t))
}

func TestInsertAndGetManyPermutation(t *testing.T) {
	InsertAndGetManyPermutation(tests.NewTestHelper(t))
}

func TestInsertAndGetManyPar(t *testing.T) {
	InsertAndGetManyPar(tests.NewTestHelper(t))
}

func TestInsertAndGetManyParPermutation(t *testing.T) {
	InsertAndGetManyParPermutation(tests.NewTestHelper(t))
}
