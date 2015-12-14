package skiplist

import (
	"goshawkdb.io/tests"
	"testing"
)

var hosts = []string{"localhost:10001", "localhost:10002", "localhost:10003"}

func TestCreate(t *testing.T) {
	Create(tests.NewTestHelper(t, hosts...))
}

func TestInsert(t *testing.T) {
	Insert(tests.NewTestHelper(t, hosts...))
}

func TestInsertAndGet(t *testing.T) {
	InsertAndGet(tests.NewTestHelper(t, hosts...))
}

func TestInsertAndGetManyOrdered(t *testing.T) {
	InsertAndGetManyOrdered(tests.NewTestHelper(t, hosts...))
}

func TestInsertAndGetManyPermutation(t *testing.T) {
	InsertAndGetManyPermutation(tests.NewTestHelper(t, hosts...))
}

func TestInsertAndGetManyPar(t *testing.T) {
	InsertAndGetManyPar(tests.NewTestHelper(t, hosts...))
}

func TestInsertAndGetManyParPermutation(t *testing.T) {
	InsertAndGetManyParPermutation(tests.NewTestHelper(t, hosts...))
}
