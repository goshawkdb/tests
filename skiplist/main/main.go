package main

import (
	"goshawkdb.io/tests"
	"goshawkdb.io/tests/skiplist"
	"log"
	"time"
)

func main() {
	then := time.Now()
	skiplist.InsertAndGetManyOrdered(tests.NewTestHelper(nil))
	log.Printf("Total Ordered: %v\n", time.Now().Sub(then))

	then = time.Now()
	skiplist.InsertAndGetManyPermutation(tests.NewTestHelper(nil))
	log.Printf("Total Permutation: %v\n", time.Now().Sub(then))

	then = time.Now()
	skiplist.InsertAndGetManyPar(tests.NewTestHelper(nil))
	log.Printf("Total Parallel: %v\n", time.Now().Sub(then))
}
