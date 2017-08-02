package main

import (
	"fmt"
	"goshawkdb.io/tests"
	"goshawkdb.io/tests/skiplist"
	"time"
)

func main() {
	then := time.Now()
	skiplist.InsertAndGetManyOrdered(tests.NewMainHelper())
	fmt.Printf("\nTotal Ordered: %v\n", time.Now().Sub(then))

	then = time.Now()
	skiplist.InsertAndGetManyPermutation(tests.NewMainHelper())
	fmt.Printf("\nTotal Permutation: %v\n", time.Now().Sub(then))

	then = time.Now()
	skiplist.InsertAndGetManyPar(tests.NewMainHelper())
	fmt.Printf("\nTotal Parallel: %v\n", time.Now().Sub(then))
}
