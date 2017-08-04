package main

import (
	"fmt"
	"goshawkdb.io/tests/harness"
	"goshawkdb.io/tests/integration/skiplist"
	"time"
)

func main() {
	then := time.Now()
	skiplist.InsertAndGetManyOrdered(harness.NewMainHelper())
	fmt.Printf("\nTotal Ordered: %v\n", time.Now().Sub(then))

	then = time.Now()
	skiplist.InsertAndGetManyPermutation(harness.NewMainHelper())
	fmt.Printf("\nTotal Permutation: %v\n", time.Now().Sub(then))

	then = time.Now()
	skiplist.InsertAndGetManyPar(harness.NewMainHelper())
	fmt.Printf("\nTotal Parallel: %v\n", time.Now().Sub(then))
}
