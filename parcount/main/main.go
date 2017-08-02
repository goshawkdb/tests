package main

import (
	"fmt"
	"goshawkdb.io/tests"
	"goshawkdb.io/tests/parcount"
	"time"
)

func main() {
	then := time.Now()
	parcount.ParCount(tests.NewMainHelper())
	fmt.Printf("\nTotal Parcount: %v\n", time.Now().Sub(then))
}
