package main

import (
	"fmt"
	"goshawkdb.io/tests/harness"
	"goshawkdb.io/tests/integration/parcount"
	"time"
)

func main() {
	then := time.Now()
	parcount.ParCount(harness.NewMainHelper())
	fmt.Printf("\nTotal Parcount: %v\n", time.Now().Sub(then))
}
