package main

import (
	"goshawkdb.io/tests"
	"goshawkdb.io/tests/parcount"
	"log"
	"time"
)

func main() {
	then := time.Now()
	parcount.ParCount(tests.NewTestHelper(nil))
	log.Printf("Total Parcount: %v\n", time.Now().Sub(then))
}
