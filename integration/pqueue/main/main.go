package main

import (
	"goshawkdb.io/tests/harness"
	"goshawkdb.io/tests/integration/pqueue"
)

func main() {
	pqueue.PQueue(harness.NewMainHelper())
}
