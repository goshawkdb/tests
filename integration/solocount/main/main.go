package main

import (
	"goshawkdb.io/tests/harness"
	"goshawkdb.io/tests/integration/solocount"
)

func main() {
	solocount.SoloCount(harness.NewMainHelper())
}
