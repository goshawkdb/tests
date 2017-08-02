package main

import (
	"goshawkdb.io/tests"
	"goshawkdb.io/tests/solocount"
)

func main() {
	solocount.SoloCount(tests.NewMainHelper())
}
