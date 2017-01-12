package main

import (
	h "goshawkdb.io/tests/harness"
	ht "goshawkdb.io/tests/harness/topology"
	"log"
)

func main() {
	before := &ht.PortsAndF{
		Ports: []uint16{10001, 10002, 10003},
		F:     1,
	}
	after := &ht.PortsAndF{
		Ports: []uint16{10001, 10003, 10004},
		F:     1,
	}
	setup := h.NewSetup()
	prog := h.Program(ht.TopologyChange(before, after, setup))
	log.Println(h.Run(setup, prog))
}
