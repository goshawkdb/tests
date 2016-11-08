package main

import (
	"fmt"
	h "goshawkdb.io/tests/harness"
	"log"
	"time"
)

func main() {
	setup := h.NewSetup()

	dalmations := 21 // yeah yeah, I know

	rms := make([]*h.RM, dalmations)
	rmsStart := make([]h.Instruction, dalmations)
	for idx := range rms {
		rms[idx] = setup.NewRM(fmt.Sprintf("dalmation%v", idx), uint16(10000+idx), nil, nil)
		rmsStart[idx] = rms[idx].Start()
	}

	prog := h.Program([]h.Instruction{
		setup,
		setup.InParallel(rmsStart...),

		setup.Sleep(20 * time.Minute),
	})
	log.Println(h.Run(setup, prog))
}
