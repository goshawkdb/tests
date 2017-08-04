package main

import (
	"fmt"
	"goshawkdb.io/tests/harness/interpreter"
	"time"
)

func main() {
	ie := interpreter.NewInterpreterEnv()
	setup := interpreter.NewSetup()
	dalmations := 21 // yeah yeah, I know

	rms := make([]*interpreter.RM, dalmations)
	rmsStart := make([]interpreter.Instruction, dalmations)
	for idx := range rms {
		rms[idx] = setup.NewRM(fmt.Sprintf("dalmation%v", idx), uint16(10000+idx), nil, nil)
		rmsStart[idx] = rms[idx].Start()
	}

	prog := interpreter.Program([]interpreter.Instruction{
		setup,
		setup.InParallel(rmsStart...),

		setup.Sleep(20 * time.Minute),
	})
	ie.MaybeExit(ie.Run(setup, prog))
}
