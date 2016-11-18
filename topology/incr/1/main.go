package main

import (
	h "goshawkdb.io/tests/harness"
	"log"
	"time"
)

func main() {
	setup := h.NewSetup()

	config1, err := h.NewPathProvider("./v1.json", false)
	if err != nil {
		log.Fatal(err)
	}
	config2, err := h.NewPathProvider("./v2.json", false)
	if err != nil {
		log.Fatal(err)
	}

	rm1 := setup.NewRM("one", 10001, nil, config1)
	rm2 := setup.NewRM("two", 10002, nil, config2)

	prog := h.Program([]h.Instruction{
		setup,
		rm1.Start(),
		setup.Sleep(5 * time.Second),
		rm2.Start(),
		setup.Sleep(15 * time.Second),
		rm1.Terminate(),
		rm2.Terminate(),
		rm1.Wait(),
		rm2.Wait(),
	})
	log.Println(h.Run(setup, prog))

}
