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
	configCopy, err := h.NewPathProvider("", false)
	if err != nil {
		log.Fatal(err)
	}

	rm1 := setup.NewRM("one", 10001, nil, configCopy)
	rm2 := setup.NewRM("two", 10002, nil, configCopy)

	prog := h.Program([]h.Instruction{
		setup,
		config1.CopyTo(setup.Dir, configCopy),
		rm1.Start(),
		setup.Sleep(10 * time.Second),
		config2.CopyTo(configCopy, configCopy),
		rm2.Start(),
		setup.Sleep(30 * time.Second),
	})
	log.Println(h.Run(setup, prog))

}
