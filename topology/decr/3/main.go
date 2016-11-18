package main

import (
	h "goshawkdb.io/tests/harness"
	"log"
	"syscall"
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
	rm3 := setup.NewRM("three", 10003, nil, configCopy)

	prog := h.Program([]h.Instruction{
		setup,

		config1.CopyTo(setup.Dir, configCopy),
		rm1.Start(),
		rm2.Start(),
		rm3.Start(),

		setup.Sleep(5 * time.Second),
		config2.CopyTo(configCopy, configCopy),
		rm2.Signal(syscall.SIGHUP),

		setup.Sleep(15 * time.Second),
		rm1.Terminate(),
		rm2.Terminate(),
		rm3.Terminate(),
		rm1.Wait(),
		rm2.Wait(),
		rm3.Wait(),
	})
	log.Println(h.Run(setup, prog))

}
