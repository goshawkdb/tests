package main

import (
	h "goshawkdb.io/tests/harness"
	"log"
	//	"syscall"
	"time"
)

func main() {
	setup := h.NewSetup()

	rm1 := setup.NewRM("one", 10001, nil, nil)
	rm2 := setup.NewRM("two", 10002, nil, nil)
	rm3 := setup.NewRM("three", 10003, nil, nil)

	goPP, err := h.NewPathProvider("go", true)
	if err != nil {
		log.Fatal(err)
	}
	cwdPP, err := h.NewPathProvider("/home/matthew/src/goshawkdb/collections/Go/src/goshawkdb.io/collections/linearhash", false)
	if err != nil {
		log.Fatal(err)
	}

	collectionSoak := setup.NewCmd(
		goPP,
		[]string{"test", "-timeout=1h", "-run", "Soak"},
		cwdPP,
		nil,
	)

	stoppable := setup.UntilStopped(h.Program([]h.Instruction{
		collectionSoak.Start(),
		setup.AbsorbError(collectionSoak.Wait()),
		setup.Sleep(5 * time.Second),
	}))

	prog := h.Program([]h.Instruction{
		setup,
		setup.InParallel(rm1.Start(), rm2.Start(), rm3.Start()),

		setup.Sleep(5 * time.Second),

		setup.InParallel(

			stoppable,

			h.Program([]h.Instruction{
				setup.UntilError(h.Program([]h.Instruction{
					setup.SleepRandom(5*time.Second, 10*time.Second),
					rm2.Terminate(),
					setup.SleepRandom(1*time.Second, 5*time.Second),
					rm3.Terminate(),
					rm2.Wait(),
					rm3.Wait(),
					setup.SleepRandom(1*time.Second, 5*time.Second),
					rm3.Start(),
					setup.SleepRandom(1*time.Second, 5*time.Second),
					rm2.Start(),
				})),
				setup.Log("Servers have errored!"),
			}),

			h.Program([]h.Instruction{
				setup.Sleep(20 * time.Minute),
				stoppable.Stop(),
			}),
		),
	})
	log.Println(h.Run(setup, prog))
}
