package main

import (
	h "goshawkdb.io/tests/harness"
	"log"
	//	"syscall"
	"time"
)

func main() {
	setup := h.NewSetup()

	rm1 := setup.NewRM("one", 10001, "", "")
	rm2 := setup.NewRM("two", 10002, "", "")
	rm3 := setup.NewRM("three", 10003, "", "")

	collectionSoak := setup.NewCmd(
		"go",
		[]string{"test", "-timeout=1h", "-run", "Soak"},
		"/home/matthew/programming/goshawkdb/collections/Go/src/goshawkdb.io/collections/linearhash",
		nil,
	)

	prog := h.Program([]h.Instruction{
		setup,
		setup.InParallel(rm1.Start(), rm2.Start(), rm3.Start()),

		setup.Sleep(5 * time.Second),

		setup.InParallel(

			setup.UntilError(h.Program([]h.Instruction{
				collectionSoak.Start(),
				setup.AbsorbError(collectionSoak.Wait()),
				setup.Sleep(5 * time.Second),
			})),

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
		),
	})
	log.Println(h.Run(setup, prog))
}
