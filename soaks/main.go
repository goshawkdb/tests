package main

import (
	h "goshawkdb.io/tests/harness"
	//	"syscall"
	"time"
)

func main() {
	pathToGoshawkDBbinary := "/home/matthew/programming/goshawkdb/Go/src/goshawkdb.io/server/cmd/goshawkdb/goshawkdb"
	setup := h.NewSetup(pathToGoshawkDBbinary)

	logger := setup.NewLogger()

	rm1 := setup.NewRM("one", 10001,
		"/home/matthew/programming/goshawkdb/Go/src/goshawkdb.io/tests/testCert.pem",
		"/home/matthew/programming/goshawkdb/Go/src/goshawkdb.io/tests/testConfig2.json")
	rm2 := setup.NewRM("two", 10002,
		"/home/matthew/programming/goshawkdb/Go/src/goshawkdb.io/tests/testCert.pem",
		"/home/matthew/programming/goshawkdb/Go/src/goshawkdb.io/tests/testConfig2.json")
	rm3 := setup.NewRM("three", 10003,
		"/home/matthew/programming/goshawkdb/Go/src/goshawkdb.io/tests/testCert.pem",
		"/home/matthew/programming/goshawkdb/Go/src/goshawkdb.io/tests/testConfig2.json")

	collectionSoak := setup.NewCmd(
		"go",
		[]string{"test", "-timeout=1h", "-run", "Soak"},
		"/home/matthew/programming/goshawkdb/collections/Go/src/goshawkdb.io/collections/linearhash",
		[]string{
			"GOPATH=/home/matthew/src/Go_external_1.7.1:/home/matthew/programming/goshawkdb/collections/Go:/home/matthew/programming/gotimerwheel/Go:/home/matthew/programming/skiplist/Go:/home/matthew/programming/chancell/Go:/home/matthew/programming/gomdb/Go:/home/matthew/programming/gsim/Go:/home/matthew/programming/goshawkdb/Go",
			"GOSHAWKDB_CLUSTER_HOSTS=localhost:10001",
		},
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
	logger.Print(prog.Exec(logger))
}
