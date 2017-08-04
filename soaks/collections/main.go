package main

import (
	"goshawkdb.io/tests/harness/interpreter"
	"syscall"
	"time"
)

func main() {
	ie := interpreter.NewInterpreterEnv()
	setup := interpreter.NewSetup()

	rm1 := setup.NewRM("one", 10001, nil, nil)
	rm2 := setup.NewRM("two", 10002, nil, nil)
	rm3 := setup.NewRM("three", 10003, nil, nil)

	goPP, err := interpreter.NewPathProvider("go", true)
	ie.MaybeExit(err)

	cwdPP, err := interpreter.NewPathProvider("/home/matthew/programming/goshawkdb/Go/src/goshawkdb.io/collections/linearhash", false)
	ie.MaybeExit(err)

	collectionSoak := setup.NewCmd(
		goPP,
		[]string{"test", "-timeout=1h", "-run", "Soak"},
		cwdPP,
		nil,
	)

	stoppableTest := setup.UntilStopped(interpreter.Program([]interpreter.Instruction{
		collectionSoak.Start(),
		setup.AbsorbError(collectionSoak.Wait()),
		setup.Sleep(5 * time.Second),
	}))

	stoppableServers := setup.UntilStopped(interpreter.Program([]interpreter.Instruction{
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
	}))

	prog := interpreter.Program([]interpreter.Instruction{
		setup,
		setup.InParallel(rm1.Start(), rm2.Start(), rm3.Start()),

		setup.Sleep(5 * time.Second),

		setup.InParallel(

			stoppableTest,
			stoppableServers,

			interpreter.Program([]interpreter.Instruction{
				setup.Sleep(2 * time.Minute),
				stoppableTest.Stop(),
				stoppableServers.Stop(), // will leave all 3 running
				setup.Sleep(240 * time.Second),
				rm1.Signal(syscall.SIGUSR1),
				setup.Sleep(time.Second),
				rm2.Signal(syscall.SIGUSR1),
				setup.Sleep(time.Second),
				rm3.Signal(syscall.SIGUSR1),
				setup.Sleep(60 * time.Second),
			}),
		),
	})
	ie.MaybeExit(ie.Run(setup, prog))
}
