package banktransfer

import (
	"encoding/binary"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	"math/rand"
	"sync"
	"time"
)

// This is essentially testing for the A6 phantom anomaly.
func BankTransfer(th *harness.TestHelper) {
	accounts := 20
	transfers := 2000
	initialWealth := uint64(1000)
	parTransfers := 8

	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	guidBuf, err := conn.SetRootToNZeroObjs(accounts)
	th.MaybeFatal(err)
	_, err = conn.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr, _ := txn.Root(conn.RootName)
		if _, rootRefs, err := txn.Read(rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, initialWealth)
			for _, account := range rootRefs {
				if err = txn.Write(account, buf); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}
	})
	th.MaybeFatal(err)

	totalWealth := initialWealth * uint64(accounts)

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parTransfers)
	endBarrier, errCh := th.InParallel(parTransfers, func(connIdx int, conn *harness.Connection) error {
		return runTransfers(accounts, conn, guidBuf, transfers, totalWealth, startBarrier)
	})

	c := make(chan struct{})
	go func() {
		endBarrier.Wait()
		close(c)
		close(errCh)
	}()

	startBarrier.Wait()
	observeTotalWealth(conn, totalWealth, c)
	// ensure we do one final observation right at the end
	observeTotalWealth(conn, totalWealth, c)
	th.MaybeFatal(<-errCh)
}

func observeTotalWealth(conn *harness.Connection, totalWealth uint64, terminate chan struct{}) {
	for {
		time.Sleep(15 * time.Millisecond)
		res, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			sum := uint64(0)
			rootPtr, _ := txn.Root(conn.RootName)
			if _, rootRefs, err := txn.Read(rootPtr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				for _, account := range rootRefs {
					if val, _, err := txn.Read(account); err != nil || txn.RestartNeeded() {
						return nil, err
					} else {
						sum += binary.BigEndian.Uint64(val)
					}
				}
				return sum, nil
			}
		})
		conn.MaybeFatal(err)
		foundWealth := res.(uint64)
		if foundWealth != totalWealth {
			conn.Fatal("FoundWealth != TotalWealth:", foundWealth, totalWealth)
		} else {
			conn.Log("wealth", foundWealth)
		}
		select {
		case <-terminate:
			return
		default:
		}
	}
}

func runTransfers(accounts int, conn *harness.Connection, guidBuf []byte, transferCount int, totalWealth uint64, startBarrier *sync.WaitGroup) error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rootRefs, err := conn.AwaitRootVersionChange(guidBuf, accounts)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	conn.Log("transferer", "starting")
	defer conn.Log("transferer", "finished")
	for ; transferCount > 0; transferCount-- {
		time.Sleep(50 * time.Millisecond)
		from := rng.Intn(accounts)
		to := rng.Intn(accounts - 1)
		if to >= from {
			to++
		}
		_, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			fromAccount := rootRefs[from]
			toAccount := rootRefs[to]
			if fromVal, _, err := txn.Read(fromAccount); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if toVal, _, err := txn.Read(toAccount); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				fromWealth := int64(binary.BigEndian.Uint64(fromVal))
				toWealth := int64(binary.BigEndian.Uint64(toVal))
				if fromWealth == 0 {
					return nil, nil
				}
				transfer := rng.Int63n(fromWealth + 1)
				fromWealth -= transfer
				toWealth += transfer
				binary.BigEndian.PutUint64(fromVal, uint64(fromWealth))
				binary.BigEndian.PutUint64(toVal, uint64(toWealth))
				if err = txn.Write(fromAccount, fromVal); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					return nil, txn.Write(toAccount, toVal)
				}
			}
		})
		if err != nil {
			return err
		}
	}
	return nil
}
