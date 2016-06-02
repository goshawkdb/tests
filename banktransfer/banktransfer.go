package banktransfer

import (
	"encoding/binary"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests"
	"math/rand"
	"sync"
	"time"
)

// This is essentially testing for the A6 phantom anomaly.
func BankTransfer(th *tests.TestHelper) {
	accounts := 20
	transfers := 2000
	initialWealth := uint64(1000)
	parTransfers := 8

	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	vsn, _ := conn.SetRootToNZeroObjs(accounts)
	_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		refs, err := rootObj.References()
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, initialWealth)
		for _, account := range refs {
			err = account.Set(buf)
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	th.MaybeFatal(err)

	totalWealth := initialWealth * uint64(accounts)

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parTransfers)
	endBarrier, errCh := th.InParallel(parTransfers, func(connIdx int, conn *tests.Connection) error {
		return runTransfers(accounts, conn, vsn, transfers, totalWealth, startBarrier)
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

func observeTotalWealth(conn *tests.Connection, totalWealth uint64, terminate chan struct{}) {
	for {
		res, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			sum := uint64(0)
			rootObj, err := txn.GetRootObject()
			if err != nil {
				return nil, err
			}
			refs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			for _, account := range refs {
				val, err := account.Value()
				if err != nil {
					return nil, err
				}
				sum += binary.BigEndian.Uint64(val)
			}
			return sum, nil
		})
		conn.MaybeFatal(err)
		foundWealth := res.(uint64)
		if foundWealth != totalWealth {
			conn.Fatal("FoundWealth != TotalWealth:", foundWealth, totalWealth)
		} else {
			conn.Log(foundWealth)
		}
		select {
		case <-terminate:
			return
		default:
		}
	}
}

func runTransfers(accounts int, conn *tests.Connection, rootVsn *common.TxnId, transferCount int, totalWealth uint64, startBarrier *sync.WaitGroup) error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	err := conn.AwaitRootVersionChange(rootVsn)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	bufFrom := make([]byte, 8)
	bufTo := make([]byte, 8)
	for ; transferCount > 0; transferCount-- {
		time.Sleep(10 * time.Millisecond)
		from := rng.Intn(accounts)
		to := rng.Intn(accounts - 1)
		if to >= from {
			to++
		}
		_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj, err := txn.GetRootObject()
			if err != nil {
				return nil, err
			}
			accountObjs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			fromAccount := accountObjs[from]
			toAccount := accountObjs[to]
			fromVal, err := fromAccount.Value()
			if err != nil {
				return nil, err
			}
			toVal, err := toAccount.Value()
			if err != nil {
				return nil, err
			}
			fromWealth := int64(binary.BigEndian.Uint64(fromVal))
			toWealth := int64(binary.BigEndian.Uint64(toVal))
			if fromWealth == 0 {
				return nil, nil
			}
			transfer := rng.Int63n(fromWealth)
			fromWealth -= transfer
			toWealth += transfer
			binary.BigEndian.PutUint64(bufFrom, uint64(fromWealth))
			binary.BigEndian.PutUint64(bufTo, uint64(toWealth))
			if err = fromAccount.Set(bufFrom); err != nil {
				return nil, err
			}
			return nil, toAccount.Set(bufTo)
		})
		if err != nil {
			return err
		}
	}
	return nil
}
