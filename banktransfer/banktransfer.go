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

	th.CreateConnections(parTransfers + 1)
	defer th.Shutdown()

	vsn := th.SetRootToNZeroObjs(accounts)
	th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		refs, err := rootObj.References()
		if err != nil {
			return nil, err
		}
		for _, account := range refs {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, initialWealth)
			err = account.Set(buf)
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})

	totalWealth := initialWealth * uint64(accounts)

	startBarrier, endBarrier := new(sync.WaitGroup), new(sync.WaitGroup)
	startBarrier.Add(parTransfers)
	endBarrier.Add(parTransfers)
	for idx := 0; idx < parTransfers; idx++ {
		connNum := idx + 1
		go runTransfers(connNum, accounts, th, vsn, transfers, totalWealth, startBarrier, endBarrier)
	}

	c := make(chan struct{})
	go func() {
		endBarrier.Wait()
		close(c)
	}()

	startBarrier.Wait()
	observeTotalWealth(th, totalWealth, c)
	// ensure we do one final observation right at the end
	observeTotalWealth(th, totalWealth, c)
}

func observeTotalWealth(th *tests.TestHelper, totalWealth uint64, terminate chan struct{}) {
	for {
		res, _ := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
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
		foundWealth := res.(uint64)
		if foundWealth != totalWealth {
			th.Fatal("FoundWealth != TotalWealth:", foundWealth, totalWealth)
		} else {
			th.Log(foundWealth)
		}
		select {
		case <-terminate:
			return
		default:
		}
	}
}

func runTransfers(connNum, accounts int, th *tests.TestHelper, rootVsn *common.TxnId, transferCount int, totalWealth uint64, startBarrier, endBarrier *sync.WaitGroup) {
	defer endBarrier.Done()
	th.AwaitRootVersionChange(connNum, rootVsn)
	startBarrier.Done()
	startBarrier.Wait()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	bufFrom := make([]byte, 8)
	bufTo := make([]byte, 8)
	for ; transferCount > 0; transferCount-- {
		time.Sleep(10 * time.Millisecond)
		from := rng.Intn(accounts)
		to := rng.Intn(accounts - 1)
		if to >= from {
			to++
		}
		th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
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
		/*
			res, _ := th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
				sum := uint64(0)
				rootObj := txn.GetRootObject()
				for _, account := range rootObj.References() {
					sum += binary.BigEndian.Uint64(account.Value())
				}
				return sum, nil
			})
			foundWealth := res.(uint64)
			if foundWealth != totalWealth {
				th.Fatal("FoundWealth != TotalWealth:", foundWealth, totalWealth)
			} else {
				th.Log(foundWealth)
			}
		*/
	}
}
