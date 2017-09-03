package strongserializable

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	"sync"
	"time"
)

// Careful, this one is quite timing sensitive - you want the number
// of proposers/acceptors to stay very close to 0 (<10).
func StrongSerializable(th *harness.TestHelper) {
	par := 3
	iterations := 1000

	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	guidBuf, err := conn.SetRootToNZeroObjs(par + par)
	th.MaybeFatal(err)
	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(par)
	endBarrier, errCh := th.InParallel(par, func(connIdx int, conn *harness.Connection) error {
		return runTest(connIdx, conn, par+par, guidBuf, iterations, startBarrier)
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runTest(connNum int, conn *harness.Connection, objCount int, guidBuf []byte, iterations int, startBarrier *sync.WaitGroup) error {
	rootRefs, err := conn.AwaitRootVersionChange(guidBuf, objCount)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	buf := make([]byte, 8)
	objRefs := []client.RefCap{rootRefs[connNum+connNum], rootRefs[connNum+connNum+1]}
	for ; iterations > 0; iterations-- {
		time.Sleep(11 * time.Millisecond)
		n := uint64(iterations)
		binary.BigEndian.PutUint64(buf, n)
		// set both objs to some value n
		_, err = conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			if err = txn.Write(objRefs[0], buf); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if err = txn.Write(objRefs[1], buf); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				return nil, nil
			}
		})
		if err != nil {
			return err
		}
		time.Sleep(7 * time.Millisecond)
		n++
		binary.BigEndian.PutUint64(buf, n)
		// set obj0 to n+1
		_, err = conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			return nil, txn.Write(objRefs[0], buf)
		})
		if err != nil {
			return err
		}
		n++
		binary.BigEndian.PutUint64(buf, n)
		// set obj0 to n+2
		_, err = conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			return nil, txn.Write(objRefs[0], buf)
		})
		if err != nil {
			return err
		}
		// now read obj2 and hope that the last two writes haven't been reordered
		res, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			if val, _, err := txn.Read(objRefs[0]); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				return binary.BigEndian.Uint64(val), nil
			}
		})
		if err != nil {
			return err
		}
		if m, ok := res.(uint64); !ok || m != n {
			return fmt.Errorf("Expected %v got %v (%v)", n, m, ok)
		}
	}
	return nil
}
