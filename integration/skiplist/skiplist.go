package skiplist

import (
	"encoding/binary"
	"errors"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	sk "goshawkdb.io/tests/integration/skiplist/skiplist"
	"math/rand"
	"sync"
	"time"
)

func Create(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	createSkipList(conn)
}

func Insert(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	sl := createSkipList(conn)
	node, err := sl.Insert(conn, []byte("a key"), []byte("a value"))
	if err != nil {
		th.Fatal(err)
	}

	k, err := node.Key(conn)
	if err != nil {
		th.Fatal(err)
	}
	if str := string(k); str != "a key" {
		th.Fatal("Expected key 'a key', got", str)
	}

	v, err := node.Value(conn)
	if err != nil {
		th.Fatal(err)
	}
	if str := string(v); str != "a value" {
		th.Fatal("Expected value 'a value', got", str)
	}
}

func InsertAndGet(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	sl := createSkipList(conn)
	nodeInsert, err := sl.Insert(conn, []byte("a key"), []byte("a value"))
	if err != nil {
		th.Fatal(err)
	}

	nodeGot, err := sl.Get(conn, []byte("a key"))
	if err != nil {
		th.Fatal(err)
	}

	if !nodeInsert.Equal(nodeGot) {
		th.Fatal("Insert node and fetched node are not equal!")
	}
	k, err := nodeGot.Key(conn)
	if err != nil {
		th.Fatal(err)
	}
	if str := string(k); str != "a key" {
		th.Fatal("Expected key 'a key', got", str)
	}

	v, err := nodeGot.Value(conn)
	if err != nil {
		th.Fatal(err)
	}
	if str := string(v); str != "a value" {
		th.Fatal("Expected value 'a value', got", str)
	}
}

func InsertAndGetManyOrdered(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	sl := createSkipList(conn)
	for idx := 0; idx < 512; idx++ {
		conn.Log("idx", idx)
		//time.Sleep(15 * time.Millisecond)
		key, value := make([]byte, 8), make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(idx))
		binary.BigEndian.PutUint64(value, uint64(idx*idx))
		_, err := sl.Insert(conn, key, value)
		if err != nil {
			th.Fatal(err)
		}
	}
}

func InsertAndGetManyPermutation(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := rng.Perm(512)

	sl := createSkipList(conn)
	for idx, num := range keys {
		conn.Log(idx, num)
		//time.Sleep(15 * time.Millisecond)
		key, value := make([]byte, 8), make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(num))
		binary.BigEndian.PutUint64(value, uint64(num*num))
		_, err := sl.Insert(conn, key, value)
		if err != nil {
			th.Fatal(err)
		}
	}
}

func InsertAndGetManyPar(th *harness.TestHelper) {
	par := 8
	limit := 512
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(par)

	guidBuf, err := conn.SetRootToNZeroObjs(0)
	th.MaybeFatal(err)
	createSkipList(conn)

	endBarrier, errCh := th.InParallel(par, func(connIdx int, conn *harness.Connection) error {
		rootRefs, err := conn.AwaitRootVersionChange(guidBuf, 1)
		startBarrier.Done()
		if err != nil {
			return err
		}
		startBarrier.Wait()

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		slCopy := sk.SkipListFromObjPtr(rng, rootRefs[0])
		key, value := make([]byte, 8), make([]byte, 8)
		for idx := connIdx; idx < limit; idx = idx + par {
			conn.Log(connIdx, idx)
			//time.Sleep(15 * time.Millisecond)
			binary.BigEndian.PutUint64(key, uint64(idx))
			binary.BigEndian.PutUint64(value, uint64(idx*idx))
			_, err := slCopy.Insert(conn, key, value)
			if err != nil {
				return err
			}
		}
		return nil
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func InsertAndGetManyParPermutation(th *harness.TestHelper) {
	par := 4
	limit := 512 / par
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(par)

	guidBuf, err := conn.SetRootToNZeroObjs(0)
	th.MaybeFatal(err)
	createSkipList(conn)

	endBarrier, errCh := th.InParallel(par, func(connIdx int, conn *harness.Connection) error {
		rootRefs, err := conn.AwaitRootVersionChange(guidBuf, 1)
		startBarrier.Done()
		if err != nil {
			return err
		}
		startBarrier.Wait()

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		slCopy := sk.SkipListFromObjPtr(rng, rootRefs[0])
		keys := rng.Perm(limit)
		key, value := make([]byte, 8), make([]byte, 8)
		for idx, base := range keys {
			num := base*par + connIdx
			conn.Log(connIdx, idx, "num", num)
			//time.Sleep(15 * time.Millisecond)
			binary.BigEndian.PutUint64(key, uint64(num))
			binary.BigEndian.PutUint64(value, uint64(num*num))
			_, err := slCopy.Insert(conn, key, value)
			if err != nil {
				return err
			}
		}
		return nil
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func createSkipList(conn *harness.Connection) *sk.SkipList {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	result, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
		if sl, err := sk.NewSkipList(txn, rng); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if rootPtr := txn.Root(conn.RootName); rootPtr == nil {
			return nil, errors.New("No such root")
		} else if rootVal, _, err := txn.Read(*rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else {
			return sl, txn.Write(*rootPtr, rootVal, sl.ObjPtr)
		}
	})
	conn.MaybeFatal(err)
	return result.(*sk.SkipList)
}
