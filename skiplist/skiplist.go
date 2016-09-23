package skiplist

import (
	"encoding/binary"
	"goshawkdb.io/client"
	"goshawkdb.io/tests"
	sk "goshawkdb.io/tests/skiplist/skiplist"
	"log"
	"math/rand"
	"sync"
	"time"
)

func Create(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	createSkipList(conn)
}

func Insert(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	sl := createSkipList(conn)
	node, err := sl.Insert([]byte("a key"), []byte("a value"))
	if err != nil {
		th.Fatal(err)
	}

	k, err := node.Key()
	if err != nil {
		th.Fatal(err)
	}
	if str := string(k); str != "a key" {
		th.Fatal("Expected key 'a key', got", str)
	}

	v, err := node.Value()
	if err != nil {
		th.Fatal(err)
	}
	if str := string(v); str != "a value" {
		th.Fatal("Expected value 'a value', got", str)
	}
}

func InsertAndGet(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	sl := createSkipList(conn)
	nodeInsert, err := sl.Insert([]byte("a key"), []byte("a value"))
	if err != nil {
		th.Fatal(err)
	}

	nodeGot, err := sl.Get([]byte("a key"))
	if err != nil {
		th.Fatal(err)
	}

	if !nodeInsert.Equal(nodeGot) {
		th.Fatal("Insert node and fetched node are not equal!")
	}
	k, err := nodeGot.Key()
	if err != nil {
		th.Fatal(err)
	}
	if str := string(k); str != "a key" {
		th.Fatal("Expected key 'a key', got", str)
	}

	v, err := nodeGot.Value()
	if err != nil {
		th.Fatal(err)
	}
	if str := string(v); str != "a value" {
		th.Fatal("Expected value 'a value', got", str)
	}
}

func InsertAndGetManyOrdered(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	sl := createSkipList(conn)
	for idx := 0; idx < 512; idx++ {
		log.Println(idx)
		//time.Sleep(15 * time.Millisecond)
		key, value := make([]byte, 8), make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(idx))
		binary.BigEndian.PutUint64(value, uint64(idx*idx))
		_, err := sl.Insert(key, value)
		if err != nil {
			th.Fatal(err)
		}
	}
}

func InsertAndGetManyPermutation(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := rng.Perm(512)

	sl := createSkipList(conn)
	for idx, num := range keys {
		log.Printf("%v (%v)\n", idx, num)
		//time.Sleep(15 * time.Millisecond)
		key, value := make([]byte, 8), make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(num))
		binary.BigEndian.PutUint64(value, uint64(num*num))
		_, err := sl.Insert(key, value)
		if err != nil {
			th.Fatal(err)
		}
	}
}

func InsertAndGetManyPar(th *tests.TestHelper) {
	par := 8
	limit := 512
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(par)

	vsn, _ := conn.SetRootToZeroUInt64()
	sl := createSkipList(conn)

	endBarrier, errCh := th.InParallel(par, func(connIdx int, conn *tests.Connection) error {
		err := conn.AwaitRootVersionChange(vsn)
		startBarrier.Done()
		if err != nil {
			return err
		}
		startBarrier.Wait()

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		objRef, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj, err := conn.GetRootObject(txn)
			if err != nil {
				return nil, err
			}
			rootRefs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			slRootObj := rootRefs[0]
			if slRootObj.ReferencesSameAs(sl.ObjRef) {
				return slRootObj, nil
			} else {
				th.Log("retrying", sl.ObjRef, "!=", slRootObj)
				return client.Retry, nil
			}
		})
		if err != nil {
			return err
		}
		slCopy := sk.SkipListFromObjRef(conn.Connection, rng, objRef.(client.ObjectRef))
		key, value := make([]byte, 8), make([]byte, 8)
		for idx := connIdx; idx < limit; idx = idx + par {
			log.Println(connIdx, idx)
			//time.Sleep(15 * time.Millisecond)
			binary.BigEndian.PutUint64(key, uint64(idx))
			binary.BigEndian.PutUint64(value, uint64(idx*idx))
			_, err := slCopy.Insert(key, value)
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

func InsertAndGetManyParPermutation(th *tests.TestHelper) {
	par := 4
	limit := 512 / par
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(par)

	vsn, _ := conn.SetRootToZeroUInt64()
	sl := createSkipList(conn)

	endBarrier, errCh := th.InParallel(par, func(connIdx int, conn *tests.Connection) error {
		err := conn.AwaitRootVersionChange(vsn)
		startBarrier.Done()
		if err != nil {
			return err
		}
		startBarrier.Wait()

		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		objRef, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj, err := conn.GetRootObject(txn)
			if err != nil {
				return nil, err
			}
			rootRefs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			slRootObj := rootRefs[0]
			if slRootObj.ReferencesSameAs(sl.ObjRef) {
				return slRootObj, nil
			} else {
				th.Log("retrying", sl.ObjRef, "!=", slRootObj)
				return client.Retry, nil
			}
		})
		if err != nil {
			return err
		}
		slCopy := sk.SkipListFromObjRef(conn.Connection, rng, objRef.(client.ObjectRef))
		keys := rng.Perm(limit)
		key, value := make([]byte, 8), make([]byte, 8)
		for idx, base := range keys {
			num := base*par + connIdx
			log.Printf("%v %v (%v)\n", connIdx, idx, num)
			//time.Sleep(15 * time.Millisecond)
			binary.BigEndian.PutUint64(key, uint64(num))
			binary.BigEndian.PutUint64(value, uint64(num*num))
			_, err := slCopy.Insert(key, value)
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

func createSkipList(conn *tests.Connection) *sk.SkipList {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	result, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		sl, err := sk.NewSkipList(conn.Connection, rng)
		if err != nil {
			return nil, err
		}
		slObj, err := txn.GetObject(sl.ObjRef)
		if err != nil {
			return nil, err
		}
		rootObj, err := conn.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		return sl, rootObj.Set([]byte{}, slObj)
	})
	conn.MaybeFatal(err)
	return result.(*sk.SkipList)
}
