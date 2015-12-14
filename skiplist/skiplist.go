package skiplist

import (
	"encoding/binary"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests"
	sk "goshawkdb.io/tests/skiplist/skiplist"
	"log"
	"math/rand"
	"sync"
	"time"
)

func Create(th *tests.TestHelper) {
	th.CreateConnections(1)
	defer th.Shutdown()

	createSkipList(th)
}

func Insert(th *tests.TestHelper) {
	th.CreateConnections(1)
	defer th.Shutdown()

	sl := createSkipList(th)
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
	th.CreateConnections(1)
	defer th.Shutdown()

	sl := createSkipList(th)
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
	th.CreateConnections(1)
	defer th.Shutdown()

	sl := createSkipList(th)
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
	th.CreateConnections(1)
	defer th.Shutdown()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := rng.Perm(512)

	sl := createSkipList(th)
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
	th.CreateConnections(1 + par)
	defer th.Shutdown()

	startBarrier, endBarrier := new(sync.WaitGroup), new(sync.WaitGroup)
	startBarrier.Add(par)
	endBarrier.Add(par)

	vsn := th.SetRootToZeroUInt64()
	sl := createSkipList(th)

	for c := 0; c < par; c++ {
		conn := c + 1
		go func() {
			defer endBarrier.Done()
			th.AwaitRootVersionChange(conn, vsn)
			startBarrier.Done()
			startBarrier.Wait()

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			objId, _ := th.RunTransaction(conn, func(txn *client.Txn) (interface{}, error) {
				rootObj, err := txn.GetRootObject()
				if err != nil {
					return nil, err
				}
				rootRefs, err := rootObj.References()
				if err != nil {
					return nil, err
				}
				slRootObjId := rootRefs[0].Id
				if slRootObjId.Equal(sl.ObjId) {
					return slRootObjId, nil
				} else {
					th.Log("retrying", sl.ObjId, "!=", slRootObjId)
					return client.Retry, nil
				}
			})
			slCopy := sk.SkipListFromObjId(th.Connections[conn].Connection, rng, objId.(*common.VarUUId))
			for idx := conn; idx < limit; idx = idx + par {
				log.Println(conn, idx)
				//time.Sleep(15 * time.Millisecond)
				key, value := make([]byte, 8), make([]byte, 8)
				binary.BigEndian.PutUint64(key, uint64(idx))
				binary.BigEndian.PutUint64(value, uint64(idx*idx))
				_, err := slCopy.Insert(key, value)
				if err != nil {
					th.Fatal(err)
				}
			}
		}()
	}
	endBarrier.Wait()
}

func InsertAndGetManyParPermutation(th *tests.TestHelper) {
	par := 4
	limit := 512 / par
	th.CreateConnections(1 + par)
	defer th.Shutdown()

	startBarrier, endBarrier := new(sync.WaitGroup), new(sync.WaitGroup)
	startBarrier.Add(par)
	endBarrier.Add(par)

	vsn := th.SetRootToZeroUInt64()
	sl := createSkipList(th)

	for c := 0; c < par; c++ {
		conn := c + 1
		go func() {
			defer endBarrier.Done()
			th.AwaitRootVersionChange(conn, vsn)
			startBarrier.Done()
			startBarrier.Wait()

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			objId, _ := th.RunTransaction(conn, func(txn *client.Txn) (interface{}, error) {
				rootObj, err := txn.GetRootObject()
				if err != nil {
					return nil, err
				}
				rootRefs, err := rootObj.References()
				if err != nil {
					return nil, err
				}
				slRootObjId := rootRefs[0].Id
				if slRootObjId.Equal(sl.ObjId) {
					return slRootObjId, nil
				} else {
					th.Log("retrying", sl.ObjId, "!=", slRootObjId)
					return client.Retry, nil
				}
			})
			slCopy := sk.SkipListFromObjId(th.Connections[conn].Connection, rng, objId.(*common.VarUUId))
			keys := rng.Perm(limit)
			for idx, base := range keys {
				num := base*par + conn
				log.Printf("%v %v (%v)\n", conn, idx, num)
				//time.Sleep(15 * time.Millisecond)
				key, value := make([]byte, 8), make([]byte, 8)
				binary.BigEndian.PutUint64(key, uint64(num))
				binary.BigEndian.PutUint64(value, uint64(num*num))
				_, err := slCopy.Insert(key, value)
				if err != nil {
					th.Fatal(err)
				}
			}
		}()
	}
	endBarrier.Wait()
}

func createSkipList(th *tests.TestHelper) *sk.SkipList {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	conn := th.Connections[0].Connection
	result, _ := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		sl, err := sk.NewSkipList(conn, rng)
		if err != nil {
			return nil, err
		}
		slObj, err := txn.GetObject(sl.ObjId)
		if err != nil {
			return nil, err
		}
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		return sl, rootObj.Set([]byte{}, slObj)
	})
	return result.(*sk.SkipList)
}
