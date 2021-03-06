package skiplist

import (
	"bytes"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/client"
	msgs "goshawkdb.io/tests/skiplist/skiplist/capnp"
	// "log"
	"fmt"
	"math"
	"math/rand"
)

const (
	p            = 0.25
	defaultDepth = 2
)

type SkipList struct {
	Connection *client.Connection
	ObjRef     client.ObjectRef
	rng        *rand.Rand
}

type Node struct {
	SkipList *SkipList
	ObjRef   client.ObjectRef
}

func NewSkipList(conn *client.Connection, rng *rand.Rand) (*SkipList, error) {
	depth := defaultDepth

	terminusSeg := capn.NewBuffer(nil)
	terminusCap := msgs.NewRootSkipListNodeCap(terminusSeg)
	terminusCap.SetHeightRand(0)
	terminusBuf := new(bytes.Buffer)
	terminusCap.SetNextKeys(terminusSeg.NewDataList(depth))

	if _, err := terminusSeg.WriteTo(terminusBuf); err != nil {
		return nil, err
	}
	terminusBytes := terminusBuf.Bytes()

	skipListSeg := capn.NewBuffer(nil)
	skipListCap := msgs.NewRootSkipListCap(skipListSeg)
	skipListCap.SetLength(0)
	probsCap := skipListSeg.NewFloat32List(1)
	skipListCap.SetLevelProbabilities(probsCap)
	probsCap.Set(0, p)
	skipListCap.SetCurDepth(uint64(depth))
	skipListCap.SetCurCapacity(calculateCapacity(uint64(depth)))

	skipListBuf := new(bytes.Buffer)
	if _, err := skipListSeg.WriteTo(skipListBuf); err != nil {
		return nil, err
	}
	skipListBytes := skipListBuf.Bytes()

	result, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		terminusObj, err := txn.CreateObject(terminusBytes)
		if err != nil {
			return nil, err
		}
		skipListObj, err := txn.CreateObject(skipListBytes, terminusObj)
		if err != nil {
			return nil, err
		}
		//                                            sl           val          prev
		terminusRefs := []client.ObjectRef{skipListObj, terminusObj, terminusObj}
		for idx := 0; idx < depth; idx++ {
			terminusRefs = append(terminusRefs, terminusObj)
		}
		if err = terminusObj.Set(terminusBytes, terminusRefs...); err != nil {
			return nil, err
		}
		return skipListObj.ObjectRef, nil
	})
	if err != nil {
		return nil, err
	}
	return &SkipList{
		Connection: conn,
		ObjRef:     result.(client.ObjectRef),
		rng:        rng,
	}, nil
}

func SkipListFromObjRef(conn *client.Connection, rng *rand.Rand, objRef client.ObjectRef) *SkipList {
	return &SkipList{
		Connection: conn,
		ObjRef:     objRef,
		rng:        rng,
	}
}

func calculateCapacity(curDepth uint64) uint64 {
	base := float64(1.0) / p
	capacity := math.Pow(base, float64(curDepth))
	return uint64(math.Floor(capacity))
}

func (s *SkipList) within(fun func(client.ObjectRef, []client.ObjectRef, *msgs.SkipListCap, *client.Txn) (interface{}, error)) (interface{}, *client.Stats, error) {
	return s.Connection.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		// log.Printf("within starting %v\n", fun)
		sObj, err := txn.GetObject(s.ObjRef)
		if err != nil {
			return nil, err
		}
		sObjVal, sObjRefs, err := sObj.ValueReferences()
		if err != nil {
			return nil, err
		}
		sSeg, _, err := capn.ReadFromMemoryZeroCopy(sObjVal)
		if err != nil {
			return nil, err
		}
		sCap := msgs.ReadRootSkipListCap(sSeg)
		return fun(sObj, sObjRefs, &sCap, txn)
	})
}

func (s *SkipList) withinNode(nodeId client.ObjectRef, fun func(*msgs.SkipListNodeCap, client.ObjectRef, []client.ObjectRef, *client.Txn) (interface{}, error)) (interface{}, *client.Stats, error) {
	return s.Connection.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		// log.Printf("withinNode starting %v\n", fun)
		nObj, err := txn.GetObject(nodeId)
		if err != nil {
			return nil, err
		}
		nObjVal, nObjRefs, err := nObj.ValueReferences()
		if err != nil {
			return nil, err
		}
		nSeg, _, err := capn.ReadFromMemoryZeroCopy(nObjVal)
		if err != nil {
			return nil, err
		}
		nCap := msgs.ReadRootSkipListNodeCap(nSeg)
		return fun(&nCap, nObj, nObjRefs, txn)
	})
}

func (s *SkipList) chooseNumLevels() (float32, int, error) {
	r := s.rng.Float32()
	result, _, err := s.within(func(sObj client.ObjectRef, sObjRefs []client.ObjectRef, sCap *msgs.SkipListCap, txn *client.Txn) (interface{}, error) {
		// log.Printf("chooseNumLevels starting\n")
		// defer log.Printf("chooseNumLevels ended\n")
		probs := sCap.LevelProbabilities()
		max := probs.Len()
		for idx := 0; idx < max; idx++ {
			if r > probs.At(idx) {
				return idx + 1, nil
			}
		}
		return max + 1, nil
	})
	if err != nil {
		return 0, 0, err
	}
	return r, result.(int), nil
}

func (s *SkipList) ensureCapacity() error {
	_, _, err := s.within(func(sObj client.ObjectRef, sObjRefs []client.ObjectRef, sCap *msgs.SkipListCap, txn *client.Txn) (interface{}, error) {
		// log.Printf("ensureCapacity starting\n")
		// defer log.Printf("ensureCapacity ended\n")
		if sCap.Length() < sCap.CurCapacity() {
			return nil, nil
		}

		skipListSeg := capn.NewBuffer(nil)
		skipListCap := msgs.NewRootSkipListCap(skipListSeg)
		skipListCap.SetLength(sCap.Length())

		probs := sCap.LevelProbabilities()
		curDepth := sCap.CurDepth()
		threshold := p * probs.At(int(curDepth-2))
		curDepth++
		probsLen := probs.Len()
		probsNew := skipListSeg.NewFloat32List(probsLen + 1)
		skipListCap.SetLevelProbabilities(probsNew)
		for idx := 0; idx < probsLen; idx++ {
			probsNew.Set(idx, probs.At(idx))
		}
		probsNew.Set(probsLen, threshold)
		skipListCap.SetCurDepth(curDepth)
		skipListCap.SetCurCapacity(calculateCapacity(curDepth))

		skipListBuf := new(bytes.Buffer)
		if _, err := skipListSeg.WriteTo(skipListBuf); err != nil {
			return nil, err
		}
		skipListBytes := skipListBuf.Bytes()

		tObj := sObjRefs[0]
		if err := sObj.Set(skipListBytes, tObj); err != nil {
			return nil, err
		}

		cur := tObj
		tObjRefs, err := tObj.References()
		if err != nil {
			return nil, err
		}
		lvl := len(tObjRefs) - 1
		prev := cur
		for {
			curRefs, err := cur.References()
			if err != nil {
				return nil, err
			}
			next := curRefs[lvl]
			newPrev, _, err := s.withinNode(cur, func(curCap *msgs.SkipListNodeCap, curObj client.ObjectRef, curRefs []client.ObjectRef, txn *client.Txn) (interface{}, error) {
				// log.Printf("ensureCapacity inner starting\n")
				// defer log.Printf("ensureCapacity inner ended\n")
				if curCap.HeightRand() <= threshold {
					newSeg := capn.NewBuffer(nil)
					newCap := msgs.NewRootSkipListNodeCap(newSeg)
					newCap.SetHeightRand(curCap.HeightRand())
					newCap.SetKey(curCap.Key())
					oldKeys := curCap.NextKeys()
					newKeys := newSeg.NewDataList(oldKeys.Len() + 1)
					newCap.SetNextKeys(newKeys)
					for idx, l := 0, oldKeys.Len(); idx < l; idx++ {
						newKeys.Set(idx, oldKeys.At(idx))
					}

					newBuf := new(bytes.Buffer)
					if _, err := newSeg.WriteTo(newBuf); err != nil {
						return nil, err
					}
					newBytes := newBuf.Bytes()

					if err := curObj.Set(newBytes, append(curRefs, tObj)...); err != nil {
						return nil, err
					}

					_, err = s.setNextKey(prev.ObjectRef, lvl-2, curCap.Key(), curObj)
					if err != nil {
						return nil, err
					}
					return cur, nil
				}
				return prev, nil
			})
			if err != nil {
				return nil, err
			}
			prev = newPrev.(client.ObjectRef)
			if next.ReferencesSameAs(tObj) {
				break
			} else {
				cur = next
			}
		}

		return nil, nil
	})
	return err
}

func (s *SkipList) getEqOrLessThan(k []byte) (client.ObjectRef, []client.ObjectRef, error) {
	var node client.ObjectRef
	var descent []client.ObjectRef
	_, _, err := s.within(func(sObj client.ObjectRef, sObjRefs []client.ObjectRef, sCap *msgs.SkipListCap, txn *client.Txn) (interface{}, error) {
		// log.Printf("getEqOrLessThan starting\n")
		// defer log.Printf("getEqOrLessThan ended\n")
		descent = nil
		tObj := sObjRefs[0]
		cur := tObj
		curRefs, err := cur.References()
		if err != nil {
			return nil, err
		}
		lvl := len(curRefs) - 1
		descent = make([]client.ObjectRef, lvl-2)
		descent[lvl-3] = cur
		for ; lvl >= 3; lvl-- {
			for {
				curRefs, err := cur.References()
				if err != nil {
					return nil, err
				}
				next := curRefs[lvl]
				if next.ReferencesSameAs(tObj) {
					break
				}
				nextKey, _, err := s.withinNode(cur, func(curCap *msgs.SkipListNodeCap, curObj client.ObjectRef, curRefs []client.ObjectRef, txn *client.Txn) (interface{}, error) {
					// log.Printf("getEqOrLessThan inner starting\n")
					// defer log.Printf("getEqOrLessThan inner ended\n")
					return curCap.NextKeys().At(lvl - 3), nil
				})
				if err != nil {
					return nil, err
				}
				if len(nextKey.([]byte)) == 0 {
					panic(fmt.Sprintf("Encountered empty key for node %v (which is not the terminus)", next.ObjectRef))
				}
				if cmp := bytes.Compare(nextKey.([]byte), k); cmp < 0 {
					cur = next
				} else if cmp == 0 {
					node = next
					return nil, nil
				} else {
					break
				}
			}
			descent[lvl-3] = cur
		}
		node = cur
		return nil, nil
	})
	// log.Println("getEqOrLessThan done")
	if err != nil {
		return client.ObjectRef{}, nil, err
	}
	return node, descent, nil
}

func (s *SkipList) Insert(k, v []byte) (*Node, error) {
	result, _, err := s.within(func(sObj client.ObjectRef, sObjRefs []client.ObjectRef, sCap *msgs.SkipListCap, txn *client.Txn) (interface{}, error) {
		// log.Printf("insert starting\n")
		// defer log.Printf("insert ended\n")
		tObj := sObjRefs[0]

		if err := s.ensureCapacity(); err != nil {
			return nil, err
		}
		curObj, descent, err := s.getEqOrLessThan(k)
		if err != nil {
			return nil, err
		}
		vObj, err := txn.CreateObject(v)
		if err != nil {
			return nil, err
		}
		if tObj.ReferencesSameAs(curObj) {
			eq, _, err := s.withinNode(curObj, func(nCap *msgs.SkipListNodeCap, nObj client.ObjectRef, nRefs []client.ObjectRef, txn *client.Txn) (interface{}, error) {
				// log.Printf("insert inner starting\n")
				// defer log.Printf("insert inner ended\n")
				return bytes.Equal(nCap.Key(), k), nil
			})
			if err != nil {
				return nil, err
			}
			if eq.(bool) {
				curRefs, err := curObj.References()
				if err != nil {
					return nil, err
				}
				curRefs[1] = vObj
				curVal, err := curObj.Value()
				if err != nil {
					return nil, err
				}
				if err = curObj.Set(curVal, curRefs...); err != nil {
					return nil, err
				}
				return curObj, nil
			}
		}
		heightRand, height, err := s.chooseNumLevels()
		// fmt.Printf("hr:%v;h:%v ", heightRand, height)
		if err != nil {
			return nil, err
		}
		descent = descent[:height]

		nodeSeg := capn.NewBuffer(nil)
		nodeCap := msgs.NewRootSkipListNodeCap(nodeSeg)
		nodeCap.SetHeightRand(heightRand)
		nodeCap.SetKey(k)
		nodeNextKeys := nodeSeg.NewDataList(height)
		nodeCap.SetNextKeys(nodeNextKeys)

		nodeRefs := []client.ObjectRef{sObj, vObj, curObj}
		for idx, pObj := range descent {
			pObjRefs, err := pObj.References()
			if err != nil {
				return nil, err
			}
			nodeRefs = append(nodeRefs, pObjRefs[idx+3])
		}
		nObj, err := txn.CreateObject([]byte{}, nodeRefs...)
		if err != nil {
			return nil, err
		}

		nextObj := descent[0]
		nextRefs, err := nextObj.References()
		if err != nil {
			return nil, err
		}
		nextRefs[2] = nObj
		nextVal, err := nextObj.Value()
		if err != nil {
			return nil, err
		}
		if err = nextObj.Set(nextVal, nextRefs...); err != nil {
			return nil, err
		}

		for idx, pObj := range descent {
			nextKey, err := s.setNextKey(pObj, idx, k, nObj)
			if err != nil {
				return nil, err
			}
			nodeNextKeys.Set(idx, nextKey)
		}

		skipListSeg := capn.NewBuffer(nil)
		skipListCap := msgs.NewRootSkipListCap(skipListSeg)
		skipListCap.SetLength(sCap.Length() + 1)
		skipListCap.SetLevelProbabilities(sCap.LevelProbabilities())
		skipListCap.SetCurDepth(sCap.CurDepth())
		skipListCap.SetCurCapacity(sCap.CurCapacity())

		skipListBuf := new(bytes.Buffer)
		if _, err := skipListSeg.WriteTo(skipListBuf); err != nil {
			return nil, err
		}
		skipListBytes := skipListBuf.Bytes()

		if err = sObj.Set(skipListBytes, sObjRefs...); err != nil {
			return nil, err
		}

		nodeBuf := new(bytes.Buffer)
		if _, err := nodeSeg.WriteTo(nodeBuf); err != nil {
			return nil, err
		}
		if err = nObj.Set(nodeBuf.Bytes(), nodeRefs...); err != nil {
			return nil, err
		}

		return nObj, nil
	})
	if err != nil {
		return nil, err
	}
	return &Node{
		SkipList: s,
		ObjRef:   result.(client.ObjectRef),
	}, nil
}

func (s *SkipList) removeNode(curObj client.ObjectRef) error {
	_, _, err := s.within(func(sObj client.ObjectRef, sObjRefs []client.ObjectRef, sCap *msgs.SkipListCap, txn *client.Txn) (interface{}, error) {
		_, _, err := s.withinNode(curObj, func(curCap *msgs.SkipListNodeCap, curObj client.ObjectRef, curRefs []client.ObjectRef, txn *client.Txn) (interface{}, error) {
			curKeys := curCap.NextKeys()
			prevObj := curRefs[2]
			nextObj := curRefs[3]

			nextRefs, err := nextObj.References()
			if err != nil {
				return nil, err
			}
			nextRefs[2] = prevObj
			nextVal, err := nextObj.Value()
			if err != nil {
				return nil, err
			}
			nextObj.Set(nextVal, nextRefs...)

			k, _, err := s.withinNode(prevObj, func(prevCap *msgs.SkipListNodeCap, prevObj client.ObjectRef, prevRefs []client.ObjectRef, txn *client.Txn) (interface{}, error) {
				return prevCap.Key(), nil
			})
			if err != nil {
				return nil, err
			}
			_, descent, err := s.getEqOrLessThan(k.([]byte))
			if err != nil {
				return nil, err
			}

			for idx, obj := range descent[:len(curRefs)-3] {
				_, err := s.setNextKey(obj, idx, curKeys.At(idx), curRefs[idx+3])
				if err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		if err != nil {
			return nil, err
		}

		skipListSeg := capn.NewBuffer(nil)
		skipListCap := msgs.NewRootSkipListCap(skipListSeg)
		skipListCap.SetLength(sCap.Length() - 1)
		skipListCap.SetLevelProbabilities(sCap.LevelProbabilities())
		skipListCap.SetCurDepth(sCap.CurDepth())
		skipListCap.SetCurCapacity(sCap.CurCapacity())

		skipListBuf := new(bytes.Buffer)
		if _, err := skipListSeg.WriteTo(skipListBuf); err != nil {
			return nil, err
		}
		skipListBytes := skipListBuf.Bytes()
		return nil, sObj.Set(skipListBytes, sObjRefs...)
	})
	return err
}

func (s *SkipList) refFromTerminus(idx int) (*Node, error) {
	result, _, err := s.within(func(sObj client.ObjectRef, sObjRefs []client.ObjectRef, sCap *msgs.SkipListCap, txn *client.Txn) (interface{}, error) {
		tObj := sObjRefs[0]
		tObjRefs, err := tObj.References()
		if err != nil {
			return nil, err
		}
		firstObj := tObjRefs[idx]
		if firstObj.ReferencesSameAs(tObj) {
			return nil, nil
		}
		return firstObj, nil
	})
	id, ok := result.(client.ObjectRef)
	switch {
	case err != nil:
		return nil, err
	case ok:
		return &Node{SkipList: s, ObjRef: id}, nil
	default:
		return nil, nil
	}
}

func (s *SkipList) Length() (uint64, error) {
	result, _, err := s.within(func(sObj client.ObjectRef, sObjRefs []client.ObjectRef, sCap *msgs.SkipListCap, txn *client.Txn) (interface{}, error) {
		return sCap.Length(), nil
	})
	if err != nil {
		return 0, err
	} else {
		return result.(uint64), nil
	}
}

func (s *SkipList) First() (*Node, error) {
	return s.refFromTerminus(3)
}

func (s *SkipList) Last() (*Node, error) {
	return s.refFromTerminus(2)
}

func (s *SkipList) Get(k []byte) (*Node, error) {
	result, _, err := s.Connection.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		sObj, err := txn.GetObject(s.ObjRef)
		if err != nil {
			return nil, err
		}
		sObjRefs, err := sObj.References()
		if err != nil {
			return nil, err
		}
		tObj := sObjRefs[0]
		obj, _, err := s.getEqOrLessThan(k)
		if err != nil {
			return nil, err
		}
		if obj.ReferencesSameAs(tObj) {
			return nil, nil
		}
		eq, _, err := s.withinNode(obj, func(curCap *msgs.SkipListNodeCap, curObj client.ObjectRef, curRefs []client.ObjectRef, txn *client.Txn) (interface{}, error) {
			return bytes.Equal(curCap.Key(), k), nil
		})
		if err != nil {
			return nil, err
		}
		if eq.(bool) {
			return obj, nil
		} else {
			return nil, nil
		}
	})
	id, ok := result.(client.ObjectRef)
	switch {
	case err != nil:
		return nil, err
	case ok:
		return &Node{SkipList: s, ObjRef: id}, nil
	default:
		return nil, nil
	}
}

func (s *SkipList) setNextKey(objRef client.ObjectRef, lvl int, newKey []byte, newObj client.ObjectRef) ([]byte, error) {
	result, _, err := s.withinNode(objRef, func(curCap *msgs.SkipListNodeCap, curObj client.ObjectRef, curRefs []client.ObjectRef, txn *client.Txn) (interface{}, error) {
		newSeg := capn.NewBuffer(nil)
		newCap := msgs.NewRootSkipListNodeCap(newSeg)
		newCap.SetHeightRand(curCap.HeightRand())
		newCap.SetKey(curCap.Key())
		oldNextKey := curCap.NextKeys().At(lvl)
		newCap.SetNextKeys(curCap.NextKeys())
		newCap.NextKeys().Set(lvl, newKey)

		newBuf := new(bytes.Buffer)
		if _, err := newSeg.WriteTo(newBuf); err != nil {
			return nil, err
		}
		newBytes := newBuf.Bytes()

		curRefs[lvl+3] = newObj
		if err := curObj.Set(newBytes, curRefs...); err != nil {
			return nil, err
		}

		return oldNextKey, nil
	})
	if err != nil {
		return nil, err
	} else {
		return result.([]byte), nil
	}
}

func (n *Node) Key() ([]byte, error) {
	result, _, err := n.SkipList.withinNode(n.ObjRef, func(curCap *msgs.SkipListNodeCap, curObj client.ObjectRef, curRefs []client.ObjectRef, txn *client.Txn) (interface{}, error) {
		return curCap.Key(), nil
	})
	if err != nil {
		return nil, err
	} else {
		return result.([]byte), err
	}
}

func (n *Node) Value() ([]byte, error) {
	result, _, err := n.SkipList.Connection.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		cObj, err := txn.GetObject(n.ObjRef)
		if err != nil {
			return nil, err
		}
		cObjRefs, err := cObj.References()
		if err != nil {
			return nil, err
		}
		return cObjRefs[1].Value()
	})
	if err != nil {
		return nil, err
	} else {
		return result.([]byte), err
	}
}

func (n *Node) Next() (*Node, error) {
	return n.refFrom(3)
}

func (n *Node) Prev() (*Node, error) {
	return n.refFrom(2)
}

func (n *Node) refFrom(idx int) (*Node, error) {
	result, _, err := n.SkipList.Connection.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		sObj, err := txn.GetObject(n.SkipList.ObjRef)
		if err != nil {
			return nil, err
		}
		sObjRefs, err := sObj.References()
		if err != nil {
			return nil, err
		}
		tObj := sObjRefs[0]
		cObj, err := txn.GetObject(n.ObjRef)
		if err != nil {
			return nil, err
		}
		cObjRefs, err := cObj.References()
		if err != nil {
			return nil, err
		}
		nObj := cObjRefs[idx]
		if nObj.ReferencesSameAs(tObj) {
			return nil, nil
		}
		return nObj, nil
	})
	id, ok := result.(client.ObjectRef)
	switch {
	case err != nil:
		return nil, err
	case ok:
		return &Node{SkipList: n.SkipList, ObjRef: id}, nil
	default:
		return nil, nil
	}
}

func (n *Node) Remove() error {
	_, _, err := n.SkipList.Connection.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		k, err := n.Key()
		if err != nil {
			return nil, err
		}
		m, err := n.SkipList.Get(k)
		if err != nil {
			return nil, err
		}
		if m.ObjRef.ReferencesSameAs(n.ObjRef) {
			return nil, n.SkipList.removeNode(n.ObjRef)
		}
		return nil, nil
	})
	return err
}

func (a *Node) Equal(b *Node) bool {
	return a.ObjRef.ReferencesSameAs(b.ObjRef)
}
