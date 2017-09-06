package skiplist

import (
	"bytes"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/tests/integration/skiplist/skiplist/capnp"
	"math"
	"math/rand"
)

const (
	p            = 0.25
	defaultDepth = 2
)

type SkipList struct {
	ObjPtr client.RefCap
	rng    *rand.Rand
}

type Node struct {
	SkipList *SkipList
	ObjPtr   client.RefCap
}

func NewSkipList(txn *client.Transaction, rng *rand.Rand) (*SkipList, error) {
	depth := defaultDepth

	terminusSeg := capn.NewBuffer(nil)
	terminusCap := msgs.NewRootSkipListNodeCap(terminusSeg)
	terminusCap.SetHeightRand(0)
	terminusCap.SetNextKeys(terminusSeg.NewDataList(depth))

	terminusBytes := common.SegToBytes(terminusSeg)

	skipListSeg := capn.NewBuffer(nil)
	skipListCap := msgs.NewRootSkipListCap(skipListSeg)
	skipListCap.SetLength(0)
	probsCap := skipListSeg.NewFloat32List(1)
	skipListCap.SetLevelProbabilities(probsCap)
	probsCap.Set(0, p)
	skipListCap.SetCurDepth(uint64(depth))
	skipListCap.SetCurCapacity(calculateCapacity(uint64(depth)))

	skipListBytes := common.SegToBytes(skipListSeg)

	if terminusPtr, err := txn.Create(terminusBytes); err != nil || txn.RestartNeeded() {
		return nil, err
	} else if skipListPtr, err := txn.Create(skipListBytes, terminusPtr); err != nil || txn.RestartNeeded() {
		return nil, err
	} else {
		//                                       sl           val          prev
		terminusRefs := []client.RefCap{skipListPtr, terminusPtr, terminusPtr}
		for idx := 0; idx < depth; idx++ {
			terminusRefs = append(terminusRefs, terminusPtr)
		}
		if err = txn.Write(terminusPtr, terminusBytes, terminusRefs...); err != nil || txn.RestartNeeded() {
			return nil, err
		} else {
			return &SkipList{
				ObjPtr: skipListPtr,
				rng:    rng,
			}, nil
		}
	}
}

func SkipListFromObjPtr(rng *rand.Rand, objPtr client.RefCap) *SkipList {
	return &SkipList{
		ObjPtr: objPtr,
		rng:    rng,
	}
}

func calculateCapacity(curDepth uint64) uint64 {
	base := float64(1.0) / p
	capacity := math.Pow(base, float64(curDepth))
	return uint64(math.Floor(capacity))
}

func (s *SkipList) within(txn client.Transactor, fun func([]client.RefCap, *msgs.SkipListCap, *client.Transaction) (interface{}, error)) (interface{}, error) {
	return txn.Transact(func(txn *client.Transaction) (interface{}, error) {
		// log.Printf("within starting %v\n", fun)
		if sVal, sRefs, err := txn.Read(s.ObjPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if sSeg, _, err := capn.ReadFromMemoryZeroCopy(sVal); err != nil {
			return nil, err
		} else {
			sCap := msgs.ReadRootSkipListCap(sSeg)
			return fun(sRefs, &sCap, txn)
		}
	})
}

func (s *SkipList) withinNode(txn client.Transactor, nodeId client.RefCap, fun func(*msgs.SkipListNodeCap, []client.RefCap, *client.Transaction) (interface{}, error)) (interface{}, error) {
	return txn.Transact(func(txn *client.Transaction) (interface{}, error) {
		// log.Printf("withinNode starting %v\n", fun)
		if nVal, nRefs, err := txn.Read(nodeId); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if nSeg, _, err := capn.ReadFromMemoryZeroCopy(nVal); err != nil {
			return nil, err
		} else {
			nCap := msgs.ReadRootSkipListNodeCap(nSeg)
			return fun(&nCap, nRefs, txn)
		}
	})
}

func (s *SkipList) chooseNumLevels(txn *client.Transaction) (float32, int, error) {
	r := s.rng.Float32()
	result, err := s.within(txn, func(sRefs []client.RefCap, sCap *msgs.SkipListCap, txn *client.Transaction) (interface{}, error) {
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

func (s *SkipList) ensureCapacity(txn *client.Transaction) error {
	_, err := s.within(txn, func(sObjRefs []client.RefCap, sCap *msgs.SkipListCap, txn *client.Transaction) (interface{}, error) {
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

		skipListBytes := common.SegToBytes(skipListSeg)

		tObj := sObjRefs[0]
		if err := txn.Write(s.ObjPtr, skipListBytes, tObj); err != nil || txn.RestartNeeded() {
			return nil, err
		}

		cur := tObj
		_, tObjRefs, err := txn.Read(tObj)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		lvl := len(tObjRefs) - 1
		prev := cur
		for {
			_, curRefs, err := txn.Read(cur)
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}
			next := curRefs[lvl]
			newPrev, err := s.withinNode(txn, cur, func(curCap *msgs.SkipListNodeCap, curRefs []client.RefCap, txn *client.Transaction) (interface{}, error) {
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

					newBytes := common.SegToBytes(newSeg)

					if err := txn.Write(cur, newBytes, append(curRefs, tObj)...); err != nil || txn.RestartNeeded() {
						return nil, err
					}

					if _, err = s.setNextKey(txn, prev, lvl-2, curCap.Key(), cur); err != nil {
						return nil, err
					}
					return cur, nil
				}
				return prev, nil
			})
			if err != nil {
				return nil, err
			}
			prev = newPrev.(client.RefCap)
			if next.SameReferent(tObj) {
				break
			} else {
				cur = next
			}
		}

		return nil, nil
	})
	return err
}

func (s *SkipList) getEqOrLessThan(txn *client.Transaction, k []byte) (client.RefCap, []client.RefCap, error) {
	var node client.RefCap
	var descent []client.RefCap
	_, err := s.within(txn, func(sObjRefs []client.RefCap, sCap *msgs.SkipListCap, txn *client.Transaction) (interface{}, error) {
		// log.Printf("getEqOrLessThan starting\n")
		// defer log.Printf("getEqOrLessThan ended\n")
		descent = nil
		tObj := sObjRefs[0]
		cur := tObj
		_, curRefs, err := txn.Read(cur)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		lvl := len(curRefs) - 1
		descent = make([]client.RefCap, lvl-2)
		descent[lvl-3] = cur
		for ; lvl >= 3; lvl-- {
			for {
				_, curRefs, err := txn.Read(cur)
				if err != nil || txn.RestartNeeded() {
					return nil, err
				}
				next := curRefs[lvl]
				if next.SameReferent(tObj) {
					break
				}
				nextKey, err := s.withinNode(txn, cur, func(curCap *msgs.SkipListNodeCap, curRefs []client.RefCap, txn *client.Transaction) (interface{}, error) {
					// log.Printf("getEqOrLessThan inner starting\n")
					// defer log.Printf("getEqOrLessThan inner ended\n")
					return curCap.NextKeys().At(lvl - 3), nil
				})
				if err != nil || txn.RestartNeeded() {
					return nil, err
				}
				if len(nextKey.([]byte)) == 0 {
					panic(fmt.Sprintf("Encountered empty key for node %v (which is not the terminus)", next))
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
		return client.RefCap{}, nil, err
	}
	return node, descent, nil
}

func (s *SkipList) Insert(txr client.Transactor, k, v []byte) (*Node, error) {
	result, err := s.within(txr, func(sObjRefs []client.RefCap, sCap *msgs.SkipListCap, txn *client.Transaction) (interface{}, error) {
		// log.Printf("insert starting\n")
		// defer log.Printf("insert ended\n")
		tObj := sObjRefs[0]

		if err := s.ensureCapacity(txn); err != nil || txn.RestartNeeded() {
			return nil, err
		}
		curObj, descent, err := s.getEqOrLessThan(txn, k)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		vObj, err := txn.Create(v)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		if tObj.SameReferent(curObj) {
			eq, err := s.withinNode(txn, curObj, func(nCap *msgs.SkipListNodeCap, nRefs []client.RefCap, txn *client.Transaction) (interface{}, error) {
				// log.Printf("insert inner starting\n")
				// defer log.Printf("insert inner ended\n")
				return bytes.Equal(nCap.Key(), k), nil
			})
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}
			if eq.(bool) {
				curVal, curRefs, err := txn.Read(curObj)
				if err != nil || txn.RestartNeeded() {
					return nil, err
				}
				curRefs[1] = vObj
				if err = txn.Write(curObj, curVal, curRefs...); err != nil || txn.RestartNeeded() {
					return nil, err
				}
				return curObj, nil
			}
		}
		heightRand, height, err := s.chooseNumLevels(txn)
		// fmt.Printf("hr:%v;h:%v ", heightRand, height)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		descent = descent[:height]

		nodeSeg := capn.NewBuffer(nil)
		nodeCap := msgs.NewRootSkipListNodeCap(nodeSeg)
		nodeCap.SetHeightRand(heightRand)
		nodeCap.SetKey(k)
		nodeNextKeys := nodeSeg.NewDataList(height)
		nodeCap.SetNextKeys(nodeNextKeys)

		nodeRefs := []client.RefCap{s.ObjPtr, vObj, curObj}
		for idx, pObj := range descent {
			if _, pObjRefs, err := txn.Read(pObj); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				nodeRefs = append(nodeRefs, pObjRefs[idx+3])
			}
		}
		nObj, err := txn.Create([]byte{}, nodeRefs...)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}

		nextObj := descent[0]
		nextVal, nextRefs, err := txn.Read(nextObj)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		nextRefs[2] = nObj
		if err = txn.Write(nextObj, nextVal, nextRefs...); err != nil || txn.RestartNeeded() {
			return nil, err
		}

		for idx, pObj := range descent {
			nextKey, err := s.setNextKey(txn, pObj, idx, k, nObj)
			if err != nil || txn.RestartNeeded() {
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

		skipListBytes := common.SegToBytes(skipListSeg)

		if err = txn.Write(s.ObjPtr, skipListBytes, sObjRefs...); err != nil || txn.RestartNeeded() {
			return nil, err
		}

		nodeBites := common.SegToBytes(nodeSeg)
		if err = txn.Write(nObj, nodeBites, nodeRefs...); err != nil || txn.RestartNeeded() {
			return nil, err
		}

		return nObj, nil
	})
	if err != nil {
		return nil, err
	}
	return &Node{
		SkipList: s,
		ObjPtr:   result.(client.RefCap),
	}, nil
}

func (s *SkipList) removeNode(txn *client.Transaction, curObj client.RefCap) error {
	_, err := s.within(txn, func(sObjRefs []client.RefCap, sCap *msgs.SkipListCap, txn *client.Transaction) (interface{}, error) {
		_, err := s.withinNode(txn, curObj, func(curCap *msgs.SkipListNodeCap, curRefs []client.RefCap, txn *client.Transaction) (interface{}, error) {
			curKeys := curCap.NextKeys()
			prevObj := curRefs[2]
			nextObj := curRefs[3]

			nextVal, nextRefs, err := txn.Read(nextObj)
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}
			nextRefs[2] = prevObj
			if err := txn.Write(nextObj, nextVal, nextRefs...); err != nil || txn.RestartNeeded() {
				return nil, err
			}

			k, err := s.withinNode(txn, prevObj, func(prevCap *msgs.SkipListNodeCap, prevRefs []client.RefCap, txn *client.Transaction) (interface{}, error) {
				return prevCap.Key(), nil
			})
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}
			_, descent, err := s.getEqOrLessThan(txn, k.([]byte))
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}

			for idx, obj := range descent[:len(curRefs)-3] {
				_, err := s.setNextKey(txn, obj, idx, curKeys.At(idx), curRefs[idx+3])
				if err != nil || txn.RestartNeeded() {
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

		skipListBytes := common.SegToBytes(skipListSeg)
		return nil, txn.Write(s.ObjPtr, skipListBytes, sObjRefs...)
	})
	return err
}

func (s *SkipList) refFromTerminus(txn *client.Transaction, idx int) (*Node, error) {
	result, err := s.within(txn, func(sObjRefs []client.RefCap, sCap *msgs.SkipListCap, txn *client.Transaction) (interface{}, error) {
		tObj := sObjRefs[0]
		_, tObjRefs, err := txn.Read(tObj)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		firstObj := tObjRefs[idx]
		if firstObj.SameReferent(tObj) {
			return nil, nil
		}
		return firstObj, nil
	})
	id, ok := result.(client.RefCap)
	switch {
	case err != nil:
		return nil, err
	case ok:
		return &Node{SkipList: s, ObjPtr: id}, nil
	default:
		return nil, nil
	}
}

func (s *SkipList) Length(txn *client.Transaction) (uint64, error) {
	result, err := s.within(txn, func(sObjRefs []client.RefCap, sCap *msgs.SkipListCap, txn *client.Transaction) (interface{}, error) {
		return sCap.Length(), nil
	})
	if err != nil {
		return 0, err
	} else {
		return result.(uint64), nil
	}
}

func (s *SkipList) First(txn *client.Transaction) (*Node, error) {
	return s.refFromTerminus(txn, 3)
}

func (s *SkipList) Last(txn *client.Transaction) (*Node, error) {
	return s.refFromTerminus(txn, 2)
}

func (s *SkipList) Get(txr client.Transactor, k []byte) (*Node, error) {
	result, err := txr.Transact(func(txn *client.Transaction) (interface{}, error) {
		_, sRefs, err := txn.Read(s.ObjPtr)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		tObj := sRefs[0]
		obj, _, err := s.getEqOrLessThan(txn, k)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		if obj.SameReferent(tObj) {
			return nil, nil
		}
		eq, err := s.withinNode(txn, obj, func(curCap *msgs.SkipListNodeCap, curRefs []client.RefCap, txn *client.Transaction) (interface{}, error) {
			return bytes.Equal(curCap.Key(), k), nil
		})
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		if eq.(bool) {
			return obj, nil
		} else {
			return nil, nil
		}
	})
	id, ok := result.(client.RefCap)
	switch {
	case err != nil:
		return nil, err
	case ok:
		return &Node{SkipList: s, ObjPtr: id}, nil
	default:
		return nil, nil
	}
}

func (s *SkipList) setNextKey(txn *client.Transaction, objRef client.RefCap, lvl int, newKey []byte, newObj client.RefCap) ([]byte, error) {
	result, err := s.withinNode(txn, objRef, func(curCap *msgs.SkipListNodeCap, curRefs []client.RefCap, txn *client.Transaction) (interface{}, error) {
		newSeg := capn.NewBuffer(nil)
		newCap := msgs.NewRootSkipListNodeCap(newSeg)
		newCap.SetHeightRand(curCap.HeightRand())
		newCap.SetKey(curCap.Key())
		oldNextKey := curCap.NextKeys().At(lvl)
		newCap.SetNextKeys(curCap.NextKeys())
		newCap.NextKeys().Set(lvl, newKey)

		newBytes := common.SegToBytes(newSeg)

		curRefs[lvl+3] = newObj
		if err := txn.Write(objRef, newBytes, curRefs...); err != nil || txn.RestartNeeded() {
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

func (n *Node) Key(txr client.Transactor) ([]byte, error) {
	result, err := n.SkipList.withinNode(txr, n.ObjPtr, func(curCap *msgs.SkipListNodeCap, curRefs []client.RefCap, txn *client.Transaction) (interface{}, error) {
		return curCap.Key(), nil
	})
	if err != nil {
		return nil, err
	} else {
		return result.([]byte), err
	}
}

func (n *Node) Value(txr client.Transactor) ([]byte, error) {
	result, err := txr.Transact(func(txn *client.Transaction) (interface{}, error) {
		_, cObjRefs, err := txn.Read(n.ObjPtr)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		} else if val, _, err := txn.Read(cObjRefs[1]); err != nil || txn.RestartNeeded() {
			return nil, err
		} else {
			return val, nil
		}
	})
	if err != nil {
		return nil, err
	} else {
		return result.([]byte), err
	}
}

func (n *Node) Next(txn *client.Transaction) (*Node, error) {
	return n.refFrom(txn, 3)
}

func (n *Node) Prev(txn *client.Transaction) (*Node, error) {
	return n.refFrom(txn, 2)
}

func (n *Node) refFrom(txn *client.Transaction, idx int) (*Node, error) {
	result, err := txn.Transact(func(txn *client.Transaction) (interface{}, error) {
		_, sRefs, err := txn.Read(n.SkipList.ObjPtr)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		tObj := sRefs[0]
		_, cRefs, err := txn.Read(n.ObjPtr)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		nObj := cRefs[idx]
		if nObj.SameReferent(tObj) {
			return nil, nil
		}
		return nObj, nil
	})
	id, ok := result.(client.RefCap)
	switch {
	case err != nil:
		return nil, err
	case ok:
		return &Node{SkipList: n.SkipList, ObjPtr: id}, nil
	default:
		return nil, nil
	}
}

func (n *Node) Remove(txr client.Transactor) error {
	_, err := txr.Transact(func(txn *client.Transaction) (interface{}, error) {
		k, err := n.Key(txr)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		m, err := n.SkipList.Get(txn, k)
		if err != nil || txn.RestartNeeded() {
			return nil, err
		}
		if m.ObjPtr.SameReferent(n.ObjPtr) {
			return nil, n.SkipList.removeNode(txn, n.ObjPtr)
		}
		return nil, nil
	})
	return err
}

func (a *Node) Equal(b *Node) bool {
	return a.ObjPtr.SameReferent(b.ObjPtr)
}
