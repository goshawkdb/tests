package capnp

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
	"math"
)

type SkipListCap C.Struct

func NewSkipListCap(s *C.Segment) SkipListCap      { return SkipListCap(s.NewStruct(24, 1)) }
func NewRootSkipListCap(s *C.Segment) SkipListCap  { return SkipListCap(s.NewRootStruct(24, 1)) }
func AutoNewSkipListCap(s *C.Segment) SkipListCap  { return SkipListCap(s.NewStructAR(24, 1)) }
func ReadRootSkipListCap(s *C.Segment) SkipListCap { return SkipListCap(s.Root(0).ToStruct()) }
func (s SkipListCap) Length() uint64               { return C.Struct(s).Get64(0) }
func (s SkipListCap) SetLength(v uint64)           { C.Struct(s).Set64(0, v) }
func (s SkipListCap) LevelProbabilities() C.Float32List {
	return C.Float32List(C.Struct(s).GetObject(0))
}
func (s SkipListCap) SetLevelProbabilities(v C.Float32List) { C.Struct(s).SetObject(0, C.Object(v)) }
func (s SkipListCap) CurDepth() uint64                      { return C.Struct(s).Get64(8) }
func (s SkipListCap) SetCurDepth(v uint64)                  { C.Struct(s).Set64(8, v) }
func (s SkipListCap) CurCapacity() uint64                   { return C.Struct(s).Get64(16) }
func (s SkipListCap) SetCurCapacity(v uint64)               { C.Struct(s).Set64(16, v) }
func (s SkipListCap) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"length\":")
	if err != nil {
		return err
	}
	{
		s := s.Length()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"levelProbabilities\":")
	if err != nil {
		return err
	}
	{
		s := s.LevelProbabilities()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"curDepth\":")
	if err != nil {
		return err
	}
	{
		s := s.CurDepth()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"curCapacity\":")
	if err != nil {
		return err
	}
	{
		s := s.CurCapacity()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s SkipListCap) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s SkipListCap) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("length = ")
	if err != nil {
		return err
	}
	{
		s := s.Length()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("levelProbabilities = ")
	if err != nil {
		return err
	}
	{
		s := s.LevelProbabilities()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("curDepth = ")
	if err != nil {
		return err
	}
	{
		s := s.CurDepth()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("curCapacity = ")
	if err != nil {
		return err
	}
	{
		s := s.CurCapacity()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s SkipListCap) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type SkipListCap_List C.PointerList

func NewSkipListCapList(s *C.Segment, sz int) SkipListCap_List {
	return SkipListCap_List(s.NewCompositeList(24, 1, sz))
}
func (s SkipListCap_List) Len() int             { return C.PointerList(s).Len() }
func (s SkipListCap_List) At(i int) SkipListCap { return SkipListCap(C.PointerList(s).At(i).ToStruct()) }
func (s SkipListCap_List) ToArray() []SkipListCap {
	n := s.Len()
	a := make([]SkipListCap, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s SkipListCap_List) Set(i int, item SkipListCap) { C.PointerList(s).Set(i, C.Object(item)) }

type SkipListNodeCap C.Struct

func NewSkipListNodeCap(s *C.Segment) SkipListNodeCap { return SkipListNodeCap(s.NewStruct(8, 2)) }
func NewRootSkipListNodeCap(s *C.Segment) SkipListNodeCap {
	return SkipListNodeCap(s.NewRootStruct(8, 2))
}
func AutoNewSkipListNodeCap(s *C.Segment) SkipListNodeCap { return SkipListNodeCap(s.NewStructAR(8, 2)) }
func ReadRootSkipListNodeCap(s *C.Segment) SkipListNodeCap {
	return SkipListNodeCap(s.Root(0).ToStruct())
}
func (s SkipListNodeCap) HeightRand() float32      { return math.Float32frombits(C.Struct(s).Get32(0)) }
func (s SkipListNodeCap) SetHeightRand(v float32)  { C.Struct(s).Set32(0, math.Float32bits(v)) }
func (s SkipListNodeCap) Key() []byte              { return C.Struct(s).GetObject(0).ToData() }
func (s SkipListNodeCap) SetKey(v []byte)          { C.Struct(s).SetObject(0, s.Segment.NewData(v)) }
func (s SkipListNodeCap) NextKeys() C.DataList     { return C.DataList(C.Struct(s).GetObject(1)) }
func (s SkipListNodeCap) SetNextKeys(v C.DataList) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s SkipListNodeCap) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"heightRand\":")
	if err != nil {
		return err
	}
	{
		s := s.HeightRand()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"key\":")
	if err != nil {
		return err
	}
	{
		s := s.Key()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"nextKeys\":")
	if err != nil {
		return err
	}
	{
		s := s.NextKeys()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s SkipListNodeCap) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s SkipListNodeCap) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("heightRand = ")
	if err != nil {
		return err
	}
	{
		s := s.HeightRand()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("key = ")
	if err != nil {
		return err
	}
	{
		s := s.Key()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("nextKeys = ")
	if err != nil {
		return err
	}
	{
		s := s.NextKeys()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				buf, err = json.Marshal(s)
				if err != nil {
					return err
				}
				_, err = b.Write(buf)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s SkipListNodeCap) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type SkipListNodeCap_List C.PointerList

func NewSkipListNodeCapList(s *C.Segment, sz int) SkipListNodeCap_List {
	return SkipListNodeCap_List(s.NewCompositeList(8, 2, sz))
}
func (s SkipListNodeCap_List) Len() int { return C.PointerList(s).Len() }
func (s SkipListNodeCap_List) At(i int) SkipListNodeCap {
	return SkipListNodeCap(C.PointerList(s).At(i).ToStruct())
}
func (s SkipListNodeCap_List) ToArray() []SkipListNodeCap {
	n := s.Len()
	a := make([]SkipListNodeCap, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s SkipListNodeCap_List) Set(i int, item SkipListNodeCap) {
	C.PointerList(s).Set(i, C.Object(item))
}
