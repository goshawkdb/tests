using Go = import "go.capnp";

$Go.package("capnp");
$Go.import("goshawkdb.io/tests/skiplist/skiplist/capnp");

@0x8e9e413cece820ef;

struct SkipListCap {
  length             @0: UInt64;
  levelProbabilities @1: List(Float32);
  curDepth           @2: UInt64;
  curCapacity        @3: UInt64;
}

struct SkipListNodeCap {
  heightRand @0: Float32;
  key        @1: Data;
  nextKeys   @2: List(Data);
}
