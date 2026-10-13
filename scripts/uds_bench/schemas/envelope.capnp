@0xc5bfdcf1a4e47b97;
struct Envelope {
  version @0 :UInt16 = 1;
  operation @1 :Operation;
  generation @2 :UInt64;
  stream @3 :UInt8;
  offset @4 :UInt64;
  payload @5 :Data;
  enum Operation { data @0; }
}
