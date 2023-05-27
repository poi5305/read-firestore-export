import protobuf from 'protobufjs';

export interface ProtobufValue {
  id: number;
  value: number | string | ProtobufValue[];
}

export function readTag(value: number): [number, number] {
  const wiretype = value & 7;
  const id = value >>> 3;
  return [id, wiretype];
}

export function decodeProtobuf(bs: protobuf.BufferReader): ProtobufValue[] {
  const protobufValues: ProtobufValue[] = [];
  while (bs.pos < bs.len) {
    const [id, wiretype] = readTag(bs.buf[bs.pos]);
    if (wiretype === 2) {
      const bytes = bs.skip(1).bytes();
      const [childId, childType] = readTag(bytes[0]);
      if (childType === 2 || childType === 0) {
        try {
          const childReader = new protobuf.BufferReader(bytes);
          const childValues = decodeProtobuf(childReader);
          protobufValues.push({
            id: id,
            value: childValues,
          });
        } catch (e) {
          protobufValues.push({
            id: id,
            value: bytes.toString(),
          });
        }
      } else {
        protobufValues.push({
          id: id,
          value: bytes.toString(),
        });
      }
    } else if (wiretype === 0) {
      // the value might be wrong here, if varint value large than 32 bits
      protobufValues.push({
        id: id,
        value: bs.skip(1).int64().low,
      });
    } else if (wiretype === 1) {
      protobufValues.push({
        id: id,
        value: bs.skip(1).double(),
      });
    } else if (wiretype === 5) {
      protobufValues.push({
        id: id,
        value: bs.skip(1).float(),
      });
    } else {
      throw new Error('UnknownWiretype');
    }
  }
  return protobufValues;
}

export function readKindMetadata(bs: Buffer): ProtobufValue[] {
  const metadata = new protobuf.BufferReader(bs);
  const values = decodeProtobuf(metadata);
  return values;
}
