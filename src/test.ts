import fs, { promises as fsPromises, readv } from 'fs';
import { buf } from 'crc-32/crc32c';
import protobuf from 'protobufjs';
import { FirestoreBackupDocumentProtoJSON, readFirestoreExport } from './firestore';

async function ttt() {
  const root = await protobuf.load('ok.proto');
  const EntityProto = root.lookupType('EntityProto');
}

const BLOCK_SIZE = 32768;

const ZERO = 0;
const FULL = 1;
const FIRST = 2;
const MIDDLE = 3;
const LAST = 4;

const TYPE_CRC32C_TABLE = [
  buf([ZERO]) >>> 0,
  buf([FULL]) >>> 0,
  buf([FIRST]) >>> 0,
  buf([MIDDLE]) >>> 0,
  buf([LAST]) >>> 0,
];

interface Record {
  end: number;
  type: number;
  data: Buffer;
}

async function readfile() {
  let now = Date.now();
  await readFirestoreExport(
    '/mnt/data/firestorebackup/2023-03-13T18:00:03_70712/all_namespaces/kind_v1_Channel/all_namespaces_kind_v1_Channel.export_metadata',
    (data: any) => {
      console.log(data);
    }
  );

  return;
  // const bs2 = fs.readFileSync('./ok.proto');
  const root = protobuf.Root.fromJSON(FirestoreBackupDocumentProtoJSON);
  // const root = await protobuf.load('./ok.proto');
  // console.log(JSON.stringify(root.toJSON()))
  // return;
  const EntityProto = root.lookupType('EntityProto');
  console.log('proto time', Date.now() - now);

  const bs = await fsPromises.readFile(
    '/mnt/data/firestorebackup/2023-03-13T18:00:03_70712/all_namespaces/kind_v1_Channel/output-0'
  );

  now = Date.now();
  let start = 0;
  for (let i = 0; i < 100000; i++) {
    try {
      const data = readData(bs.subarray(start), true);
      if (data.data.length !== 0) {
        // decode protobuf
        const message = EntityProto.decode(data.data);
      }

      start += data.end;
      const blockRemainingBytes = ((start & 0x7fff) ^ 0x7fff) + 1;
      // should not be zero, something wrong, sikp to next block
      // or block bytes <= 6, sikp to next block
      if (data.data.length === 0 || blockRemainingBytes <= 6) {
        start += blockRemainingBytes;
      }
    } catch (e) {
      console.log(start, e);
      break;
    }
    if (start >= bs.length) {
      // file end
      break;
    }
  }
  console.log(Date.now() - now);
}

function readData(bs: Buffer, doChecksum: boolean): { data: Buffer; end: number } {
  let data = Buffer.alloc(0);
  let end = 0;
  do {
    const record = readRecord(bs, doChecksum);
    if (record.type < 0 || record.type > 4) {
      throw new Error(`UnknownRecordType ${record.type}`);
    }
    data = Buffer.concat([data, record.data]);
    end += record.end;
    if (record.type === FULL || record.type === LAST || record.type === ZERO) {
      return {
        data: data,
        end: end,
      };
    }
    bs = bs.subarray(record.end);
  } while (true);
}

// input as leveldb log format record start buffer, output is data buffer
function readRecord(bs: Buffer, doChecksum: boolean): Record {
  const checksum = bs.readUInt32LE(0);
  const length = bs.readUInt16LE(4);
  const type = bs.readUint8(6);
  const data = bs.subarray(7, length + 7);
  const checksum2 = calculateChecksum(data, type);

  if (doChecksum && checksum != 0 && checksum !== checksum2) {
    throw new Error('ChecksumNotMatch');
  }
  // console.log(checksum, checksum2, length, type);
  return { end: length + 7, type, data };
}

function calculateChecksum(data: Buffer, type: number) {
  const checksum2 = buf(data, TYPE_CRC32C_TABLE[type]) >>> 0;
  return ((((checksum2 >>> 15) | (checksum2 << 17)) + 0xa282ead8) & 0xffffffff) >>> 0;
}

readfile();

function test(bs: Buffer) {
  const checksum = bs.readUInt32LE(0);
  const length = bs.readUInt16LE(4);
  const type = bs.readUint8(6);
  const data = bs.subarray(7, length + 7);
  // const checksum2 = buf(data, TYPE_CRC32C_TABLE[type]) >>> 0;
  const checksum2 = calculateChecksum(data, type);

  // const v = buf([type]) >>> 0;
  // console.log(v, TYPE_CRC32C_TABLE[type]);

  // const kMaskDelta = 0xa282ead8 >>> 0;
  // const cs = ((checksum2 >>> 15) | (checksum2 << 17)) + kMaskDelta;

  // console.log('debug', kMaskDelta.toString(2));
  // console.log('debug', checksum2.toString(2));
  // console.log('debug', (checksum2 >>> 15).toString(2));
  // console.log('debug', (checksum2 << 17).toString(2));
  // console.log('debug', ((checksum2 >>> 15) | (checksum2 << 17)).toString(2));

  // console.log(kMaskDelta, (checksum2 >>> 15) | (checksum2 << 17));
  // console.log(checksum2, checksum2 >>> 15);
  // 11110000111101011000110101110010
  // 1111000010101

  // console.log(cs & 0xffffffff, cs.toString(2));

  console.log(checksum, checksum2, length, type, data.toString().substring(0, 20));
  // assert(checksum === checksum2)
}
