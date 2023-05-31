import fs from 'fs';
import { buf as crc32c } from 'crc-32/crc32c';
import path from 'path';
import { readKindMetadata } from './protobufRawReader';
import { FirestoreParser } from './firestoreParser';

const ZERO = 0;
const FULL = 1;
const FIRST = 2;
const MIDDLE = 3;
const LAST = 4;

const TYPE_CRC32C_TABLE = [
  crc32c([ZERO]) >>> 0,
  crc32c([FULL]) >>> 0,
  crc32c([FIRST]) >>> 0,
  crc32c([MIDDLE]) >>> 0,
  crc32c([LAST]) >>> 0,
];

export interface Record {
  type: number;
  data: Buffer;
}

export class FirestoreBackupReader {
  public static BLOCK_SIZE = 32768;

  public debug: boolean = false;
  public filePath: string;
  public fileLength: number;
  public doChecksum: boolean;
  public fd: number;
  public fdPos: number;

  private blockBuffer: Buffer = Buffer.alloc(FirestoreBackupReader.BLOCK_SIZE);
  private blockOffset: number = FirestoreBackupReader.BLOCK_SIZE;
  private blockLength: number = 0;

  public constructor(filePath: string, doChecksum: boolean) {
    this.filePath = filePath;
    this.doChecksum = doChecksum;
    this.fd = fs.openSync(this.filePath, 'r');
    this.fdPos = 0;
    const stat = fs.fstatSync(this.fd);
    this.fileLength = stat.size;
  }

  public async readAll() {
    for (let i = 0; i < 1; i++) {
      try {
        const data = await this.readDocument();

        if (data.length !== 0) {
          const object = FirestoreParser.convertBufferToObject(data);
          console.log(object);
        }

        // check zero buffer
        const blockRemainingBytes = ((this.blockOffset & 0x7fff) ^ 0x7fff) + 1;
        // should not be zero, something wrong, sikp to next block
        // or block bytes <= 6, sikp to next block
        if (data.length === 0 || blockRemainingBytes <= 6) {
          this.blockOffset += blockRemainingBytes;
        }
      } catch (e) {
        console.log(e);
        break;
      }
      if (this.blockOffset >= this.blockLength && this.fdPos >= this.fileLength) {
        // file end
        fs.closeSync(this.fd);
        break;
      }
    }
  }

  public async readDocument(): Promise<Buffer> {
    let data = Buffer.alloc(0);
    do {
      const record = await this.readRecord();
      if (record.type < 0 || record.type > 4) {
        throw new Error(`UnknownRecordType ${record.type}`);
      }
      data = Buffer.concat([data, record.data]);
      if (record.type === FULL || record.type === LAST || record.type === ZERO) {
        return data;
      }
    } while (true);
  }

  // input as leveldb log format record start buffer, output is data buffer
  public async readRecord(): Promise<Record> {
    if (FirestoreBackupReader.BLOCK_SIZE - this.blockOffset <= 6) {
      await this.readBlockBuffer();
    }
    if (this.blockOffset >= this.blockLength) {
      throw new Error('FileEnd');
    }
    const bs = this.blockBuffer.subarray(this.blockOffset);
    const checksum = bs.readUInt32LE(0);
    const length = bs.readUInt16LE(4);
    const type = bs.readUint8(6);
    const data = bs.subarray(7, length + 7);
    const checksum2 = FirestoreBackupReader.calculateChecksum(data, type);
    if (this.doChecksum && checksum != 0 && checksum !== checksum2) {
      throw new Error('ChecksumNotMatch');
    }
    if (this.debug) {
      console.log('readRecord', checksum, checksum2, length, type);
    }
    this.blockOffset += 7 + length;
    return { type, data };
  }

  private readBlockBuffer(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      fs.read(
        this.fd,
        this.blockBuffer,
        0,
        FirestoreBackupReader.BLOCK_SIZE,
        this.fdPos,
        (err: Error | null, bytesRead: number) => {
          if (err !== null) {
            reject(err);
            return;
          }
          this.fdPos += bytesRead;
          this.blockOffset = 0;
          this.blockLength = bytesRead;
          if (this.debug) {
            console.log(`readBlockBuffer ${this.fdPos}, ${bytesRead}`);
          }
          resolve();
        }
      );
    });
  }

  private static calculateChecksum(data: Buffer, type: number) {
    const checksum2 = crc32c(data, TYPE_CRC32C_TABLE[type]) >>> 0;
    return ((((checksum2 >>> 15) | (checksum2 << 17)) + 0xa282ead8) & 0xffffffff) >>> 0;
  }
}

export function getMetadataFilenames(exportMetadataPath: string): string[] {
  const metadataBS = fs.readFileSync(exportMetadataPath);
  const metadata = readKindMetadata(metadataBS);
  const filenames: string[] = [];
  for (const nest1 of metadata) {
    if (nest1.id !== 2 || !Array.isArray(nest1.value)) {
      continue;
    }
    for (const nest2 of nest1.value) {
      if (nest2.id === 2 && typeof nest2.value === 'string') {
        filenames.push(nest2.value);
      }
    }
  }
  return filenames;
}

/**
 * Read firestore export documents
 * @param exportMetadataPath Should be metadata in table folder all_namespaces/kind_xxxxx/all_namespaces_kind_xxxxx.export_metadata
 */
export async function readFirestoreExport(exportMetadataPath: string, callback: (data: any) => void) {
  const outputFilenames = getMetadataFilenames(exportMetadataPath);
  const dirname = path.dirname(exportMetadataPath);
  for (const filename of outputFilenames) {
    const fullpath = `${dirname}${path.sep}${filename}`;
    const reader = new FirestoreBackupReader(fullpath, true);
    await reader.readAll();
    break;
  }
}
