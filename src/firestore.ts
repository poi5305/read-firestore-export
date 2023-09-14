import fs from 'fs';
import { buf as crc32c } from 'crc-32/crc32c';
import path from 'path';
import { readKindMetadata } from './protobufRawReader';
import { FirestoreParserFaster } from './firestoreParser';
import { Worker } from 'worker_threads';
import Bluebird from 'bluebird';

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

export type CallbackType = ((d: any) => void) | ((d: any) => Promise<void>);

export class FirestoreBackupReader {
  public static BLOCK_SIZE = 32768;

  public debug: boolean = false;
  public filePath: string;
  public fileLength: number;
  public doChecksum: boolean;
  public fd: number;
  public fdPos: number;
  public isEnd: boolean = false;

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

  public async readAll(callback: CallbackType) {
    while (true) {
      if (this.isEnd) {
        break;
      }
      const object = await this.readOne();
      if (object === undefined) {
        continue;
      }
      const p = callback(object);
      if (p instanceof Promise) {
        await p;
      }
    }
  }

  public async readOne(): Promise<any | undefined> {
    const data = await this.readDocumentBS();
    // check zero buffer
    const blockRemainingBytes = ((this.blockOffset & 0x7fff) ^ 0x7fff) + 1;
    // should not be zero, something wrong, sikp to next block
    // or block bytes <= 6, sikp to next block
    if (data.length === 0 || blockRemainingBytes <= 6) {
      this.blockOffset += blockRemainingBytes;
    }
    if (this.blockOffset >= this.blockLength && this.fdPos >= this.fileLength) {
      this.isEnd = true;
      fs.closeSync(this.fd);
    }
    if (data.length !== 0) {
      const object = FirestoreParserFaster.convertBufferToObject(data);
      // const object = FirestoreParser.convertBufferToObject(data);
      return object;
    }
    return undefined;
  }

  public async readDocumentBS(): Promise<Buffer> {
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
  private async readRecord(): Promise<Record> {
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
export async function readAllFirestoreExport(exportMetadataPath: string, callback: CallbackType) {
  const outputFilenames = getMetadataFilenames(exportMetadataPath);
  const dirname = path.dirname(exportMetadataPath);
  for (const filename of outputFilenames) {
    const fullpath = `${dirname}${path.sep}${filename}`;
    const reader = new FirestoreBackupReader(fullpath, true);
    await reader.readAll(callback);
  }
}

/**
 * Read firestore one export documents
 * @param outputPath Should be metadata in table folder all_namespaces/kind_xxxxx/output-9
 */
export async function readOneFirestoreExport(outputPath: string, callback: CallbackType) {
  const reader = new FirestoreBackupReader(outputPath, true);
  await reader.readAll(callback);
}

export async function readAllFirestoreExportThreads(
  exportMetadataPath: string,
  callback: CallbackType,
  threads: number = 4
) {
  const outputFilenames = getMetadataFilenames(exportMetadataPath);
  const dirname = path.dirname(exportMetadataPath);
  await Bluebird.map(
    outputFilenames,
    async (filename: string) =>
      new Promise<void>(resolve => {
        const fullpath = `${dirname}${path.sep}${filename}`;
        const workerJS = path.resolve(__dirname, './workerThreadImpl.js');
        const workerWrapperJS = path.resolve(__dirname, './workerThread.js');
        const workerPath = fs.existsSync(workerJS) ? workerJS : workerWrapperJS;
        const worker = new Worker(workerPath, {
          workerData: { fullpath: fullpath },
        });
        worker.on('message', callback);
        worker.on('exit', code => {
          console.log(`done ${fullpath}`);
          resolve();
        });
      }),
    { concurrency: threads }
  );
}
