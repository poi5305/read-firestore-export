import fs from 'fs';
import { buf as crc32c } from 'crc-32/crc32c';
import path from 'path';
import protobuf from 'protobufjs';
import { readKindMetadata } from './protobufRawReader';

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
  private entityProto: protobuf.Type;

  public constructor(filePath: string, doChecksum: boolean) {
    this.filePath = filePath;
    this.doChecksum = doChecksum;
    this.fd = fs.openSync(this.filePath, 'r');
    this.fdPos = 0;
    const stat = fs.fstatSync(this.fd);
    this.fileLength = stat.size;

    const root = protobuf.Root.fromJSON(FirestoreBackupDocumentProtoJSON);
    this.entityProto = root.lookupType('EntityProto');
  }

  public async readAll() {
    for (let i = 0; i < 1; i++) {
      try {
        const data = await this.readDocument();

        if (data.length !== 0) {
          // decode protobuf
          const message = this.entityProto.decode(data);
          console.log(JSON.stringify(message, null, 2));
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

// got from https://github.com/GoogleCloudPlatform/appengine-python-standard/blob/main/src/google/appengine/datastore/entity_bytes_pb2.py
export const FirestoreBackupDocumentProtoJSON: protobuf.INamespace = {
  nested: {
    storage_onestore_v3_bytes: {
      options: {
        go_package: 'storage_onestore_v3',
        java_outer_classname: 'OnestoreEntity',
        java_package: 'com.google.storage.onestore.v3',
      },
      nested: {
        PropertyValue: {
          fields: {
            int64Value: { type: 'int64', id: 1 },
            booleanValue: { type: 'bool', id: 2 },
            stringValue: { type: 'bytes', id: 3 },
            doubleValue: { type: 'double', id: 4 },
            pointValue: { type: 'PointValue', id: 5 },
            userValue: { type: 'UserValue', id: 8 },
            referenceValue: { type: 'ReferenceValue', id: 12 },
          },
          nested: {
            PointValue: {
              fields: {
                x: { rule: 'required', type: 'double', id: 6 },
                y: { rule: 'required', type: 'double', id: 7 },
              },
              group: true,
            },
            UserValue: {
              fields: {
                email: { rule: 'required', type: 'string', id: 9 },
                authDomain: { rule: 'required', type: 'string', id: 10 },
                nickname: { type: 'string', id: 11 },
                gaiaid: { rule: 'required', type: 'int64', id: 18 },
                obfuscatedGaiaid: { type: 'string', id: 19 },
                federatedIdentity: { type: 'string', id: 21 },
                federatedProvider: { type: 'string', id: 22 },
              },
              group: true,
            },
            ReferenceValue: {
              fields: {
                app: { rule: 'required', type: 'string', id: 13 },
                nameSpace: { type: 'string', id: 20 },
                pathElement: { rule: 'repeated', type: 'PathElement', id: 14 },
                databaseId: { type: 'string', id: 23 },
              },
              group: true,
              nested: {
                PathElement: {
                  fields: {
                    type: { rule: 'required', type: 'string', id: 15 },
                    id: { type: 'int64', id: 16 },
                    name: { type: 'string', id: 17 },
                  },
                  group: true,
                },
              },
            },
          },
        },
        Property: {
          fields: {
            meaning: { type: 'Meaning', id: 1, options: { default: 'NO_MEANING' } },
            meaningUri: { type: 'string', id: 2 },
            name: { rule: 'required', type: 'string', id: 3 },
            value: { rule: 'required', type: 'PropertyValue', id: 5 },
            multiple: { rule: 'required', type: 'bool', id: 4 },
            stashed: { type: 'int32', id: 6, options: { default: -1 } },
            computed: { type: 'bool', id: 7, options: { default: false } },
          },
          nested: {
            Meaning: {
              values: {
                NO_MEANING: 0,
                BLOB: 14,
                TEXT: 15,
                BYTESTRING: 16,
                ATOM_CATEGORY: 1,
                ATOM_LINK: 2,
                ATOM_TITLE: 3,
                ATOM_CONTENT: 4,
                ATOM_SUMMARY: 5,
                ATOM_AUTHOR: 6,
                GD_WHEN: 7,
                GD_EMAIL: 8,
                GEORSS_POINT: 9,
                GD_IM: 10,
                GD_PHONENUMBER: 11,
                GD_POSTALADDRESS: 12,
                GD_RATING: 13,
                BLOBKEY: 17,
                ENTITY_PROTO: 19,
                EMPTY_LIST: 24,
                INDEX_VALUE: 18,
              },
            },
          },
        },
        Path: {
          fields: { element: { rule: 'repeated', type: 'Element', id: 1 } },
          nested: {
            Element: {
              fields: {
                type: { rule: 'required', type: 'string', id: 2 },
                id: { type: 'int64', id: 3 },
                name: { type: 'string', id: 4 },
              },
              group: true,
            },
          },
        },
        Reference: {
          fields: {
            app: { rule: 'required', type: 'string', id: 13 },
            nameSpace: { type: 'string', id: 20 },
            path: { rule: 'required', type: 'Path', id: 14 },
            databaseId: { type: 'string', id: 23 },
          },
        },
        User: {
          fields: {
            email: { rule: 'required', type: 'string', id: 1 },
            authDomain: { rule: 'required', type: 'string', id: 2 },
            nickname: { type: 'string', id: 3 },
            gaiaid: { rule: 'required', type: 'int64', id: 4 },
            obfuscatedGaiaid: { type: 'string', id: 5 },
            federatedIdentity: { type: 'string', id: 6 },
            federatedProvider: { type: 'string', id: 7 },
          },
        },
        EntityProto: {
          fields: {
            key: { rule: 'required', type: 'Reference', id: 13 },
            entityGroup: { rule: 'required', type: 'Path', id: 16 },
            owner: { type: 'User', id: 17 },
            kind: { type: 'Kind', id: 4 },
            kindUri: { type: 'string', id: 5 },
            property: { rule: 'repeated', type: 'Property', id: 14, options: { packed: false } },
            rawProperty: { rule: 'repeated', type: 'Property', id: 15, options: { packed: false } },
          },
          nested: { Kind: { values: { GD_CONTACT: 1, GD_EVENT: 2, GD_MESSAGE: 3 } } },
        },
        EntityMetadata: {
          fields: { createdVersion: { type: 'int64', id: 1 }, updatedVersion: { type: 'int64', id: 2 } },
        },
        EntitySummary: {
          fields: {
            largeRawProperty: { rule: 'repeated', type: 'PropertySummary', id: 1, options: { packed: false } },
          },
          nested: {
            PropertySummary: {
              fields: {
                name: { rule: 'required', type: 'string', id: 1 },
                propertyTypeForStats: { type: 'string', id: 2 },
                sizeBytes: { type: 'int32', id: 3 },
              },
            },
          },
        },
        CompositeProperty: {
          fields: {
            indexId: { rule: 'required', type: 'int64', id: 1 },
            value: { rule: 'repeated', type: 'bytes', id: 2 },
          },
        },
        Index: {
          fields: {
            entityType: { rule: 'required', type: 'string', id: 1 },
            ancestor: { rule: 'required', type: 'bool', id: 5 },
            parent: { type: 'bool', id: 7 },
            version: { type: 'Version', id: 8, options: { default: 'VERSION_UNSPECIFIED' } },
            property: { rule: 'repeated', type: 'Property', id: 2 },
          },
          nested: {
            Version: { values: { VERSION_UNSPECIFIED: 0, V1: 1, V2: 2, V3: 3 } },
            Property: {
              fields: {
                name: { rule: 'required', type: 'string', id: 3 },
                direction: { type: 'Direction', id: 4, options: { default: 'DIRECTION_UNSPECIFIED' } },
                mode: { type: 'Mode', id: 6, options: { default: 'MODE_UNSPECIFIED' } },
              },
              group: true,
              nested: {
                Direction: { values: { DIRECTION_UNSPECIFIED: 0, ASCENDING: 1, DESCENDING: 2 } },
                Mode: { values: { MODE_UNSPECIFIED: 0, GEOSPATIAL: 3, ARRAY_CONTAINS: 4 } },
              },
            },
          },
        },
        CompositeIndex: {
          fields: {
            appId: { rule: 'required', type: 'string', id: 1 },
            databaseId: { type: 'string', id: 12 },
            id: { rule: 'required', type: 'int64', id: 2 },
            definition: { rule: 'required', type: 'Index', id: 3 },
            state: { rule: 'required', type: 'State', id: 4 },
            workflowState: { type: 'WorkflowState', id: 10, options: { deprecated: true } },
            errorMessage: { type: 'string', id: 11, options: { deprecated: true } },
            onlyUseIfRequired: { type: 'bool', id: 6, options: { default: false, deprecated: true } },
            disabledIndex: { type: 'bool', id: 9, options: { default: false, deprecated: true } },
            deprecatedReadDivisionFamily: { rule: 'repeated', type: 'string', id: 7 },
            deprecatedWriteDivisionFamily: { type: 'string', id: 8 },
          },
          nested: {
            State: { values: { WRITE_ONLY: 1, READ_WRITE: 2, DELETED: 3, ERROR: 4 } },
            WorkflowState: { values: { PENDING: 1, ACTIVE: 2, COMPLETED: 3 } },
          },
        },
        SearchIndexEntry: {
          fields: {
            indexId: { rule: 'required', type: 'int64', id: 1 },
            writeDivisionFamily: { rule: 'required', type: 'string', id: 2 },
            fingerprint_1999: { type: 'fixed64', id: 3 },
            fingerprint_2011: { type: 'fixed64', id: 4 },
          },
        },
        IndexPostfix: {
          fields: {
            indexValue: { rule: 'repeated', type: 'IndexValue', id: 1, options: { packed: false } },
            key: { type: 'Reference', id: 2 },
            before: { type: 'bool', id: 3, options: { default: true } },
            beforeAscending: { type: 'bool', id: 4 },
          },
          nested: {
            IndexValue: {
              fields: {
                propertyName: { rule: 'required', type: 'string', id: 1 },
                value: { rule: 'required', type: 'PropertyValue', id: 2 },
              },
            },
          },
        },
        IndexPosition: {
          fields: {
            key: { type: 'bytes', id: 1 },
            before: { type: 'bool', id: 2, options: { default: true } },
            beforeAscending: { type: 'bool', id: 3 },
          },
        },
      },
    },
  },
};
