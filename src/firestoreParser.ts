import protobuf from 'protobufjs';
import { FirestoreBackupDocumentProtoJSON } from './firestoreBackupProto';
import { readTag } from './protobufRawReader';

interface TreeField {
  id: number;
  name: string;
  type: string;
  group: boolean;
  repeated: boolean;
  fields: (TreeField | null)[];
  nested: { [type: string]: TreeField };
}

function buildFieldTree(root: any, parent: any = undefined): TreeField {
  const tree: TreeField = {
    id: 0,
    name: '',
    type: '',
    group: false,
    repeated: false,
    fields: [],
    nested: {},
  };
  tree.group = root.group || false;
  // index nested
  if (root.nested !== undefined) {
    for (const typename in root.nested) {
      const type = root.nested[typename];
      tree.nested[typename] = buildFieldTree(type);
    }
  }
  // run again with parent
  if (root.nested !== undefined) {
    for (const typename in root.nested) {
      const type = root.nested[typename];
      tree.nested[typename] = buildFieldTree(type, tree);
    }
  }

  // build fields
  if (root.fields !== undefined) {
    for (const fieldname in root.fields) {
      const field = root.fields[fieldname];
      const id = field.id || 0;
      const type = field.type;
      const repeated = field.rule === 'repeated';
      const child: TreeField = {
        id,
        type,
        name: fieldname,
        group: false,
        repeated,
        fields: [],
        nested: {},
      };
      const childTreeField = tree.nested[type] || parent?.nested[type];
      if (childTreeField !== undefined) {
        child.id = id;
        child.type = type;
        child.name = fieldname;
        child.fields = childTreeField.fields;
      } else {
        // console.log('basicType', type);
      }
      tree.fields[id] = child;
    }
  }
  return tree;
}

// const r = buildFieldTree(FirestoreBackupDocumentProtoJSON.nested.storage_onestore_v3_bytes);
// console.log(JSON.stringify(r.nested['EntityProto'], null, 2));

function getVarint(buffer: Buffer) {
  console.log(buffer);
  let pos = 0;
  let i = 0;
  let value = buffer[pos];
  while (buffer[pos] & 0x80) {
    value |= (buffer[pos++] & 0x7f) << (i++ * 7);
  }
  console.log(value);
}

function longToNumber(v: protobuf.Long): number {
  return (v.high & 2097151) * 2 ** 32 + v.low;
}

// getVarint(Buffer.from([0xc5, 0x01]));

export class FirestoreParserFaster {
  public static EntityProtoIndex = buildFieldTree(FirestoreBackupDocumentProtoJSON.nested.storage_onestore_v3_bytes)
    .nested['EntityProto'];

  public static convertBufferToObject(data: Buffer): any {
    const bs = new protobuf.BufferReader(data);
    return this.convertBufferToObjectImpl(bs, this.EntityProtoIndex, bs.len);
  }

  public static entityProtoToJSON(proto: any) {
    const json: any = {};
    const _key = proto.key?.path?.element?.[0]?.name?.toString();
    if (_key !== undefined) {
      json._key = _key;
    }
    if (proto.property !== undefined) {
      for (const property of proto.property) {
        if (property.multiple === 1) {
          if (json[property.name] === undefined) {
            json[property.name] = [];
          }
          json[property.name].push(property.value);
        } else {
          json[property.name] = property.value;
        }
      }
    }
    if (proto.rawProperty !== undefined) {
      for (const property of proto.rawProperty) {
        if (property.multiple === 1) {
          if (json[property.name] === undefined) {
            json[property.name] = [];
          }
          json[property.name].push(property.value);
        } else {
          json[property.name] = property.value;
        }
      }
    }

    // console.log(json);
    return json;
  }

  public static convertBufferToObjectImpl(bs: protobuf.BufferReader, proto: TreeField, end: number): any {
    let object: any = {};
    while (bs.pos < end) {
      const [id, wiretype] = readTag(bs.buf[bs.pos]);
      bs.skip(1);
      if (bs.pos >= end) {
        return object;
      }
      const protoType = proto.fields[id]!;
      let fieldValue: any = undefined;

      if (wiretype === 0) {
        const int64 = bs.int64();
        fieldValue = longToNumber(int64);
        if (proto.type === 'PropertyValue') {
          return protoType.name === 'booleanValue' ? fieldValue === 1 : fieldValue;
        }
      } else if (wiretype === 1) {
        fieldValue = bs.double();
        if (proto.type === 'PropertyValue') {
          return fieldValue;
        }
      } else if (wiretype === 2) {
        let len = bs.int32();
        if (len === 0) {
          continue;
        }
        // TODO fix this, wired ???
        if (len === 1 && protoType.type === 'Path') {
          len = bs.int32();
        }
        if (protoType.type === 'string' || protoType.type === 'bytes') {
          fieldValue = bs.buf.subarray(bs.pos, bs.pos + len);
          bs.skip(len);
        } else {
          fieldValue = this.convertBufferToObjectImpl(bs, protoType, bs.pos + len);
        }
      } else if (wiretype === 3) {
        fieldValue = this.convertBufferToObjectImpl(bs, protoType, bs.len);
      } else if (wiretype === 4) {
        break;
      } else {
        throw new Error('unknown wiretype ' + wiretype);
      }
      if (protoType.repeated) {
        if (object[protoType.name] === undefined) {
          object[protoType.name] = [];
        }
        object[protoType.name].push(fieldValue);
      } else {
        object[protoType.name] = fieldValue;
      }
    } // while

    if (proto.type === 'Property') {
      object.name = object.name.toString();
      if (object.meaning === undefined) {
        if (object.value?.stringValue !== undefined) {
          object.value = object.value.stringValue.toString();
        }
      } else if (object.meaning === 24) {
        object.value = [];
      } else if (object.meaning === 19) {
        object.value = this.convertBufferToObject(object.value.stringValue);
      }
    } else if (proto.type === '') {
      // root
      return this.entityProtoToJSON(object);
    }
    return object;
  }
}

export class FirestoreParser {
  private static Root: protobuf.Root = protobuf.Root.fromJSON(FirestoreBackupDocumentProtoJSON);
  private static EntityProto: protobuf.Type = FirestoreParser.Root.lookupType('EntityProto');

  public static convertBufferToObject(data: Buffer): any {
    const message = FirestoreParser.EntityProto.decode(data) as any;
    return this.convertMessageToObject(message.toJSON());
  }

  public static convertMessageToObject(message: any): any {
    const object: any = {};
    this.parseKey(object, message.key);
    const properties: any[] = message.property || [];
    for (const property of properties) {
      this.parseProperty(object, property);
    }
    const raw_properties: any[] = message.rawProperty || [];
    for (const property of raw_properties) {
      this.parseProperty(object, property);
    }
    return object;
  }

  // repeated Property property = 14;
  // repeated Property raw_property = 15;
  public static parseProperty(object: any, property: any) {
    const meaning = property.meaning || 'NO_MEANING';
    const name = property.name || '';
    const value = property.value; // PropertyValue
    const multiple = property.multiple || false;
    if (multiple && object[name] === undefined) {
      object[name] = [];
    }
    let v: any = null;
    if (meaning === 'NO_MEANING') {
      if (value.booleanValue !== undefined) {
        v = value.booleanValue;
      } else if (value.int64Value !== undefined) {
        v = Number.parseInt(value.int64Value);
      } else if (value.doubleValue !== undefined) {
        v = Number.parseFloat(value.doubleValue);
      } else if (value.stringValue !== undefined) {
        v = Buffer.from(value.stringValue, 'base64').toString();
      } else {
        // console.log('Unknown', property, name, value);
      }
    } else if (meaning === 'ENTITY_PROTO') {
      // console.log('ENTITY_PROTO', value);
      const buffer = Buffer.from(value.stringValue, 'base64');
      const message = this.EntityProto.decode(buffer);
      const obj = this.convertMessageToObject(message.toJSON());
      v = obj;
    } else if (meaning === 'EMPTY_LIST') {
      v = [];
    } else {
      console.log(`Unhandled meaning ${meaning}`);
    }
    if (multiple) {
      object[name].push(v);
    } else {
      object[name] = v;
    }
  }

  // parse required Reference key = 13;
  public static parseKey(object: any, key: any) {
    const _key = key?.path?.element?.[0]?.name;
    if (_key !== undefined) {
      object._key = _key;
    }
  }
}
