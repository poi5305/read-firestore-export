import protobuf from 'protobufjs';
import { FirestoreBackupDocumentProtoJSON } from './firestoreBackupProto';

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
