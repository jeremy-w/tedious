import { codepageBySortId, codepageByLcid } from './collation';
import Parser from './token/stream-parser';
import { InternalConnectionOptions } from './connection';
import { TYPE, DataType } from './data-type';
import { CryptoMetadata } from './always-encrypted/types';

import { sprintf } from 'sprintf-js';

interface Collation {
  lcid: number;
  flags: number;
  _flags: CollationFlags;
  version: number;
  sortId: number;
  codepage: string;
}

interface XmlSchema {
  dbname: string;
  owningSchema: string;
  xmlSchemaCollection: string;
}

interface UdtInfo {
  maxByteSize: number;
  dbname: string;
  owningSchema: string;
  typeName: string;
  assemblyName: string;
}

export type BaseMetadata = {
  userType: number;

  flags: number;
  /**
   * The column's type, such as VarChar, Int or Binary.
   */
  type: DataType;

  collation: Collation | undefined;
  /**
   * The precision. Only applicable to numeric and decimal.
   */
  precision: number | undefined;

  /**
   * The scale. Only applicable to numeric, decimal, time, datetime2 and datetimeoffset.
   */
  scale: number | undefined;

  /**
   * The length, for char, varchar, nvarchar and varbinary.
   */
  dataLength: number | undefined;

  schema: XmlSchema | undefined;

  udtInfo: UdtInfo | undefined;
}

export type Metadata = {
  cryptoMetadata?: CryptoMetadata;
} & BaseMetadata;

interface CollationFlags {
  fIgnoreCase: boolean;
  fIgnoreAccent: boolean;
  fIgnoreWidth: boolean;
  fIgnoreKana: boolean;
  fBinary: boolean;
  fBinary2: boolean;
  fUTF8: boolean;
  fReservedBit: boolean;
}

/** @private
 *
 * Parses a Collation, including its _flags, from a 5-byte Buffer.
 *
 * The buffer looks like:
 *
 *     LL LL FL VF SS
 *
 * Legend per nybble:
 *
 * - L: lcid
 * - F: flags
 * - V: version
 * - S: sortId - 0 indicates to base it on the lcid
 */
function _parseCollation(collationData: Buffer): Collation {
  // LCID is 20 bits: lo byte at 0, hi byte at 1, and sortId nybble in 2.
  let lcid = (collationData[2] & 0x0F) << 16;
  lcid |= collationData[1] << 8;
  lcid |= collationData[0];

  // Flags are split between bytes at index 2 and 3.
  // Which we take as the hi and which the lo is arbitrary, so long as we mask off the correct bits when reading the flags. So we choose the byte order that requires no bitshifting.
  // The flags are in LSB order, and we have put the "ignore" flags in 0xF0, and the binary/utf8 flags in 0x0F.
  const flags = collationData[2] & 0xF0 | collationData[3] & 0x0F;
  const _flags: CollationFlags = {
    fIgnoreCase: (flags & 0x10) === 0x10,
    fIgnoreAccent: (flags & 0x20) === 0x20,
    fIgnoreKana: (flags & 0x40) === 0x40,
    fIgnoreWidth: (flags & 0x80) === 0x80,
    fBinary: (flags & 0x01) === 0x01,
    fBinary2: (flags & 0x02) === 0x02,
    fUTF8: (flags & 0x04) === 0x04,
    fReservedBit: (flags & 0x08) === 0x08,
  };

  const version = (collationData[3] & 0xF0) >> 4 & 0x0F;

  const sortId = collationData[4];

  // UTF-8 flag wins over all. Failing that, sortID of 0 says to look at LCID.
  let codepage: string | undefined;
  const fallbackCodepage = 'CP1252';
  if (_flags.fUTF8) {
    codepage = 'utf8';
  } else if (sortId === 0x00) {
    codepage = codepageByLcid[lcid];
  } else {
    codepage = codepageBySortId[sortId];
  }

  const collation = { lcid, flags, _flags, version, sortId, codepage: codepage ?? fallbackCodepage };
  return collation;
}

function readCollation(parser: Parser, callback: (collation: Collation | undefined) => void) {
  // s2.2.5.1.2
  parser.readBuffer(5, (collationData) => {
    const collation = _parseCollation(collationData);
    callback(collation);
  });
}

function readSchema(parser: Parser, callback: (schema: XmlSchema | undefined) => void) {
  // s2.2.5.5.3
  parser.readUInt8((schemaPresent) => {
    if (schemaPresent === 0x01) {
      parser.readBVarChar((dbname) => {
        parser.readBVarChar((owningSchema) => {
          parser.readUsVarChar((xmlSchemaCollection) => {
            callback({
              dbname: dbname,
              owningSchema: owningSchema,
              xmlSchemaCollection: xmlSchemaCollection
            });
          });
        });
      });
    } else {
      callback(undefined);
    }
  });
}

function readUDTInfo(parser: Parser, callback: (udtInfo: UdtInfo | undefined) => void) {
  parser.readUInt16LE((maxByteSize) => {
    parser.readBVarChar((dbname) => {
      parser.readBVarChar((owningSchema) => {
        parser.readBVarChar((typeName) => {
          parser.readUsVarChar((assemblyName) => {
            callback({
              maxByteSize: maxByteSize,
              dbname: dbname,
              owningSchema: owningSchema,
              typeName: typeName,
              assemblyName: assemblyName
            });
          });
        });
      });
    });
  });
}

function metadataParse(parser: Parser, options: InternalConnectionOptions, callback: (metadata: Metadata) => void) {
  (options.tdsVersion < '7_2' ? parser.readUInt16LE : parser.readUInt32LE).call(parser, (userType) => {
    parser.readUInt16LE((flags) => {
      parser.readUInt8((typeNumber) => {
        const type: DataType = TYPE[typeNumber];

        if (!type) {
          throw new Error(sprintf('Unrecognised data type 0x%02X', typeNumber));
        }

        switch (type.name) {
          case 'Null':
          case 'TinyInt':
          case 'SmallInt':
          case 'Int':
          case 'BigInt':
          case 'Real':
          case 'Float':
          case 'SmallMoney':
          case 'Money':
          case 'Bit':
          case 'SmallDateTime':
          case 'DateTime':
          case 'Date':
            return callback({
              userType: userType,
              flags: flags,
              type: type,
              collation: undefined,
              precision: undefined,
              scale: undefined,
              dataLength: undefined,
              schema: undefined,
              udtInfo: undefined
            });

          case 'IntN':
          case 'FloatN':
          case 'MoneyN':
          case 'BitN':
          case 'UniqueIdentifier':
          case 'DateTimeN':
            return parser.readUInt8((dataLength) => {
              callback({
                userType: userType,
                flags: flags,
                type: type,
                collation: undefined,
                precision: undefined,
                scale: undefined,
                dataLength: dataLength,
                schema: undefined,
                udtInfo: undefined
              });
            });

          case 'Variant':
            return parser.readUInt32LE((dataLength) => {
              callback({
                userType: userType,
                flags: flags,
                type: type,
                collation: undefined,
                precision: undefined,
                scale: undefined,
                dataLength: dataLength,
                schema: undefined,
                udtInfo: undefined
              });
            });

          case 'VarChar':
          case 'Char':
          case 'NVarChar':
          case 'NChar':
            return parser.readUInt16LE((dataLength) => {
              readCollation(parser, (collation) => {
                callback({
                  userType: userType,
                  flags: flags,
                  type: type,
                  collation: collation,
                  precision: undefined,
                  scale: undefined,
                  dataLength: dataLength,
                  schema: undefined,
                  udtInfo: undefined
                });
              });
            });

          case 'Text':
          case 'NText':
            return parser.readUInt32LE((dataLength) => {
              readCollation(parser, (collation) => {
                callback({
                  userType: userType,
                  flags: flags,
                  type: type,
                  collation: collation,
                  precision: undefined,
                  scale: undefined,
                  dataLength: dataLength,
                  schema: undefined,
                  udtInfo: undefined
                });
              });
            });

          case 'VarBinary':
          case 'Binary':
            return parser.readUInt16LE((dataLength) => {
              callback({
                userType: userType,
                flags: flags,
                type: type,
                collation: undefined,
                precision: undefined,
                scale: undefined,
                dataLength: dataLength,
                schema: undefined,
                udtInfo: undefined
              });
            });

          case 'Image':
            return parser.readUInt32LE((dataLength) => {
              callback({
                userType: userType,
                flags: flags,
                type: type,
                collation: undefined,
                precision: undefined,
                scale: undefined,
                dataLength: dataLength,
                schema: undefined,
                udtInfo: undefined
              });
            });

          case 'Xml':
            return readSchema(parser, (schema) => {
              callback({
                userType: userType,
                flags: flags,
                type: type,
                collation: undefined,
                precision: undefined,
                scale: undefined,
                dataLength: undefined,
                schema: schema,
                udtInfo: undefined
              });
            });

          case 'Time':
          case 'DateTime2':
          case 'DateTimeOffset':
            return parser.readUInt8((scale) => {
              callback({
                userType: userType,
                flags: flags,
                type: type,
                collation: undefined,
                precision: undefined,
                scale: scale,
                dataLength: undefined,
                schema: undefined,
                udtInfo: undefined
              });
            });

          case 'NumericN':
          case 'DecimalN':
            return parser.readUInt8((dataLength) => {
              parser.readUInt8((precision) => {
                parser.readUInt8((scale) => {
                  callback({
                    userType: userType,
                    flags: flags,
                    type: type,
                    collation: undefined,
                    precision: precision,
                    scale: scale,
                    dataLength: dataLength,
                    schema: undefined,
                    udtInfo: undefined
                  });
                });
              });
            });

          case 'UDT':
            return readUDTInfo(parser, (udtInfo) => {
              callback({
                userType: userType,
                flags: flags,
                type: type,
                collation: undefined,
                precision: undefined,
                scale: undefined,
                dataLength: undefined,
                schema: undefined,
                udtInfo: udtInfo
              });
            });

          default:
            throw new Error(sprintf('Unrecognised type %s', type.name));
        }
      });
    });
  });
}

export default metadataParse;
export { readCollation, _parseCollation };

module.exports = metadataParse;
module.exports.readCollation = readCollation;
module.exports._parseCollation = _parseCollation;
