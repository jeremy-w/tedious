const dataTypeByName = require('../../../src/data-type').typeByName;
const WritableTrackingBuffer = require('../../../src/tracking-buffer/writable-tracking-buffer');
const StreamParser = require('../../../src/token/stream-parser');
const assert = require('chai').assert;
const { _parseCollation } = require('../../../src/metadata-parser');

describe('Colmetadata Token Parser', () => {
  describe('parsing the column metadata for a result with many columns', function() {
    it('should parse them correctly', async function() {
      const userType = 2;
      const flags = 3;
      const columnName = 'name';

      const buffer = new WritableTrackingBuffer(50, 'ucs2');

      buffer.writeUInt8(0x81);
      // Column Count
      buffer.writeUInt16LE(1024);

      for (let i = 0; i < 1024; i++) {
        buffer.writeUInt32LE(userType);
        buffer.writeUInt16LE(flags);
        buffer.writeUInt8(dataTypeByName.Int.id);
        buffer.writeBVarchar(columnName);
      }

      const parser = StreamParser.parseTokens([buffer.data], {}, {});

      const result = await parser.next();
      assert.isFalse(result.done);
      const token = result.value;

      assert.isOk(!token.error);

      assert.strictEqual(token.columns.length, 1024);

      for (let i = 0; i < 1024; i++) {
        assert.strictEqual(token.columns[i].userType, 2);
        assert.strictEqual(token.columns[i].flags, 3);
        assert.strictEqual(token.columns[i].type.name, 'Int');
        assert.strictEqual(token.columns[i].colName, 'name');
      }

      assert.isTrue((await parser.next()).done);
    });
  });

  it('should int', async () => {
    const numberOfColumns = 1;
    const userType = 2;
    const flags = 3;
    const columnName = 'name';

    const buffer = new WritableTrackingBuffer(50, 'ucs2');

    buffer.writeUInt8(0x81);
    buffer.writeUInt16LE(numberOfColumns);
    buffer.writeUInt32LE(userType);
    buffer.writeUInt16LE(flags);
    buffer.writeUInt8(dataTypeByName.Int.id);
    buffer.writeBVarchar(columnName);
    // console.log(buffer.data)

    const parser = StreamParser.parseTokens([buffer.data], {}, {});

    const result = await parser.next();
    assert.isFalse(result.done);
    const token = result.value;

    assert.isOk(!token.error);
    assert.strictEqual(token.columns.length, 1);
    assert.strictEqual(token.columns[0].userType, 2);
    assert.strictEqual(token.columns[0].flags, 3);
    assert.strictEqual(token.columns[0].type.name, 'Int');
    assert.strictEqual(token.columns[0].colName, 'name');

    assert.isTrue((await parser.next()).done);
  });

  it('should varchar', async () => {
    const numberOfColumns = 1;
    const userType = 2;
    const flags = 3;
    const length = 3;
    const collation = Buffer.from([0x09, 0x04, 0x50, 0x78, 0x9a]);
    const columnName = 'name';

    const buffer = new WritableTrackingBuffer(50, 'ucs2');

    buffer.writeUInt8(0x81);
    buffer.writeUInt16LE(numberOfColumns);
    buffer.writeUInt32LE(userType);
    buffer.writeUInt16LE(flags);
    buffer.writeUInt8(dataTypeByName.VarChar.id);
    buffer.writeUInt16LE(length);
    buffer.writeBuffer(collation);
    buffer.writeBVarchar(columnName);
    // console.log(buffer)


    const parser = StreamParser.parseTokens([buffer.data], {}, {});
    const result = await parser.next();
    assert.isFalse(result.done);
    const token = result.value;
    assert.isOk(!token.error);
    assert.strictEqual(token.columns.length, 1);
    assert.strictEqual(token.columns[0].userType, 2);
    assert.strictEqual(token.columns[0].flags, 3);
    assert.strictEqual(token.columns[0].type.name, 'VarChar');
    assert.strictEqual(token.columns[0].collation.lcid, 0x0409, 'collation.lcid');
    assert.strictEqual(token.columns[0].collation.codepage, 'CP1257', 'collation.codepage');  // per sortId=0x9A=154, since UTF8 flag is not set.
    /*
    0x58 = 0b0101_1111
    ColFlags =
    0101: [fIgnoreCase] fIgnoreAccent [fIgnoreKana] fIgnoreWidth (read RTL - LSB=>MSB)
    1000: [fBinary] fBinary2 fUTF8 FRESERVEDBIT (read RTL - LSB=>MSB)
    */
    // assert.strictEqual(token.columns[0].collation.flags, 0x58, 'collation.flags');
    assert.deepStrictEqual(token.columns[0].collation._flags, {
      // 0x50 = 0b0101
      fIgnoreCase: true,
      fIgnoreAccent: false,
      fIgnoreKana: true,
      fIgnoreWidth: false,
      // 0x08 = 0b1000
      fBinary: false,
      fBinary2: false,
      fUTF8: false,
      fReservedBit: true
    }, 'collation._flags');
    assert.strictEqual(token.columns[0].collation.version, 0x7, 'collation.version');
    assert.strictEqual(token.columns[0].collation.sortId, 0x9a, 'collation.sortId');
    assert.strictEqual(token.columns[0].colName, 'name');
    assert.strictEqual(token.columns[0].dataLength, length);
  });
});

// The collation payloads were captured using Wireshark while snooping on a sqlcmd session.
// By creating a table with columns using varying collations, then requiring a select * from that (empty, even) table,
// you get COLMETADATA with embedded COLLATION values as specified.
describe('_parseCollation', () => {
  const table = [
    ['Latin1_General_100_CS_AI_SC_UTF8', '09 04 e0 24 00', {
      fIgnoreCase: false,
      fIgnoreAccent: true,
      fIgnoreWidth: true,
      fIgnoreKana: true,
      fBinary: false,
      fBinary2: false,
      fUTF8: true,
      fReservedBit: false,
    }],
    ['Latin1_General_100_BIN2_UTF8', '09 04 00 26 00', {
      fIgnoreCase: false,
      fIgnoreAccent: false,
      fIgnoreWidth: false,
      fIgnoreKana: false,
      fBinary: false,
      fBinary2: true,
      fUTF8: true,
      fReservedBit: false,
    }],
    ['SQL_Latin1_General_CP1_CI_AS', '09 04 d0 00 34', {
      fIgnoreCase: true,
      fIgnoreAccent: false,
      fIgnoreWidth: true,
      fIgnoreKana: true,
      fBinary: false,
      fBinary2: false,
      fUTF8: false,
      fReservedBit: false,
    }],
    ['Latin1_General_100_CS_AS_KS_WS', '09 04 00 20 00', {
      fIgnoreCase: false,
      fIgnoreAccent: false,
      fIgnoreWidth: false,
      fIgnoreKana: false,
      fBinary: false,
      fBinary2: false,
      fUTF8: false,
      fReservedBit: false,
    }],
    ['Latin1_General_100_CI_AI_KS_WS', '09 04 30 20 00', {
      fIgnoreCase: true,
      fIgnoreAccent: true,
      fIgnoreWidth: false,
      fIgnoreKana: false,
      fBinary: false,
      fBinary2: false,
      fUTF8: false,
      fReservedBit: false,
    }]
  ];
  for (const [name, hex, flags] of table) {
    it(`parses flags for ${name}`, () => {
      const buffer = Buffer.from(hex.split(' ').join(''), 'hex');
      const collation = _parseCollation(buffer);
      assert.deepStrictEqual(collation._flags, flags, 'collation._flags');
      if (flags.fUTF8) {
        assert.equal(collation.codepage, 'utf8', 'collation.codepage');
      }
    });
  }
});
