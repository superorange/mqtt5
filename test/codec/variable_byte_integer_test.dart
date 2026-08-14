import 'dart:typed_data';

import 'package:mqtt5/src/codec/mqtt_reader.dart';
import 'package:mqtt5/src/codec/mqtt_writer.dart';
import 'package:mqtt5/src/codec/variable_byte_integer.dart';
import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:test/test.dart';

void main() {
  group('VariableByteInteger.encode', () {
    test('encodes boundary values', () {
      expect(VariableByteInteger.encode(0), [0x00]);
      expect(VariableByteInteger.encode(127), [0x7F]);
      expect(VariableByteInteger.encode(128), [0x80, 0x01]);
      expect(VariableByteInteger.encode(16383), [0xFF, 0x7F]);
      expect(VariableByteInteger.encode(16384), [0x80, 0x80, 0x01]);
      expect(VariableByteInteger.encode(2097151), [0xFF, 0xFF, 0x7F]);
      expect(VariableByteInteger.encode(2097152), [0x80, 0x80, 0x80, 0x01]);
      expect(
        VariableByteInteger.encode(268435455),
        [0xFF, 0xFF, 0xFF, 0x7F],
      );
    });

    test('rejects out of range values', () {
      expect(() => VariableByteInteger.encode(-1), throwsRangeError);
      expect(
        () => VariableByteInteger.encode(268435456),
        throwsRangeError,
      );
    });

    test('reports encoded length', () {
      expect(VariableByteInteger.encodedLength(0), 1);
      expect(VariableByteInteger.encodedLength(127), 1);
      expect(VariableByteInteger.encodedLength(128), 2);
      expect(VariableByteInteger.encodedLength(16383), 2);
      expect(VariableByteInteger.encodedLength(16384), 3);
      expect(VariableByteInteger.encodedLength(2097151), 3);
      expect(VariableByteInteger.encodedLength(2097152), 4);
      expect(VariableByteInteger.encodedLength(268435455), 4);
    });
  });

  group('VariableByteInteger.decode', () {
    test('decodes boundary values', () {
      expect(_decode([0x00]), 0);
      expect(_decode([0x7F]), 127);
      expect(_decode([0x80, 0x01]), 128);
      expect(_decode([0xFF, 0x7F]), 16383);
      expect(_decode([0x80, 0x80, 0x01]), 16384);
      expect(_decode([0xFF, 0xFF, 0x7F]), 2097151);
      expect(_decode([0x80, 0x80, 0x80, 0x01]), 2097152);
      expect(_decode([0xFF, 0xFF, 0xFF, 0x7F]), 268435455);
    });

    test('round trips all interesting values', () {
      final values = [
        0,
        1,
        127,
        128,
        255,
        256,
        16383,
        16384,
        65535,
        2097151,
        2097152,
        268435454,
        268435455,
      ];
      for (final value in values) {
        expect(_decode(VariableByteInteger.encode(value)), value,
            reason: 'value $value');
      }
    });

    test('rejects encoding longer than 4 bytes', () {
      expect(
        () => _decode([0x80, 0x80, 0x80, 0x80, 0x01]),
        throwsA(isA<MqttMalformedPacketException>()),
      );
      expect(
        () => _decode([0xFF, 0xFF, 0xFF, 0xFF, 0x01]),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('rejects truncated encodings as incomplete', () {
      expect(
        () => _decode([]),
        throwsA(isA<MqttIncompletePacketException>()),
      );
      expect(
        () => _decode([0x80]),
        throwsA(isA<MqttIncompletePacketException>()),
      );
      expect(
        () => _decode([0x80, 0x80]),
        throwsA(isA<MqttIncompletePacketException>()),
      );
      expect(
        () => _decode([0x80, 0x80, 0x80]),
        throwsA(isA<MqttIncompletePacketException>()),
      );
    });

    test('leaves reader positioned after the value', () {
      final reader = MqttReader(Uint8List.fromList([0x96, 0x01, 0xFF]));
      expect(VariableByteInteger.decode(reader), 150);
      expect(reader.offset, 2);
      expect(reader.readByte(), 0xFF);
    });
  });

  group('VariableByteInteger writer integration', () {
    test('encodeTo writes to writer', () {
      final writer = MqttWriter();
      VariableByteInteger.encodeTo(writer, 300);
      expect(writer.toBytes(), [0xAC, 0x02]);
    });
  });
}

int _decode(List<int> bytes) {
  return VariableByteInteger.decode(MqttReader(Uint8List.fromList(bytes)));
}
