import 'dart:convert';
import 'dart:typed_data';

import 'package:mqtt5/src/codec/mqtt_reader.dart';
import 'package:mqtt5/src/codec/mqtt_writer.dart';
import 'package:mqtt5/src/codec/mqtt_utf8.dart';
import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:test/test.dart';

void main() {
  group('MqttUtf8.encodeTo', () {
    test('encodes ascii string with length prefix', () {
      final writer = MqttWriter();
      MqttUtf8.encodeTo(writer, 'hello');
      expect(writer.toBytes(), [0x00, 0x05, ...utf8.encode('hello')]);
    });

    test('encodes multi-byte characters', () {
      final writer = MqttWriter();
      MqttUtf8.encodeTo(writer, '设备');
      final bytes = utf8.encode('设备');
      expect(writer.toBytes(), [0x00, bytes.length, ...bytes]);
    });

    test('rejects U+0000 on encode', () {
      final writer = MqttWriter();
      expect(
        () => MqttUtf8.encodeTo(writer, 'a\u0000b'),
        throwsArgumentError,
      );
    });
  });

  group('MqttUtf8.decode', () {
    test('decodes ascii string', () {
      final reader = MqttReader(Uint8List.fromList([0x00, 0x05, ...'hello'.codeUnits]));
      expect(MqttUtf8.decode(reader), 'hello');
      expect(reader.remainingLength, 0);
    });

    test('decodes multi-byte characters', () {
      final bytes = utf8.encode('设备');
      final reader = MqttReader(
        Uint8List.fromList([0x00, bytes.length, ...bytes]),
      );
      expect(MqttUtf8.decode(reader), '设备');
    });

    test('rejects malformed UTF-8', () {
      // 0xFF is never valid UTF-8.
      final reader = MqttReader(Uint8List.fromList([0x00, 0x01, 0xFF]));
      expect(
        () => MqttUtf8.decode(reader),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('rejects overlong UTF-8 encoding', () {
      // 0xC0 0x80 is an overlong encoding of U+0000 and is invalid UTF-8.
      final reader = MqttReader(Uint8List.fromList([0x00, 0x02, 0xC0, 0x80]));
      expect(
        () => MqttUtf8.decode(reader),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('rejects truncated multi-byte sequence', () {
      // 0xE4 is the leading byte of a 3-byte sequence but has no
      // continuation bytes: the declared length is present but the UTF-8
      // sequence is incomplete.
      final reader = MqttReader(Uint8List.fromList([0x00, 0x01, 0xE4]));
      expect(
        () => MqttUtf8.decode(reader),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('rejects encoded U+0000', () {
      final reader = MqttReader(
        Uint8List.fromList([0x00, 0x03, 0x61, 0x00, 0x62]),
      );
      expect(
        () => MqttUtf8.decode(reader),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('rejects lone surrogate encoded as UTF-8', () {
      // 0xED 0xA0 0x80 encodes U+D800 (a surrogate), which is ill-formed.
      final reader = MqttReader(
        Uint8List.fromList([0x00, 0x03, 0xED, 0xA0, 0x80]),
      );
      expect(
        () => MqttUtf8.decode(reader),
        throwsA(isA<MqttMalformedPacketException>()),
      );
    });

    test('throws incomplete when length prefix is truncated', () {
      final reader = MqttReader(Uint8List.fromList([0x00]));
      expect(
        () => MqttUtf8.decode(reader),
        throwsA(isA<MqttIncompletePacketException>()),
      );
    });

    test('throws incomplete when body is truncated', () {
      final reader = MqttReader(Uint8List.fromList([0x00, 0x05, 0x68, 0x69]));
      expect(
        () => MqttUtf8.decode(reader),
        throwsA(isA<MqttIncompletePacketException>()),
      );
    });

    test('round trips a representative set of strings', () {
      final samples = [
        '',
        'a',
        'hello world',
        '设备/状态',
        'emoji \u{1F600} works',
        'mixed ASCII and 中文',
      ];
      for (final sample in samples) {
        final writer = MqttWriter();
        MqttUtf8.encodeTo(writer, sample);
        final reader = MqttReader(writer.toBytes());
        expect(MqttUtf8.decode(reader), sample, reason: 'sample "$sample"');
      }
    });
  });
}
