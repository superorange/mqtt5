import 'dart:typed_data';

import 'package:mqtt5/src/codec/mqtt_reader.dart';
import 'package:mqtt5/src/codec/mqtt_writer.dart';
import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:test/test.dart';

void main() {
  group('MqttWriter', () {
    test('writes bytes', () {
      final w = MqttWriter();
      w.writeByte(0x01);
      w.writeByte(0x02);
      expect(w.toBytes(), [0x01, 0x02]);
    });

    test('writes uint16 big-endian', () {
      final w = MqttWriter();
      w.writeUint16(0x1234);
      expect(w.toBytes(), [0x12, 0x34]);
    });

    test('writes uint32 big-endian', () {
      final w = MqttWriter();
      w.writeUint32(0x12345678);
      expect(w.toBytes(), [0x12, 0x34, 0x56, 0x78]);
    });

    test('masks values above byte range', () {
      final w = MqttWriter();
      w.writeByte(0x1FF);
      expect(w.toBytes(), [0xFF]);
    });

    test('tracks length', () {
      final w = MqttWriter();
      expect(w.length, 0);
      w.writeUint16(0);
      expect(w.length, 2);
    });
  });

  group('MqttReader', () {
    test('reads bytes', () {
      final r = MqttReader(Uint8List.fromList([0x01, 0x02, 0x03]));
      expect(r.readByte(), 0x01);
      expect(r.readUint16(), 0x0203);
      expect(r.hasRemaining, isFalse);
    });

    test('reads uint32', () {
      final r = MqttReader(Uint8List.fromList([0xDE, 0xAD, 0xBE, 0xEF]));
      expect(r.readUint32(), 0xDEADBEEF);
    });

    test('readBytes returns a view and advances offset', () {
      final r = MqttReader(Uint8List.fromList([0x00, 0x01, 0x02, 0x03]));
      r.readByte();
      final view = r.readBytes(2);
      expect(view, [0x01, 0x02]);
      expect(r.offset, 3);
    });

    test('readRemaining consumes the rest', () {
      final r = MqttReader(Uint8List.fromList([0x00, 0x01, 0x02, 0x03]));
      r.readByte();
      expect(r.readRemaining(), [0x01, 0x02, 0x03]);
    });

    test('throws incomplete on over-read', () {
      final r = MqttReader(Uint8List.fromList([0x01]));
      expect(() => r.readUint16(), throwsA(isA<MqttIncompletePacketException>()));
      expect(() => r.readBytes(2), throwsA(isA<MqttIncompletePacketException>()));
    });

    test('throws malformed on negative length', () {
      final r = MqttReader(Uint8List.fromList([0x01]));
      expect(() => r.readBytes(-1), throwsA(isA<MqttMalformedPacketException>()));
    });
  });
}
