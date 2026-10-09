import 'dart:typed_data';

import '../exception/mqtt_exception.dart';
import 'mqtt_reader.dart';
import 'mqtt_writer.dart';

/// MQTT Variable Byte Integer encoding (specification section 1.5.5).
///
/// Values range from 0 to 268,435,455 and are encoded in one to four bytes.
/// The low 7 bits of each byte carry data; the most significant bit is a
/// continuation flag.
abstract final class VariableByteInteger {
  static const int maxValue = 268435455;

  static const int _maxBytes = 4;

  /// Encodes [value] into a standalone byte array.
  static Uint8List encode(int value) {
    _checkRange(value);
    final bytes = <int>[];
    do {
      var b = value % 128;
      value ~/= 128;
      if (value > 0) {
        b |= 0x80;
      }
      bytes.add(b);
    } while (value > 0);
    return Uint8List.fromList(bytes);
  }

  /// Encodes [value] into [writer].
  static void encodeTo(MqttWriter writer, int value) {
    writer.writeBytes(encode(value));
  }

  /// Decodes a Variable Byte Integer from [reader].
  ///
  /// Throws [MqttMalformedPacketException] if the encoding exceeds four
  /// bytes. Throws [MqttIncompletePacketException] if the reader runs out of
  /// data before the encoding terminates.
  static int decode(MqttReader reader) {
    var value = 0;
    var multiplier = 1;
    for (var i = 0; i < _maxBytes; i++) {
      final b = reader.readByte();
      value += (b & 0x7F) * multiplier;
      if ((b & 0x80) == 0) {
        // MQTT-1.5.5: the encoding must use the fewest possible bytes, so a
        // continuation byte followed by a zero terminator (0x80 0x00) is
        // malformed even though it decodes to a representable value.
        if (i > 0 && b == 0) {
          throw MqttMalformedPacketException(
            'Variable Byte Integer is not minimally encoded',
          );
        }
        return value;
      }
      multiplier <<= 7;
    }
    throw MqttMalformedPacketException(
      'Variable Byte Integer exceeds $_maxBytes bytes',
    );
  }

  static void _checkRange(int value) {
    if (value < 0 || value > maxValue) {
      throw RangeError.range(value, 0, maxValue, 'value');
    }
  }
}
