import 'dart:convert';
import 'dart:typed_data';

import '../exception/mqtt_exception.dart';
import 'mqtt_reader.dart';
import 'mqtt_writer.dart';

/// MQTT UTF-8 Encoded String (specification section 1.5.4).
///
/// An MQTT UTF-8 string is a two byte length prefix followed by the UTF-8
/// encoded bytes. The character data MUST be well-formed UTF-8 and MUST NOT
/// contain U+0000.
abstract final class MqttUtf8 {
  static const int _maxLength = 0xFFFF;

  /// Writes [value] as an MQTT UTF-8 string into [writer].
  static void encodeTo(MqttWriter writer, String value) {
    final bytes = utf8.encode(value);
    if (bytes.length > _maxLength) {
      throw ArgumentError.value(
        value,
        'value',
        'MQTT UTF-8 string exceeds $_maxLength bytes',
      );
    }
    if (bytes.contains(0)) {
      throw ArgumentError.value(
        value,
        'value',
        'MQTT UTF-8 string MUST NOT contain U+0000',
      );
    }
    writer.writeUint16(bytes.length);
    writer.writeBytes(bytes);
  }

  /// Reads an MQTT UTF-8 string from [reader].
  static String decode(MqttReader reader) {
    final length = reader.readUint16();
    final bytes = reader.readBytes(length);
    return decodeBytes(bytes);
  }

  /// Decodes an MQTT UTF-8 string from raw bytes, validating well-formedness.
  static String decodeBytes(Uint8List bytes) {
    String value;
    try {
      value = utf8.decode(bytes, allowMalformed: false);
    } on FormatException catch (e) {
      throw MqttMalformedPacketException('Malformed UTF-8 string', e);
    }
    if (value.contains('\u0000')) {
      throw MqttMalformedPacketException(
        'MQTT UTF-8 string MUST NOT contain U+0000',
      );
    }
    return value;
  }
}
