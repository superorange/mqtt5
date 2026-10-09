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

  /// Whether [value] holds a UTF-16 surrogate that is not part of a pair.
  ///
  /// Such a string has no UTF-8 encoding: `utf8.encode` would silently
  /// substitute U+FFFD, changing (for example) the topic a message goes to.
  /// MQTT-1.5.4-1 forbids encoding surrogates, so callers reject it instead.
  static bool hasLoneSurrogate(String value) {
    for (var i = 0; i < value.length; i++) {
      final unit = value.codeUnitAt(i);
      if (unit >= 0xD800 && unit <= 0xDBFF) {
        if (i + 1 < value.length) {
          final next = value.codeUnitAt(i + 1);
          if (next >= 0xDC00 && next <= 0xDFFF) {
            i++;
            continue;
          }
        }
        return true;
      }
      if (unit >= 0xDC00 && unit <= 0xDFFF) {
        return true;
      }
    }
    return false;
  }

  /// Writes [value] as an MQTT UTF-8 string into [writer].
  static void encodeTo(MqttWriter writer, String value) {
    if (hasLoneSurrogate(value)) {
      throw ArgumentError.value(
        value,
        'value',
        'MQTT UTF-8 string MUST NOT contain unpaired surrogates',
      );
    }
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
    // MQTT-1.5.4-3: 0xEF 0xBB 0xBF is U+FEFF and must not be stripped, but
    // dart:convert drops a leading byte order mark. Put it back.
    if (bytes.length >= 3 &&
        bytes[0] == 0xEF &&
        bytes[1] == 0xBB &&
        bytes[2] == 0xBF) {
      value = '\uFEFF$value';
    }
    if (value.contains('\u0000')) {
      throw MqttMalformedPacketException(
        'MQTT UTF-8 string MUST NOT contain U+0000',
      );
    }
    return value;
  }
}
