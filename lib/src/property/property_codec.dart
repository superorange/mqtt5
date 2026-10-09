import 'dart:typed_data';

import '../codec/mqtt_reader.dart';
import '../codec/mqtt_writer.dart';
import '../codec/mqtt_utf8.dart';
import '../codec/variable_byte_integer.dart';
import '../exception/mqtt_exception.dart';
import 'mqtt_property.dart';

/// Encodes and decodes the MQTT 5.0 property section, driven by the
/// declarative [MqttPropertyMeta] registry.
abstract final class PropertyCodec {
  /// Decodes the property section in [reader] for the given [context].
  static List<MqttProperty> decode(
    MqttReader reader,
    MqttPropertyContext context,
  ) {
    final propertyLength = VariableByteInteger.decode(reader);
    final end = reader.offset + propertyLength;
    if (end > reader.length) {
      throw MqttIncompletePacketException(
        'Property section length $propertyLength exceeds available bytes',
      );
    }
    final result = <MqttProperty>[];
    final seen = <int>{};
    while (reader.offset < end) {
      final identifier = VariableByteInteger.decode(reader);
      final meta = propertyMetaFor(identifier);
      if (meta == null) {
        throw MqttMalformedPacketException(
          'Unknown property identifier: $identifier',
        );
      }
      // Section 2.2.2.2: an identifier not valid for the packet type makes the
      // packet malformed.
      if (!meta.allowedPackets.contains(context)) {
        throw MqttMalformedPacketException(
          'Property ${meta.name} is not allowed in $context',
        );
      }
      if (!meta.repeatsIn(context) && !seen.add(identifier)) {
        throw MqttProtocolException('Duplicate property: ${meta.name}');
      }
      final value = _readValue(reader, meta.type);
      meta.validator?.call(value);
      result.add(meta.create(value));
    }
    if (reader.offset != end) {
      throw MqttMalformedPacketException('Property section length mismatch');
    }
    return result;
  }

  /// Encodes [properties] as a property section for [context].
  static void encode(
    MqttWriter writer,
    List<MqttProperty> properties,
    MqttPropertyContext context,
  ) {
    final body = MqttWriter();
    final seen = <int>{};
    for (final property in properties) {
      final meta = property.meta;
      if (!meta.allowedPackets.contains(context)) {
        throw MqttProtocolException(
          'Property ${meta.name} is not allowed in $context',
        );
      }
      if (!meta.repeatsIn(context) && !seen.add(property.identifier)) {
        throw MqttProtocolException('Duplicate property: ${meta.name}');
      }
      meta.validator?.call(property.wireValue);
      VariableByteInteger.encodeTo(body, property.identifier);
      _writeValue(body, property.wireValue, meta.type);
    }
    VariableByteInteger.encodeTo(writer, body.length);
    writer.writeBytes(body.toBytes());
  }

  static Object _readValue(MqttReader reader, MqttPropertyType type) {
    switch (type) {
      case MqttPropertyType.byte:
        return reader.readByte();
      case MqttPropertyType.twoByteInteger:
        return reader.readUint16();
      case MqttPropertyType.fourByteInteger:
        return reader.readUint32();
      case MqttPropertyType.utf8String:
        return MqttUtf8.decode(reader);
      case MqttPropertyType.utf8StringPair:
        final name = MqttUtf8.decode(reader);
        final value = MqttUtf8.decode(reader);
        return (name, value);
      case MqttPropertyType.binaryData:
        final length = reader.readUint16();
        return reader.readBytes(length);
      case MqttPropertyType.variableByteInteger:
        return VariableByteInteger.decode(reader);
    }
  }

  static void _writeValue(
      MqttWriter writer, Object value, MqttPropertyType type) {
    switch (type) {
      case MqttPropertyType.byte:
        writer.writeByte(value as int);
      case MqttPropertyType.twoByteInteger:
        writer.writeUint16(value as int);
      case MqttPropertyType.fourByteInteger:
        writer.writeUint32(value as int);
      case MqttPropertyType.utf8String:
        MqttUtf8.encodeTo(writer, value as String);
      case MqttPropertyType.utf8StringPair:
        final pair = value as (String, String);
        MqttUtf8.encodeTo(writer, pair.$1);
        MqttUtf8.encodeTo(writer, pair.$2);
      case MqttPropertyType.binaryData:
        final bytes = value as Uint8List;
        writer.writeUint16(bytes.length);
        writer.writeBytes(bytes);
      case MqttPropertyType.variableByteInteger:
        VariableByteInteger.encodeTo(writer, value as int);
    }
  }
}
