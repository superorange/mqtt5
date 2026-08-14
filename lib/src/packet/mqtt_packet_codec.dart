import 'dart:typed_data';

import '../codec/mqtt_reader.dart';
import '../codec/mqtt_writer.dart';
import '../codec/variable_byte_integer.dart';
import '../exception/mqtt_exception.dart';
import 'auth.dart';
import 'connack.dart';
import 'connect.dart';
import 'disconnect.dart';
import 'mqtt_packet.dart';
import 'pingreq.dart';
import 'pingresp.dart';
import 'puback.dart';
import 'pubcomp.dart';
import 'publish.dart';
import 'pubrec.dart';
import 'pubrel.dart';
import 'suback.dart';
import 'subscribe.dart';
import 'unsuback.dart';
import 'unsubscribe.dart';

/// Encodes and decodes complete MQTT 5.0 control packets, including the
/// fixed header.
abstract final class MqttPacketCodec {
  /// Encodes [packet] into its wire representation.
  static Uint8List encode(MqttPacket packet) {
    final body = MqttWriter();
    packet.encodeBody(body);
    final bodyBytes = body.toBytes();

    final out = MqttWriter();
    out.writeByte((packet.type.value << 4) | packet.fixedHeaderFlags);
    VariableByteInteger.encodeTo(out, bodyBytes.length);
    out.writeBytes(bodyBytes);
    return out.toBytes();
  }

  /// Decodes a complete packet from [packetBytes].
  static MqttPacket decode(Uint8List packetBytes) {
    final reader = MqttReader(packetBytes);
    final firstByte = reader.readByte();
    final typeValue = firstByte >> 4;
    final flags = firstByte & 0x0F;

    final type = MqttPacketType.tryFromValue(typeValue);
    if (type == null) {
      throw MqttMalformedPacketException(
        'Unknown packet type: $typeValue',
      );
    }

    final remainingLength = VariableByteInteger.decode(reader);
    final body = reader.readBytes(remainingLength);

    final bodyReader = MqttReader(body);
    final packet = decodeBody(type, flags, bodyReader);
    if (bodyReader.hasRemaining) {
      throw MqttMalformedPacketException(
        'Trailing bytes after ${type.name} packet body',
      );
    }
    if (reader.hasRemaining) {
      throw MqttMalformedPacketException(
        'Extra bytes after ${type.name} packet',
      );
    }
    return packet;
  }

  static MqttPacket decodeBody(
    MqttPacketType type,
    int flags,
    MqttReader reader,
  ) {
    if (type != MqttPacketType.publish && flags != type.fixedFlags) {
      throw MqttMalformedPacketException(
        'Invalid fixed header flags 0x${flags.toRadixString(16)} '
        'for ${type.name}',
      );
    }
    switch (type) {
      case MqttPacketType.connect:
        return MqttConnectPacket.decode(reader);
      case MqttPacketType.connack:
        return MqttConnackPacket.decode(reader);
      case MqttPacketType.publish:
        return MqttPublishPacket.decode(reader, flags);
      case MqttPacketType.puback:
        return MqttPubackPacket.decode(reader);
      case MqttPacketType.pubrec:
        return MqttPubrecPacket.decode(reader);
      case MqttPacketType.pubrel:
        return MqttPubrelPacket.decode(reader);
      case MqttPacketType.pubcomp:
        return MqttPubcompPacket.decode(reader);
      case MqttPacketType.subscribe:
        return MqttSubscribePacket.decode(reader);
      case MqttPacketType.suback:
        return MqttSubackPacket.decode(reader);
      case MqttPacketType.unsubscribe:
        return MqttUnsubscribePacket.decode(reader);
      case MqttPacketType.unsuback:
        return MqttUnsubackPacket.decode(reader);
      case MqttPacketType.pingreq:
        return MqttPingreqPacket.decode(reader);
      case MqttPacketType.pingresp:
        return MqttPingrespPacket.decode(reader);
      case MqttPacketType.disconnect:
        return MqttDisconnectPacket.decode(reader);
      case MqttPacketType.auth:
        return MqttAuthPacket.decode(reader);
    }
  }
}
