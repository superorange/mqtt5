import 'dart:typed_data';

import '../codec/mqtt_reader.dart';
import '../codec/mqtt_utf8.dart';
import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';
import '../mqtt_qos.dart';
import '../property/mqtt_property.dart';
import '../property/property_codec.dart';
import 'mqtt_packet.dart';

const String _protocolName = 'MQTT';
const int _protocolLevel = 5;

/// A Last Will and Testament message published by the server on the client's
/// behalf when the connection ends abnormally.
final class MqttWill {
  const MqttWill({
    required this.topic,
    required this.payload,
    this.qos = MqttQos.atMostOnce,
    this.retain = false,
    this.properties = const [],
  });

  final String topic;
  final Uint8List payload;
  final MqttQos qos;
  final bool retain;
  final List<MqttProperty> properties;
}

/// CONNECT packet (specification section 3.1).
final class MqttConnectPacket extends MqttPacket {
  MqttConnectPacket({
    required this.clientId,
    this.cleanStart = true,
    this.keepAliveSeconds = 60,
    this.properties = const [],
    this.will,
    this.username,
    this.password,
  }) {
    if (keepAliveSeconds < 0 || keepAliveSeconds > 0xFFFF) {
      throw ArgumentError.value(
        keepAliveSeconds,
        'keepAliveSeconds',
        'Must be between 0 and 65535',
      );
    }
  }

  final String clientId;
  final bool cleanStart;
  final int keepAliveSeconds;
  final List<MqttProperty> properties;
  final MqttWill? will;
  final String? username;
  final Uint8List? password;

  @override
  MqttPacketType get type => MqttPacketType.connect;

  @override
  void encodeBody(MqttWriter writer) {
    MqttUtf8.encodeTo(writer, _protocolName);
    writer.writeByte(_protocolLevel);

    final will = this.will;
    var flags = 0;
    if (cleanStart) {
      flags |= 0x02;
    }
    if (will != null) {
      flags |= 0x04;
      flags |= (will.qos.value << 3) & 0x18;
      if (will.retain) {
        flags |= 0x20;
      }
    }
    if (username != null) {
      flags |= 0x80;
    }
    if (password != null) {
      flags |= 0x40;
    }
    writer.writeByte(flags);

    writer.writeUint16(keepAliveSeconds);

    PropertyCodec.encode(writer, properties, MqttPropertyContext.connect);

    MqttUtf8.encodeTo(writer, clientId);
    if (will != null) {
      PropertyCodec.encode(writer, will.properties, MqttPropertyContext.will);
      MqttUtf8.encodeTo(writer, will.topic);
      writer.writeUint16(will.payload.length);
      writer.writeBytes(will.payload);
    }
    final uname = username;
    if (uname != null) {
      MqttUtf8.encodeTo(writer, uname);
    }
    final pwd = password;
    if (pwd != null) {
      writer.writeUint16(pwd.length);
      writer.writeBytes(pwd);
    }
  }

  static MqttConnectPacket decode(MqttReader reader) {
    final protocolName = MqttUtf8.decode(reader);
    if (protocolName != _protocolName) {
      throw MqttMalformedPacketException(
        'Invalid protocol name: $protocolName',
      );
    }
    final protocolLevel = reader.readByte();
    if (protocolLevel != _protocolLevel) {
      throw MqttMalformedPacketException(
        'Unsupported protocol level: $protocolLevel',
      );
    }
    final flags = reader.readByte();
    if (flags & 0x01 != 0) {
      throw MqttMalformedPacketException(
        'CONNECT reserved flag must be 0',
      );
    }
    final cleanStart = flags & 0x02 != 0;
    final willFlag = flags & 0x04 != 0;
    final willQos = MqttQos.fromValue((flags >> 3) & 0x03);
    final willRetain = flags & 0x20 != 0;
    final passwordFlag = flags & 0x40 != 0;
    final usernameFlag = flags & 0x80 != 0;

    if (!willFlag && (willQos != MqttQos.atMostOnce || willRetain)) {
      throw MqttMalformedPacketException(
        'Will QoS and Will Retain must be 0 when Will Flag is 0',
      );
    }

    final keepAliveSeconds = reader.readUint16();
    final properties = PropertyCodec.decode(
      reader,
      MqttPropertyContext.connect,
    );
    final clientId = MqttUtf8.decode(reader);

    MqttWill? will;
    if (willFlag) {
      final willProperties = PropertyCodec.decode(
        reader,
        MqttPropertyContext.will,
      );
      final willTopic = MqttUtf8.decode(reader);
      final willPayloadLength = reader.readUint16();
      final willPayload = reader.readBytes(willPayloadLength);
      will = MqttWill(
        topic: willTopic,
        payload: willPayload,
        qos: willQos,
        retain: willRetain,
        properties: willProperties,
      );
    }

    String? username;
    if (usernameFlag) {
      username = MqttUtf8.decode(reader);
    }
    Uint8List? password;
    if (passwordFlag) {
      final passwordLength = reader.readUint16();
      password = reader.readBytes(passwordLength);
    }

    return MqttConnectPacket(
      clientId: clientId,
      cleanStart: cleanStart,
      keepAliveSeconds: keepAliveSeconds,
      properties: properties,
      will: will,
      username: username,
      password: password,
    );
  }
}
