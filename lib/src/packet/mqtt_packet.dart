import '../codec/mqtt_writer.dart';
import '../exception/mqtt_exception.dart';

/// MQTT 5.0 Control Packet types (specification section 2.1.2).
enum MqttPacketType {
  connect(1, 0x0),
  connack(2, 0x0),
  publish(3, 0x0),
  puback(4, 0x0),
  pubrec(5, 0x0),
  pubrel(6, 0x2),
  pubcomp(7, 0x0),
  subscribe(8, 0x2),
  suback(9, 0x0),
  unsubscribe(10, 0x2),
  unsuback(11, 0x0),
  pingreq(12, 0x0),
  pingresp(13, 0x0),
  disconnect(14, 0x0),
  auth(15, 0x0);

  const MqttPacketType(this.value, this.fixedFlags);

  /// The packet type nibble value (bits 7-4 of the first byte).
  final int value;

  /// The fixed header flags for packet types whose flags are invariant.
  final int fixedFlags;

  static MqttPacketType? tryFromValue(int value) {
    for (final type in MqttPacketType.values) {
      if (type.value == value) {
        return type;
      }
    }
    return null;
  }
}

/// Base class of all MQTT 5.0 control packets.
abstract class MqttPacket {
  const MqttPacket();
  MqttPacketType get type;

  /// The four low bits of the fixed header first byte.
  int get fixedHeaderFlags => type.fixedFlags;

  /// Writes the variable header and payload (everything after the fixed
  /// header) into [writer].
  void encodeBody(MqttWriter writer);

  @override
  String toString() => type.name;
}

/// Marker for packets that carry a packet identifier.
abstract interface class MqttPacketWithIdentifier {
  int get packetIdentifier;
}

void _requirePacketIdentifier(int packetIdentifier) {
  if (packetIdentifier < 1 || packetIdentifier > 0xFFFF) {
    throw MqttMalformedPacketException(
      'Invalid packet identifier: $packetIdentifier',
    );
  }
}

void writePacketIdentifier(MqttWriter writer, int packetIdentifier) {
  _requirePacketIdentifier(packetIdentifier);
  writer.writeUint16(packetIdentifier);
}
