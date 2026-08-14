import '../codec/mqtt_reader.dart';
import '../codec/mqtt_writer.dart';
import 'mqtt_packet.dart';

/// PINGREQ packet (specification section 3.12).
final class MqttPingreqPacket extends MqttPacket {
  const MqttPingreqPacket();

  @override
  MqttPacketType get type => MqttPacketType.pingreq;

  @override
  void encodeBody(MqttWriter writer) {}

  static MqttPingreqPacket decode(MqttReader reader) {
    return const MqttPingreqPacket();
  }
}
