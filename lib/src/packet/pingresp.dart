import '../codec/mqtt_reader.dart';
import '../codec/mqtt_writer.dart';
import 'mqtt_packet.dart';

/// PINGRESP packet (specification section 3.13).
final class MqttPingrespPacket extends MqttPacket {
  const MqttPingrespPacket();

  @override
  MqttPacketType get type => MqttPacketType.pingresp;

  @override
  void encodeBody(MqttWriter writer) {}

  static MqttPingrespPacket decode(MqttReader reader) {
    return const MqttPingrespPacket();
  }
}
