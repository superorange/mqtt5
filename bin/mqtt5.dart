import 'package:mqtt5/mqtt5.dart';

void main(List<String> arguments) {
  print('mqtt5: MQTT 5.0 protocol engine for Dart');
  print('Packets: ${MqttPacketType.values.length}');
  print('Properties: ${MqttPropertyIdentifier.values.length}');
}
