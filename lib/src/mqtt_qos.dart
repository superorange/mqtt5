import 'exception/mqtt_exception.dart';

/// MQTT Quality of Service levels.
enum MqttQos {
  atMostOnce(0),
  atLeastOnce(1),
  exactlyOnce(2);

  const MqttQos(this.value);

  final int value;

  /// Maps a wire value to a QoS level.
  ///
  /// Throws [MqttMalformedPacketException] for any other value: this is
  /// reached from packet decoding, where an invalid QoS is a peer error that
  /// must be handled as a protocol error rather than crashing the client.
  static MqttQos fromValue(int value) {
    switch (value) {
      case 0:
        return MqttQos.atMostOnce;
      case 1:
        return MqttQos.atLeastOnce;
      case 2:
        return MqttQos.exactlyOnce;
      default:
        throw MqttMalformedPacketException('Invalid QoS value: $value');
    }
  }
}
