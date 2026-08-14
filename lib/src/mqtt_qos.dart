/// MQTT Quality of Service levels.
enum MqttQos {
  atMostOnce(0),
  atLeastOnce(1),
  exactlyOnce(2);

  const MqttQos(this.value);

  final int value;

  static MqttQos fromValue(int value) {
    switch (value) {
      case 0:
        return MqttQos.atMostOnce;
      case 1:
        return MqttQos.atLeastOnce;
      case 2:
        return MqttQos.exactlyOnce;
      default:
        throw ArgumentError.value(value, 'value', 'Invalid QoS');
    }
  }
}
