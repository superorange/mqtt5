/// The lifecycle states of an MQTT connection.
enum MqttConnectionState {
  disconnected,
  connecting,
  authenticating,
  connected,
  disconnecting,
  reconnecting,
}
