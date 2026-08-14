/// Helpers for testing code that talks MQTT, and for driving the protocol
/// engine without a socket.
///
/// This library is not needed to use [MqttClient]; it exists so tests can
/// inject bytes and inspect what the client wrote.
library;

export 'src/transport/memory_transport.dart';
