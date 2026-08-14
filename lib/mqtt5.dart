/// A pure-Dart MQTT 5.0 client library.
///
/// The client API is [MqttClient]. The packet, property and codec types are
/// also exported so the protocol engine can be used on its own (proxies,
/// brokers, packet inspection).
///
/// Internal machinery — the connection loop, flow controller, packet
/// identifier pool and session stores — is deliberately not exported; see
/// `package:mqtt5/testing.dart` for the in-memory transport used in tests.
library;

export 'src/client/mqtt_authenticator.dart';
export 'src/client/mqtt_client.dart';
export 'src/client/mqtt_connection_state.dart';
export 'src/client/mqtt_message.dart';
export 'src/client/mqtt_metrics.dart';
export 'src/client/mqtt_publish_result.dart';
export 'src/client/reconnect_manager.dart';
export 'src/client/server_capabilities.dart';
export 'src/codec/mqtt_packet_decoder.dart';
export 'src/exception/mqtt_exception.dart';
export 'src/logging/mqtt_logger.dart';
export 'src/mqtt_qos.dart';
export 'src/packet/auth.dart';
export 'src/packet/connack.dart';
export 'src/packet/connect.dart';
export 'src/packet/disconnect.dart';
export 'src/packet/mqtt_packet.dart';
export 'src/packet/mqtt_packet_codec.dart';
export 'src/packet/mqtt_reason_code.dart';
export 'src/packet/pingreq.dart';
export 'src/packet/pingresp.dart';
export 'src/packet/puback.dart';
export 'src/packet/pubcomp.dart';
export 'src/packet/publish.dart';
export 'src/packet/pubrec.dart';
export 'src/packet/pubrel.dart';
export 'src/packet/suback.dart';
export 'src/packet/subscribe.dart';
export 'src/packet/unsuback.dart';
export 'src/packet/unsubscribe.dart';
export 'src/property/mqtt_property.dart';
export 'src/property/property_identifier.dart';
export 'src/subscription.dart';
export 'src/transport/mqtt_transport.dart';
export 'src/transport/tcp_transport.dart';
export 'src/transport/tls_transport.dart';
