import 'dart:typed_data';

/// A challenge presented by the server during enhanced authentication.
final class MqttAuthChallenge {
  const MqttAuthChallenge({this.method, this.data});

  final String? method;
  final Uint8List? data;
}

/// The client's response to an authentication challenge.
final class MqttAuthResponse {
  const MqttAuthResponse(this.data);

  final Uint8List data;
}

/// Callback interface for MQTT 5.0 enhanced authentication.
///
/// The broker challenges the client; the client returns [MqttAuthResponse]
/// with the next round of authentication data, or null to abort.
abstract interface class MqttAuthenticator {
  Future<MqttAuthResponse?> authenticate(MqttAuthChallenge challenge);
}
