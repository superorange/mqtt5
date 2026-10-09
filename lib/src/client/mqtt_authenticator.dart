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

/// Optionally implemented by an [MqttAuthenticator] whose mechanism ends with
/// data the client must check, such as the server signature of SCRAM.
///
/// The connection is reported as connected (or a re-authentication as
/// complete) only after [verifyServer] returns; throwing rejects the server
/// and closes the connection.
abstract interface class MqttAuthenticationVerifier {
  /// Called with the Authentication Data carried by the successful CONNACK,
  /// or by the AUTH 0x00 that ends a re-authentication.
  Future<void> verifyServer(MqttAuthChallenge outcome);
}
