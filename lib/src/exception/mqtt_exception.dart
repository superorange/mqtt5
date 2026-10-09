import 'dart:io';

/// An MQTT failure together with the stack captured where the library first
/// classified or reported it. [MqttClient.errors] remains available for
/// backwards compatibility; diagnostics should prefer the structured stream.
final class MqttErrorEvent {
  const MqttErrorEvent(this.error, this.stackTrace);

  final Object error;
  final StackTrace stackTrace;
}

/// Whether retrying the same MQTT connection settings may make progress.
///
/// Network failures and temporary broker rejections are retryable. TLS,
/// protocol, authentication and permanent CONNACK rejections are not.
bool isRetryableMqttConnectionError(Object error) {
  if (error is Error ||
      error is TlsException ||
      error is MqttProtocolException ||
      error is MqttAuthenticationException ||
      error is MqttServerMovedException) {
    return false;
  }
  if (error is MqttServerRejectedException) {
    return _retryableConnackReasonCodes.contains(error.reasonCode);
  }
  return true;
}

const _retryableConnackReasonCodes = <int>{
  0x80, // Unspecified error
  0x83, // Implementation specific error
  0x88, // Server unavailable
  0x89, // Server busy
  0x97, // Quota exceeded
  0x9F, // Connection rate exceeded
};

/// Base class for all MQTT related exceptions thrown by this library.
class MqttException implements Exception {
  MqttException(this.message, [this.cause]);

  final String message;
  final Object? cause;

  @override
  String toString() {
    final cause = this.cause;
    return '$runtimeType: $message${cause == null ? '' : ' (cause: $cause)'}';
  }
}

/// A violation of the MQTT protocol (on the wire level).
class MqttProtocolException extends MqttException {
  MqttProtocolException(super.message, [super.cause]);
}

/// A packet that cannot be decoded because it violates the wire format.
class MqttMalformedPacketException extends MqttProtocolException {
  MqttMalformedPacketException(super.message, [super.cause]);
}

/// A Topic Alias that is 0 or above the negotiated maximum
/// (section 3.3.2.3.4). Answered with DISCONNECT 0x94.
///
/// An alias inside that range with an empty topic name, which this client
/// has not been told a mapping for, is a plain [MqttProtocolException]
/// (DISCONNECT 0x82). It is not this type.
class MqttTopicAliasInvalidException extends MqttProtocolException {
  MqttTopicAliasInvalidException(super.message, [super.cause]);
}

/// The peer sent more unacknowledged QoS 1/2 publications than the Receive
/// Maximum this client declared in its CONNECT packet.
class MqttReceiveMaximumExceededException extends MqttProtocolException {
  MqttReceiveMaximumExceededException(super.message, [super.cause]);
}

/// A packet larger than the negotiated/configured maximum packet size.
class MqttPacketTooLargeException extends MqttProtocolException {
  MqttPacketTooLargeException(super.message, [super.cause]);
}

/// The broker resumed a session (Session Present 1) that this client instance
/// holds no state for (MQTT-3.2.2-4).
///
/// Session state lives in memory and belongs to one `MqttClient` instance. A
/// new instance — in a new process or the same one — that connects with
/// `cleanStart: false` while the broker still keeps the session gets this
/// error, and the connection is closed with DISCONNECT 0x82. Connect with
/// `cleanStart: true`, or pass `adoptBrokerSession: true` to `connect` to
/// take the broker's session over knowingly.
class MqttSessionNotOwnedException extends MqttProtocolException {
  MqttSessionNotOwnedException(super.message, [super.cause]);
}

/// An exception thrown by transport level failures.
class MqttTransportException extends MqttException {
  MqttTransportException(super.message, [super.cause]);
}

/// An exception thrown when a connection cannot be established or is lost.
class MqttConnectionException extends MqttException {
  MqttConnectionException(super.message, [super.cause]);
}

/// An exception thrown when enhanced authentication fails.
class MqttAuthenticationException extends MqttException {
  MqttAuthenticationException(super.message, [super.cause]);
}

/// The server asked the client to connect to another server, or reported that
/// it has moved.
class MqttServerMovedException extends MqttException {
  MqttServerMovedException(this.reasonCode, this.serverReference,
      [Object? cause])
      : super(
          'Server requested connection to another server: $serverReference',
          cause,
        );

  final int reasonCode;
  final String? serverReference;
}

/// An exception thrown when the server rejects a request with a
/// [MqttReasonCode].
class MqttServerRejectedException extends MqttException {
  MqttServerRejectedException(this.reasonCode, super.message, [super.cause]);

  final int reasonCode;
}

/// An exception thrown when the broker does not acknowledge an operation
/// within the configured timeout.
class MqttTimeoutException extends MqttException {
  MqttTimeoutException(super.message, [super.cause]);
}

/// An exception thrown when flow control constraints would be violated.
class MqttFlowControlException extends MqttException {
  MqttFlowControlException(super.message, [super.cause]);
}

/// A packet whose fields run past the end of its declared Remaining Length.
///
/// The incremental decoder only ever hands complete packets to the parsers,
/// so running out of bytes inside one means the packet itself is malformed
/// (section 4.13), not that more data is on its way.
class MqttIncompletePacketException extends MqttMalformedPacketException {
  MqttIncompletePacketException(super.message);
}
