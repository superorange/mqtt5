/// Base class for all MQTT related exceptions thrown by this library.
class MqttException implements Exception {
  MqttException(this.message, [this.cause]);

  final String message;
  final Object? cause;

  @override
  String toString() => 'MqttException: $message';
}

/// A violation of the MQTT protocol (on the wire level).
class MqttProtocolException extends MqttException {
  MqttProtocolException(super.message, [super.cause]);
}

/// A packet that cannot be decoded because it violates the wire format.
class MqttMalformedPacketException extends MqttProtocolException {
  MqttMalformedPacketException(super.message, [super.cause]);
}

/// A packet larger than the negotiated/configured maximum packet size.
class MqttPacketTooLargeException extends MqttProtocolException {
  MqttPacketTooLargeException(super.message, [super.cause]);
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
  MqttServerMovedException(this.reasonCode, this.serverReference, [Object? cause])
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

/// An exception thrown when flow control constraints would be violated.
class MqttFlowControlException extends MqttException {
  MqttFlowControlException(super.message, [super.cause]);
}

/// Internal marker used by the incremental decoder: there is not yet enough
/// buffered data to decode a complete packet. This is never surfaced to users.
class MqttIncompletePacketException extends MqttException {
  MqttIncompletePacketException(super.message);
}
