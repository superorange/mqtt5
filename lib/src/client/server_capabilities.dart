/// Capabilities and limits advertised by the server in its CONNACK packet.
final class ServerCapabilities {
  int receiveMaximum = 65535;
  int maximumPacketSize = 268435455;
  int maximumQos = 2;
  bool retainAvailable = true;
  int topicAliasMaximum = 0;
  bool wildcardSubscriptionAvailable = true;
  bool subscriptionIdentifierAvailable = true;
  bool sharedSubscriptionAvailable = true;

  /// The Response Information the broker returned in CONNACK, used as the
  /// prefix for request/response topics. Null unless the client asked for it
  /// with a Request Response Information property in CONNECT.
  String? responseInformation;

  /// The Session Expiry Interval the broker granted, when it chose a value
  /// other than the one requested in CONNECT (section 3.2.2.3.2).
  ///
  /// This is reported, not applied: the value in CONNECT is what the client
  /// asks for, and a later reconnect asks for it again rather than silently
  /// settling for whatever the busiest moment of the previous connection
  /// allowed.
  Duration? sessionExpiryInterval;

  /// The keep alive the broker imposed, when it sent a Server Keep Alive
  /// property in CONNACK (section 3.2.2.3.4).
  Duration? serverKeepAlive;

  /// An independent copy, so handing these out cannot reach back into the
  /// values the client enforces its limits from.
  ServerCapabilities copy() => ServerCapabilities()
    ..receiveMaximum = receiveMaximum
    ..maximumPacketSize = maximumPacketSize
    ..maximumQos = maximumQos
    ..retainAvailable = retainAvailable
    ..topicAliasMaximum = topicAliasMaximum
    ..wildcardSubscriptionAvailable = wildcardSubscriptionAvailable
    ..subscriptionIdentifierAvailable = subscriptionIdentifierAvailable
    ..sharedSubscriptionAvailable = sharedSubscriptionAvailable
    ..responseInformation = responseInformation
    ..sessionExpiryInterval = sessionExpiryInterval
    ..serverKeepAlive = serverKeepAlive;

  /// Restores the protocol defaults.
  ///
  /// Capabilities are per-connection: a property absent from a CONNACK means
  /// "use the default", so they must be reset before every CONNACK is applied
  /// or values from an earlier connection leak into the new one.
  void reset() {
    receiveMaximum = 65535;
    maximumPacketSize = 268435455;
    maximumQos = 2;
    retainAvailable = true;
    topicAliasMaximum = 0;
    wildcardSubscriptionAvailable = true;
    subscriptionIdentifierAvailable = true;
    sharedSubscriptionAvailable = true;
    responseInformation = null;
    sessionExpiryInterval = null;
    serverKeepAlive = null;
  }
}
