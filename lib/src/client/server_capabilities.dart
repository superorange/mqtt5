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
  }
}
