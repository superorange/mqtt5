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
  bool requestResponseInformation = false;
}
