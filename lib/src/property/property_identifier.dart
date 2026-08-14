/// MQTT 5.0 Property Identifiers (specification section 2.2.2.2).
enum MqttPropertyIdentifier {
  payloadFormatIndicator(0x01),
  messageExpiryInterval(0x02),
  contentType(0x03),
  responseTopic(0x08),
  correlationData(0x09),
  subscriptionIdentifier(0x0B),
  sessionExpiryInterval(0x11),
  assignedClientIdentifier(0x12),
  serverKeepAlive(0x13),
  authenticationMethod(0x15),
  authenticationData(0x16),
  requestProblemInformation(0x17),
  willDelayInterval(0x18),
  requestResponseInformation(0x19),
  responseInformation(0x1A),
  serverReference(0x1C),
  reasonString(0x1F),
  receiveMaximum(0x21),
  topicAliasMaximum(0x22),
  topicAlias(0x23),
  maximumQos(0x24),
  retainAvailable(0x25),
  userProperty(0x26),
  maximumPacketSize(0x27),
  wildcardSubscriptionAvailable(0x28),
  subscriptionIdentifierAvailable(0x29),
  sharedSubscriptionAvailable(0x2A);

  const MqttPropertyIdentifier(this.value);

  final int value;
}
