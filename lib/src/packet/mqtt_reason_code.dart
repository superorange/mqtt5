/// MQTT 5.0 Reason Codes (specification section 2.4).
enum MqttReasonCode {
  success(0x00),
  disconnectWithWillMessage(0x04),
  noMatchingSubscribers(0x10),
  noSubscriptionExisted(0x11),
  continueAuthentication(0x18),
  reAuthenticate(0x19),
  unspecifiedError(0x80),
  malformedPacket(0x81),
  protocolError(0x82),
  implementationSpecificError(0x83),
  unsupportedProtocolVersion(0x84),
  clientIdentifierNotValid(0x85),
  badUserNameOrPassword(0x86),
  notAuthorized(0x87),
  serverUnavailable(0x88),
  serverBusy(0x89),
  banned(0x8A),
  serverShuttingDown(0x8B),
  badAuthenticationMethod(0x8C),
  keepAliveTimeout(0x8D),
  sessionTakenOver(0x8E),
  topicFilterInvalid(0x8F),
  topicNameInvalid(0x90),
  packetIdentifierInUse(0x91),
  packetIdentifierNotFound(0x92),
  receiveMaximumExceeded(0x93),
  topicAliasInvalid(0x94),
  packetTooLarge(0x95),
  messageRateTooHigh(0x96),
  quotaExceeded(0x97),
  administrativeAction(0x98),
  payloadFormatInvalid(0x99),
  retainNotSupported(0x9A),
  qosNotSupported(0x9B),
  useAnotherServer(0x9C),
  serverMoved(0x9D),
  sharedSubscriptionsNotSupported(0x9E),
  connectionRateExceeded(0x9F),
  maximumConnectTime(0xA0),
  subscriptionIdentifiersNotSupported(0xA1),
  wildcardSubscriptionsNotSupported(0xA2);

  const MqttReasonCode(this.value);

  final int value;

  static MqttReasonCode? tryFromValue(int value) {
    for (final code in MqttReasonCode.values) {
      if (code.value == value) {
        return code;
      }
    }
    return null;
  }
}

/// Reason codes valid in a CONNACK packet (specification section 3.2.2.2).
const Set<int> connackReasonCodes = {
  0x00, 0x18, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89,
  0x8A, 0x8C, 0x90, 0x95, 0x97, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9F,
};

/// Reason codes valid in PUBACK and PUBREC packets (section 3.4.2.1 / 3.5.2.1).
const Set<int> pubackReasonCodes = {
  0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99,
};

/// Reason codes valid in PUBREL and PUBCOMP packets (section 3.6.2.1 / 3.7.2.1).
const Set<int> pubrelReasonCodes = {0x00, 0x92};

/// Reason codes valid in a SUBACK packet (section 3.9.2.1).
const Set<int> subackReasonCodes = {
  0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2,
};

/// Reason codes valid in an UNSUBACK packet (section 3.11.2.1).
const Set<int> unsubackReasonCodes = {
  0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97,
};

/// Reason codes valid in a DISCONNECT packet (section 3.14.2.1).
const Set<int> disconnectReasonCodes = {
  0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8D, 0x8E, 0x8F,
  0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D,
  0x9E, 0x9F, 0xA0, 0xA1, 0xA2,
};

/// Reason codes valid in an AUTH packet (section 3.15.2.1).
const Set<int> authReasonCodes = {0x00, 0x18, 0x19};
