import 'dart:typed_data';

import '../exception/mqtt_exception.dart';
import '../topic.dart';

/// The wire data type of a property value.
enum MqttPropertyType {
  byte,
  twoByteInteger,
  fourByteInteger,
  utf8String,
  utf8StringPair,
  binaryData,
  variableByteInteger,
}

/// The control packet (or pseudo-packet) a property appears in.
enum MqttPropertyContext {
  connect,
  connack,
  publish,
  will,
  puback,
  pubrec,
  pubrel,
  pubcomp,
  subscribe,
  suback,
  unsubscribe,
  unsuback,
  disconnect,
  auth,
}

/// Declarative metadata describing a single MQTT 5.0 property.
///
/// The codec and validation logic are driven by this metadata instead of
/// hand written switches scattered across decoders.
final class MqttPropertyMeta {
  const MqttPropertyMeta({
    required this.identifier,
    required this.name,
    required this.type,
    required this.allowedPackets,
    this.repeatable = false,
    this.singleUseIn = const <MqttPropertyContext>{},
    this.validator,
    required this.create,
  });

  final int identifier;
  final String name;
  final MqttPropertyType type;
  final Set<MqttPropertyContext> allowedPackets;

  /// Whether the property may appear more than once.
  final bool repeatable;

  /// Contexts that override [repeatable] and permit only a single occurrence.
  ///
  /// Subscription Identifier may repeat in a PUBLISH (one per matching
  /// subscription) but must appear at most once in a SUBSCRIBE.
  final Set<MqttPropertyContext> singleUseIn;

  /// Whether the property may repeat in [context].
  bool repeatsIn(MqttPropertyContext context) =>
      repeatable && !singleUseIn.contains(context);

  /// Validates the wire value; throws [MqttProtocolException] on failure.
  final void Function(Object value)? validator;

  final MqttProperty Function(Object value) create;
}

/// Base class of all strongly typed MQTT 5.0 properties.
sealed class MqttProperty {
  const MqttProperty(this.identifier);

  final int identifier;

  MqttPropertyMeta get meta => mqttPropertyMetaById[identifier]!;

  String get propertyName => meta.name;

  Object get wireValue;

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) {
      return true;
    }
    if (other is! MqttProperty) {
      return false;
    }
    if (other.identifier != identifier) {
      return false;
    }
    return _valuesEqual(wireValue, other.wireValue);
  }

  @override
  int get hashCode => Object.hash(identifier, _valueHash(wireValue));

  @override
  String toString() => '$propertyName($wireValue)';

  static bool _valuesEqual(Object a, Object b) {
    if (a is Uint8List && b is Uint8List) {
      if (a.length != b.length) {
        return false;
      }
      for (var i = 0; i < a.length; i++) {
        if (a[i] != b[i]) {
          return false;
        }
      }
      return true;
    }
    return a == b;
  }

  static int _valueHash(Object value) {
    if (value is Uint8List) {
      return Object.hashAll(value);
    }
    return value.hashCode;
  }
}

// ---------------------------------------------------------------------------
// Concrete property classes
// ---------------------------------------------------------------------------

/// 0x01 Payload Format Indicator (Byte).
final class PayloadFormatIndicator extends MqttProperty {
  const PayloadFormatIndicator(this.value) : super(0x01);

  /// 0 = bytes, 1 = UTF-8 payload.
  final int value;

  @override
  Object get wireValue => value;
}

/// 0x02 Message Expiry Interval (Four Byte Integer), in seconds.
final class MessageExpiryInterval extends MqttProperty {
  const MessageExpiryInterval(this.seconds) : super(0x02);

  final int seconds;

  @override
  Object get wireValue => seconds;
}

/// 0x03 Content Type (UTF-8 String).
final class ContentType extends MqttProperty {
  const ContentType(this.value) : super(0x03);

  final String value;

  @override
  Object get wireValue => value;
}

/// 0x08 Response Topic (UTF-8 String).
final class ResponseTopic extends MqttProperty {
  const ResponseTopic(this.value) : super(0x08);

  final String value;

  @override
  Object get wireValue => value;
}

/// 0x09 Correlation Data (Binary Data).
final class CorrelationData extends MqttProperty {
  CorrelationData(List<int> value)
      : data = Uint8List.fromList(value),
        super(0x09);

  final Uint8List data;

  @override
  Object get wireValue => data;
}

/// 0x0B Subscription Identifier (Variable Byte Integer).
final class SubscriptionIdentifier extends MqttProperty {
  const SubscriptionIdentifier(this.value) : super(0x0B);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x11 Session Expiry Interval (Four Byte Integer), in seconds.
final class SessionExpiryInterval extends MqttProperty {
  const SessionExpiryInterval(this.seconds) : super(0x11);

  final int seconds;

  @override
  Object get wireValue => seconds;
}

/// 0x12 Assigned Client Identifier (UTF-8 String).
final class AssignedClientIdentifier extends MqttProperty {
  const AssignedClientIdentifier(this.value) : super(0x12);

  final String value;

  @override
  Object get wireValue => value;
}

/// 0x13 Server Keep Alive (Two Byte Integer), in seconds.
final class ServerKeepAlive extends MqttProperty {
  const ServerKeepAlive(this.seconds) : super(0x13);

  final int seconds;

  @override
  Object get wireValue => seconds;
}

/// 0x15 Authentication Method (UTF-8 String).
final class AuthenticationMethod extends MqttProperty {
  const AuthenticationMethod(this.value) : super(0x15);

  final String value;

  @override
  Object get wireValue => value;
}

/// 0x16 Authentication Data (Binary Data).
final class AuthenticationData extends MqttProperty {
  AuthenticationData(List<int> value)
      : data = Uint8List.fromList(value),
        super(0x16);

  final Uint8List data;

  @override
  Object get wireValue => data;
}

/// 0x17 Request Problem Information (Byte).
final class RequestProblemInformation extends MqttProperty {
  const RequestProblemInformation(this.value) : super(0x17);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x18 Will Delay Interval (Four Byte Integer), in seconds.
final class WillDelayInterval extends MqttProperty {
  const WillDelayInterval(this.seconds) : super(0x18);

  final int seconds;

  @override
  Object get wireValue => seconds;
}

/// 0x19 Request Response Information (Byte).
final class RequestResponseInformation extends MqttProperty {
  const RequestResponseInformation(this.value) : super(0x19);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x1A Response Information (UTF-8 String).
final class ResponseInformation extends MqttProperty {
  const ResponseInformation(this.value) : super(0x1A);

  final String value;

  @override
  Object get wireValue => value;
}

/// 0x1C Server Reference (UTF-8 String).
final class ServerReference extends MqttProperty {
  const ServerReference(this.value) : super(0x1C);

  final String value;

  @override
  Object get wireValue => value;
}

/// 0x1F Reason String (UTF-8 String).
final class ReasonString extends MqttProperty {
  const ReasonString(this.value) : super(0x1F);

  final String value;

  @override
  Object get wireValue => value;
}

/// 0x21 Receive Maximum (Two Byte Integer).
final class ReceiveMaximum extends MqttProperty {
  const ReceiveMaximum(this.value) : super(0x21);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x22 Topic Alias Maximum (Two Byte Integer).
final class TopicAliasMaximum extends MqttProperty {
  const TopicAliasMaximum(this.value) : super(0x22);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x23 Topic Alias (Two Byte Integer).
final class TopicAlias extends MqttProperty {
  const TopicAlias(this.value) : super(0x23);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x24 Maximum QoS (Byte).
final class MaximumQos extends MqttProperty {
  const MaximumQos(this.value) : super(0x24);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x25 Retain Available (Byte).
final class RetainAvailable extends MqttProperty {
  const RetainAvailable(this.value) : super(0x25);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x26 User Property (UTF-8 String Pair).
final class UserProperty extends MqttProperty {
  const UserProperty(this.name, this.value) : super(0x26);

  final String name;
  final String value;

  @override
  Object get wireValue => (name, value);
}

/// 0x27 Maximum Packet Size (Four Byte Integer).
final class MaximumPacketSize extends MqttProperty {
  const MaximumPacketSize(this.value) : super(0x27);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x28 Wildcard Subscription Available (Byte).
final class WildcardSubscriptionAvailable extends MqttProperty {
  const WildcardSubscriptionAvailable(this.value) : super(0x28);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x29 Subscription Identifier Available (Byte).
final class SubscriptionIdentifierAvailable extends MqttProperty {
  const SubscriptionIdentifierAvailable(this.value) : super(0x29);

  final int value;

  @override
  Object get wireValue => value;
}

/// 0x2A Shared Subscription Available (Byte).
final class SharedSubscriptionAvailable extends MqttProperty {
  const SharedSubscriptionAvailable(this.value) : super(0x2A);

  final int value;

  @override
  Object get wireValue => value;
}

// ---------------------------------------------------------------------------
// Validators
// ---------------------------------------------------------------------------

void _requireBool(Object value) {
  final v = value as int;
  if (v != 0 && v != 1) {
    throw MqttProtocolException('Expected 0 or 1, got $v');
  }
}

void _requireNonZero(Object value) {
  final v = value as int;
  if (v == 0) {
    throw MqttProtocolException('Value must not be 0');
  }
}

void _requireMaximumQos(Object value) {
  final v = value as int;
  if (v != 0 && v != 1) {
    throw MqttProtocolException('Maximum QoS must be 0 or 1, got $v');
  }
}

/// MQTT-3.3.2-14: the Response Topic MUST NOT contain wildcard characters.
/// It is a Topic Name, so the rest of section 4.7.3 applies as well.
void _requireTopicName(Object value) {
  final problem = MqttTopic.checkName(value as String);
  if (problem != null) {
    throw MqttProtocolException('Response Topic $problem');
  }
}

// ---------------------------------------------------------------------------
// Metadata registry
// ---------------------------------------------------------------------------

/// Maps every property identifier to its metadata.
final Map<int, MqttPropertyMeta> mqttPropertyMetaById = _buildMeta();

Map<int, MqttPropertyMeta> _buildMeta() {
  final metas = <MqttPropertyMeta>[
    MqttPropertyMeta(
      identifier: 0x01,
      name: 'Payload Format Indicator',
      type: MqttPropertyType.byte,
      allowedPackets: {
        MqttPropertyContext.publish,
        MqttPropertyContext.will,
      },
      validator: _requireBool,
      create: (v) => PayloadFormatIndicator(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x02,
      name: 'Message Expiry Interval',
      type: MqttPropertyType.fourByteInteger,
      allowedPackets: {
        MqttPropertyContext.publish,
        MqttPropertyContext.will,
      },
      create: (v) => MessageExpiryInterval(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x03,
      name: 'Content Type',
      type: MqttPropertyType.utf8String,
      allowedPackets: {
        MqttPropertyContext.publish,
        MqttPropertyContext.will,
      },
      create: (v) => ContentType(v as String),
    ),
    MqttPropertyMeta(
      identifier: 0x08,
      name: 'Response Topic',
      type: MqttPropertyType.utf8String,
      allowedPackets: {
        MqttPropertyContext.publish,
        MqttPropertyContext.will,
      },
      validator: _requireTopicName,
      create: (v) => ResponseTopic(v as String),
    ),
    MqttPropertyMeta(
      identifier: 0x09,
      name: 'Correlation Data',
      type: MqttPropertyType.binaryData,
      allowedPackets: {
        MqttPropertyContext.publish,
        MqttPropertyContext.will,
      },
      create: (v) => CorrelationData(v as Uint8List),
    ),
    MqttPropertyMeta(
      identifier: 0x0B,
      name: 'Subscription Identifier',
      type: MqttPropertyType.variableByteInteger,
      allowedPackets: {
        MqttPropertyContext.publish,
        MqttPropertyContext.subscribe,
      },
      repeatable: true,
      singleUseIn: {MqttPropertyContext.subscribe},
      validator: _requireNonZero,
      create: (v) => SubscriptionIdentifier(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x11,
      name: 'Session Expiry Interval',
      type: MqttPropertyType.fourByteInteger,
      allowedPackets: {
        MqttPropertyContext.connect,
        MqttPropertyContext.connack,
        MqttPropertyContext.disconnect,
      },
      create: (v) => SessionExpiryInterval(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x12,
      name: 'Assigned Client Identifier',
      type: MqttPropertyType.utf8String,
      allowedPackets: {MqttPropertyContext.connack},
      create: (v) => AssignedClientIdentifier(v as String),
    ),
    MqttPropertyMeta(
      identifier: 0x13,
      name: 'Server Keep Alive',
      type: MqttPropertyType.twoByteInteger,
      allowedPackets: {MqttPropertyContext.connack},
      create: (v) => ServerKeepAlive(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x15,
      name: 'Authentication Method',
      type: MqttPropertyType.utf8String,
      allowedPackets: {
        MqttPropertyContext.connect,
        MqttPropertyContext.connack,
        MqttPropertyContext.auth,
      },
      create: (v) => AuthenticationMethod(v as String),
    ),
    MqttPropertyMeta(
      identifier: 0x16,
      name: 'Authentication Data',
      type: MqttPropertyType.binaryData,
      allowedPackets: {
        MqttPropertyContext.connect,
        MqttPropertyContext.connack,
        MqttPropertyContext.auth,
      },
      create: (v) => AuthenticationData(v as Uint8List),
    ),
    MqttPropertyMeta(
      identifier: 0x17,
      name: 'Request Problem Information',
      type: MqttPropertyType.byte,
      allowedPackets: {MqttPropertyContext.connect},
      validator: _requireBool,
      create: (v) => RequestProblemInformation(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x18,
      name: 'Will Delay Interval',
      type: MqttPropertyType.fourByteInteger,
      allowedPackets: {MqttPropertyContext.will},
      create: (v) => WillDelayInterval(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x19,
      name: 'Request Response Information',
      type: MqttPropertyType.byte,
      allowedPackets: {MqttPropertyContext.connect},
      validator: _requireBool,
      create: (v) => RequestResponseInformation(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x1A,
      name: 'Response Information',
      type: MqttPropertyType.utf8String,
      allowedPackets: {MqttPropertyContext.connack},
      create: (v) => ResponseInformation(v as String),
    ),
    MqttPropertyMeta(
      identifier: 0x1C,
      name: 'Server Reference',
      type: MqttPropertyType.utf8String,
      allowedPackets: {
        MqttPropertyContext.connack,
        MqttPropertyContext.disconnect,
      },
      create: (v) => ServerReference(v as String),
    ),
    MqttPropertyMeta(
      identifier: 0x1F,
      name: 'Reason String',
      type: MqttPropertyType.utf8String,
      allowedPackets: {
        MqttPropertyContext.connack,
        MqttPropertyContext.puback,
        MqttPropertyContext.pubrec,
        MqttPropertyContext.pubrel,
        MqttPropertyContext.pubcomp,
        MqttPropertyContext.suback,
        MqttPropertyContext.unsuback,
        MqttPropertyContext.disconnect,
        MqttPropertyContext.auth,
      },
      create: (v) => ReasonString(v as String),
    ),
    MqttPropertyMeta(
      identifier: 0x21,
      name: 'Receive Maximum',
      type: MqttPropertyType.twoByteInteger,
      allowedPackets: {
        MqttPropertyContext.connect,
        MqttPropertyContext.connack,
      },
      validator: _requireNonZero,
      create: (v) => ReceiveMaximum(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x22,
      name: 'Topic Alias Maximum',
      type: MqttPropertyType.twoByteInteger,
      allowedPackets: {
        MqttPropertyContext.connect,
        MqttPropertyContext.connack,
      },
      create: (v) => TopicAliasMaximum(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x23,
      name: 'Topic Alias',
      type: MqttPropertyType.twoByteInteger,
      allowedPackets: {MqttPropertyContext.publish},
      validator: _requireNonZero,
      create: (v) => TopicAlias(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x24,
      name: 'Maximum QoS',
      type: MqttPropertyType.byte,
      allowedPackets: {MqttPropertyContext.connack},
      validator: _requireMaximumQos,
      create: (v) => MaximumQos(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x25,
      name: 'Retain Available',
      type: MqttPropertyType.byte,
      allowedPackets: {MqttPropertyContext.connack},
      validator: _requireBool,
      create: (v) => RetainAvailable(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x26,
      name: 'User Property',
      type: MqttPropertyType.utf8StringPair,
      allowedPackets: {
        MqttPropertyContext.connect,
        MqttPropertyContext.connack,
        MqttPropertyContext.publish,
        MqttPropertyContext.will,
        MqttPropertyContext.puback,
        MqttPropertyContext.pubrec,
        MqttPropertyContext.pubrel,
        MqttPropertyContext.pubcomp,
        MqttPropertyContext.subscribe,
        MqttPropertyContext.suback,
        MqttPropertyContext.unsubscribe,
        MqttPropertyContext.unsuback,
        MqttPropertyContext.disconnect,
        MqttPropertyContext.auth,
      },
      repeatable: true,
      create: (v) {
        final pair = v as (String, String);
        return UserProperty(pair.$1, pair.$2);
      },
    ),
    MqttPropertyMeta(
      identifier: 0x27,
      name: 'Maximum Packet Size',
      type: MqttPropertyType.fourByteInteger,
      allowedPackets: {
        MqttPropertyContext.connect,
        MqttPropertyContext.connack,
      },
      validator: _requireNonZero,
      create: (v) => MaximumPacketSize(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x28,
      name: 'Wildcard Subscription Available',
      type: MqttPropertyType.byte,
      allowedPackets: {MqttPropertyContext.connack},
      validator: _requireBool,
      create: (v) => WildcardSubscriptionAvailable(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x29,
      name: 'Subscription Identifier Available',
      type: MqttPropertyType.byte,
      allowedPackets: {MqttPropertyContext.connack},
      validator: _requireBool,
      create: (v) => SubscriptionIdentifierAvailable(v as int),
    ),
    MqttPropertyMeta(
      identifier: 0x2A,
      name: 'Shared Subscription Available',
      type: MqttPropertyType.byte,
      allowedPackets: {MqttPropertyContext.connack},
      validator: _requireBool,
      create: (v) => SharedSubscriptionAvailable(v as int),
    ),
  ];
  return {for (final m in metas) m.identifier: m};
}

/// Looks up the metadata for [identifier], or null if unknown.
MqttPropertyMeta? propertyMetaFor(int identifier) => mqttPropertyMetaById[identifier];
