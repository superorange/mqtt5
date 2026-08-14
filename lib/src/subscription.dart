import 'exception/mqtt_exception.dart';
import 'mqtt_qos.dart';

/// MQTT Retain Handling subscription option (specification section 3.8.3.1).
enum MqttRetainHandling {
  sendAtSubscribe(0),
  sendIfNew(1),
  doNotSend(2);

  const MqttRetainHandling(this.value);

  final int value;
}

/// Subscription options carried in a SUBSCRIBE packet.
final class MqttSubscriptionOptions {
  const MqttSubscriptionOptions({
    this.qos = MqttQos.atMostOnce,
    this.noLocal = false,
    this.retainAsPublished = false,
    this.retainHandling = MqttRetainHandling.sendAtSubscribe,
  });

  final MqttQos qos;
  final bool noLocal;
  final bool retainAsPublished;
  final MqttRetainHandling retainHandling;

  /// Encodes the subscription options byte (specification section 3.8.3.1).
  int toByte() {
    var value = qos.value & 0x03;
    if (noLocal) {
      value |= 0x04;
    }
    if (retainAsPublished) {
      value |= 0x08;
    }
    value |= (retainHandling.value & 0x03) << 4;
    return value;
  }

  /// Decodes the subscription options byte.
  ///
  /// Throws [MqttMalformedPacketException] if the byte encodes a reserved QoS
  /// or Retain Handling value.
  static MqttSubscriptionOptions fromByte(int byte) {
    final retainHandling = (byte >> 4) & 0x03;
    if (retainHandling == 3) {
      throw MqttMalformedPacketException(
        'Invalid Retain Handling value: $retainHandling',
      );
    }
    return MqttSubscriptionOptions(
      qos: MqttQos.fromValue(byte & 0x03),
      noLocal: byte & 0x04 != 0,
      retainAsPublished: byte & 0x08 != 0,
      retainHandling: MqttRetainHandling.values[retainHandling],
    );
  }

  @override
  bool operator ==(Object other) =>
      other is MqttSubscriptionOptions &&
      other.qos == qos &&
      other.noLocal == noLocal &&
      other.retainAsPublished == retainAsPublished &&
      other.retainHandling == retainHandling;

  @override
  int get hashCode =>
      Object.hash(qos, noLocal, retainAsPublished, retainHandling);

  @override
  String toString() =>
      'MqttSubscriptionOptions(qos: $qos, noLocal: $noLocal, '
      'retainAsPublished: $retainAsPublished, retainHandling: $retainHandling)';
}

/// A topic filter and its options, as sent in a SUBSCRIBE packet.
final class MqttSubscription {
  const MqttSubscription(this.topicFilter, {this.options = const MqttSubscriptionOptions()});

  final String topicFilter;
  final MqttSubscriptionOptions options;

  @override
  bool operator ==(Object other) =>
      other is MqttSubscription &&
      other.topicFilter == topicFilter &&
      other.options == options;

  @override
  int get hashCode => Object.hash(topicFilter, options);
}
