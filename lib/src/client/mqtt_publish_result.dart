import '../packet/mqtt_reason_code.dart';
import '../property/mqtt_property.dart';

/// The outcome of a publish operation once protocol acknowledgement completes.
final class MqttPublishResult {
  const MqttPublishResult({
    this.reasonCode = MqttReasonCode.success,
    this.properties = const [],
  });

  final MqttReasonCode reasonCode;
  final List<MqttProperty> properties;

  /// Whether the broker accepted the publication.
  ///
  /// Reason code 0x10 (No matching subscribers) is a success: the broker took
  /// the message and no subscriber matched. Test errors with [isError], or
  /// compare [reasonCode] with `0x80`.
  bool get isSuccess => reasonCode.value < 0x80;

  /// Whether [reasonCode] is 0x80 or greater.
  bool get isError => reasonCode.value >= 0x80;
}
