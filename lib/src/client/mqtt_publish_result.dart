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
}
