import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

void main() {
  test('public API exports packet types', () {
    expect(MqttQos.exactlyOnce.value, 2);
    expect(MqttPacketType.connect.value, 1);
    expect(MqttReasonCode.success.value, 0x00);
  });
}
