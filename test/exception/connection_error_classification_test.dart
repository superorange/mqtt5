import 'dart:async';
import 'dart:io';

import 'package:mqtt5/mqtt5.dart';
import 'package:test/test.dart';

void main() {
  test('classifies terminal connection errors', () {
    expect(
      isRetryableMqttConnectionError(
        TlsException('TLSV1_ALERT_UNKNOWN_CA'),
      ),
      isFalse,
    );
    expect(
      isRetryableMqttConnectionError(
        MqttProtocolException('malformed packet'),
      ),
      isFalse,
    );
    expect(
      isRetryableMqttConnectionError(
        MqttAuthenticationException('not authorized'),
      ),
      isFalse,
    );
    expect(
      isRetryableMqttConnectionError(
        MqttServerRejectedException(0x87, 'not authorized'),
      ),
      isFalse,
    );
    expect(
        isRetryableMqttConnectionError(StateError('factory failed')), isFalse);
  });

  test('classifies temporary connection errors', () {
    expect(
      isRetryableMqttConnectionError(
        MqttServerRejectedException(0x88, 'server unavailable'),
      ),
      isTrue,
    );
    expect(
      isRetryableMqttConnectionError(
        const SocketException('connection refused'),
      ),
      isTrue,
    );
    expect(
      isRetryableMqttConnectionError(TimeoutException('connect timeout')),
      isTrue,
    );
  });
}
