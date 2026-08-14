import 'dart:typed_data';

import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/client/reconnect_manager.dart';
import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:mqtt5/src/mqtt_qos.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/mqtt_packet.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/mqtt_reason_code.dart';
import 'package:mqtt5/src/packet/puback.dart';
import 'package:mqtt5/src/packet/publish.dart';
import 'package:mqtt5/src/packet/suback.dart';
import 'package:mqtt5/src/packet/subscribe.dart';
import 'package:mqtt5/src/transport/memory_transport.dart';
import 'package:test/test.dart';

void main() {
  test('session resume retransmits QoS1 inflight with DUP', () async {
    final transports = <MemoryTransport>[];
    final client = MqttClient(
      host: 'h',
      transportFactory: () {
        final t = MemoryTransport();
        transports.add(t);
        return t;
      },
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 10),
        maxDelay: const Duration(milliseconds: 10),
        jitterFactor: 0,
      ),
    );

    final connectFuture = client.connect(
      cleanStart: false,
      sessionExpiryInterval: const Duration(hours: 1),
    );
    final t1 = await _waitForTransport(transports, 1);
    await _nextPacket(t1);
    t1.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );
    await connectFuture;

    final publishFuture = client.publish(
      'resume/topic',
      Uint8List.fromList([1, 2, 3]),
      qos: MqttQos.atLeastOnce,
    );
    final original = await _nextPacket(t1) as MqttPublishPacket;
    expect(original.dup, isFalse);

    // Drop the connection before the broker acks.
    t1.injectError(MqttTransportException('simulated drop'));

    final t2 = await _waitForTransport(transports, 2);
    await _nextPacket(t2);
    t2.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: true),
      ),
    );

    final retransmit = await _nextPacket(t2) as MqttPublishPacket;
    expect(retransmit.packetIdentifier, original.packetIdentifier);
    expect(retransmit.dup, isTrue);
    expect(retransmit.payload, [1, 2, 3]);

    t2.inject(
      MqttPacketCodec.encode(
        MqttPubackPacket(packetIdentifier: retransmit.packetIdentifier),
      ),
    );
    final result = await publishFuture;
    expect(result.reasonCode, MqttReasonCode.success);

    await client.disconnect();
  });

  test('session lost fails inflight publish and re-subscribes', () async {
    final transports = <MemoryTransport>[];
    final client = MqttClient(
      host: 'h',
      transportFactory: () {
        final t = MemoryTransport();
        transports.add(t);
        return t;
      },
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 10),
        maxDelay: const Duration(milliseconds: 10),
        jitterFactor: 0,
      ),
    );

    final connectFuture = client.connect(
      cleanStart: false,
      sessionExpiryInterval: const Duration(hours: 1),
    );
    final t1 = await _waitForTransport(transports, 1);
    await _nextPacket(t1);
    t1.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );
    await connectFuture;

    // Establish a subscription.
    final subscribeFuture = client.subscribe('resume/sub');
    final subPacket = await _nextPacket(t1) as MqttSubscribePacket;
    t1.inject(
      MqttPacketCodec.encode(
        MqttSubackPacket(
          packetIdentifier: subPacket.packetIdentifier,
          reasonCodes: const [0],
        ),
      ),
    );
    await subscribeFuture;

    // Publish QoS1 without ack.
    final publishFuture = client.publish(
      'resume/topic',
      Uint8List.fromList([9]),
      qos: MqttQos.atLeastOnce,
    );
    await _nextPacket(t1);

    // Drop; then broker reports no session on reconnect.
    t1.injectError(MqttTransportException('drop'));
    final t2 = await _waitForTransport(transports, 2);
    await _nextPacket(t2);
    t2.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );

    // Inflight publish must fail.
    await expectLater(
      publishFuture,
      throwsA(isA<MqttConnectionException>()),
    );

    // A re-subscribe must be sent automatically.
    final resubscribe = await _nextPacket(t2);
    expect(resubscribe, isA<MqttSubscribePacket>());
    expect(
      (resubscribe as MqttSubscribePacket).subscriptions.single.topicFilter,
      'resume/sub',
    );

    await client.disconnect();
  });
}

Future<MemoryTransport> _waitForTransport(
  List<MemoryTransport> transports,
  int index,
) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (transports.length < index) {
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for transport #$index');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  return transports[index - 1];
}

Future<MqttPacket> _nextPacket(MemoryTransport transport) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (true) {
    final bytes = transport.takeOutgoingBytes();
    if (bytes.isNotEmpty) {
      return MqttPacketCodec.decode(bytes);
    }
    if (DateTime.now().isAfter(deadline)) {
      fail('Timed out waiting for a packet from the client');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
