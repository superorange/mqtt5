import 'dart:async';
import 'dart:typed_data';

import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/client/mqtt_connection_state.dart';
import 'package:mqtt5/src/client/reconnect_manager.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/connect.dart';
import 'package:mqtt5/src/packet/disconnect.dart';
import 'package:mqtt5/src/packet/mqtt_packet.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/mqtt_reason_code.dart';
import 'package:mqtt5/src/packet/publish.dart';
import 'package:mqtt5/src/transport/memory_transport.dart';
import 'package:test/test.dart';

void main() {
  test('connect completes handshake with CONNACK', () async {
    final transport = MemoryTransport();
    final client = MqttClient(
      host: 'localhost',
      port: 1883,
      transportFactory: () => transport,
    );

    final connectFuture = client.connect(keepAlive: const Duration(seconds: 30));

    final connectPacket = await _nextPacket(transport) as MqttConnectPacket;
    expect(connectPacket.clientId, client.clientId);
    expect(connectPacket.cleanStart, isTrue);
    expect(connectPacket.keepAliveSeconds, 30);

    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );

    await connectFuture;
    expect(client.state, MqttConnectionState.connected);

    await client.disconnect();
    expect(client.state, MqttConnectionState.disconnected);
  });

  test('disconnect sends a DISCONNECT packet', () async {
    final transport = MemoryTransport();
    final client = MqttClient(
      host: 'localhost',
      port: 1883,
      transportFactory: () => transport,
    );

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );
    await connectFuture;

    final disconnectFuture = client.disconnect();
    final packet = await _nextPacket(transport) as MqttDisconnectPacket;
    expect(packet.reasonCode, MqttReasonCode.success);
    await disconnectFuture;
  });

  test('broker PUBLISH is delivered to the messages stream', () async {
    final transport = MemoryTransport();
    final client = MqttClient(
      host: 'localhost',
      port: 1883,
      transportFactory: () => transport,
    );

    final messageFuture = client.messages.first;
    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );
    await connectFuture;

    transport.inject(
      MqttPacketCodec.encode(
        MqttPublishPacket(
          topicName: 'hello/world',
          payload: Uint8List.fromList([1, 2, 3]),
        ),
      ),
    );

    final message = await messageFuture;
    expect(message.topic, 'hello/world');
    expect(message.payload, [1, 2, 3]);

    await client.disconnect();
  });

  test('rejects and retries with backoff', () async {
    final transports = <MemoryTransport>[];
    var connectCount = 0;
    final brokerDone = Completer<void>();

    final client = MqttClient(
      host: 'localhost',
      port: 1883,
      reconnectManager: ReconnectManager(
        initialDelay: const Duration(milliseconds: 40),
        maxDelay: const Duration(milliseconds: 40),
        jitterFactor: 0,
      ),
      transportFactory: () {
        final t = MemoryTransport();
        transports.add(t);
        return t;
      },
    );

    unawaited(_rejectingBroker(transports, () => ++connectCount, brokerDone));

    unawaited(client.connect());
    await brokerDone.future.timeout(const Duration(seconds: 5));
    expect(connectCount, greaterThanOrEqualTo(2));
    expect(client.state, MqttConnectionState.reconnecting);

    await client.disconnect();
  });
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

Future<void> _rejectingBroker(
  List<MemoryTransport> transports,
  int Function() onConnect,
  Completer<void> done,
) async {
  while (!done.isCompleted) {
    if (transports.isNotEmpty) {
      final transport = transports.last;
      final bytes = transport.takeOutgoingBytes();
      if (bytes.isNotEmpty) {
        final packet = MqttPacketCodec.decode(bytes);
        if (packet is MqttConnectPacket) {
          final count = onConnect();
          transport.inject(
            MqttPacketCodec.encode(
              const MqttConnackPacket(
                sessionPresent: false,
                reasonCode: MqttReasonCode.serverUnavailable,
              ),
            ),
          );
          if (count >= 2) {
            done.complete();
          }
        }
      }
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
