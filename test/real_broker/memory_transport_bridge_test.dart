/// `MemoryTransport` (exported from `package:mqtt5/testing.dart`) carrying
/// real traffic: its outgoing bytes are pumped to a real mosquitto socket and
/// the broker's bytes are injected back. Nothing imitates the broker; the
/// transport is exercised as the in-memory adapter it is meant to be.
@Tags(['real-broker'])
@Timeout(Duration(seconds: 60))
library;

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:mqtt5/mqtt5.dart';
import 'package:mqtt5/testing.dart';
import 'package:test/test.dart';

import 'support/common.dart';

/// Connects a [MemoryTransport] to a real broker socket.
final class Bridge {
  Bridge(int port, {this.wholeBuffer = false}) {
    _connect(port);
  }

  final MemoryTransport transport = MemoryTransport();
  final bool wholeBuffer;
  Socket? _socket;
  Timer? _pump;
  Object? socketError;

  Future<void> _connect(int port) async {
    final socket = await Socket.connect('127.0.0.1', port);
    socket.done.then((_) {}, onError: (_) {});
    _socket = socket;
    socket.listen(
      transport.inject,
      onError: (Object e) {
        socketError = e;
        transport.injectError(e);
      },
      onDone: transport.injectDone,
    );
    _pump = Timer.periodic(const Duration(milliseconds: 2), (_) {
      if (transport.isClosed) {
        stop();
        return;
      }
      if (transport.outgoing.isEmpty) return;
      if (wholeBuffer) {
        socket.add(transport.takeOutgoingBytes());
      } else {
        for (final chunk in transport.takeOutgoing()) {
          socket.add(chunk);
        }
      }
    });
  }

  void stop() {
    _pump?.cancel();
    _socket?.destroy();
  }
}

void main() {
  if (!Mosquitto.available) {
    test('mosquitto not installed', () {}, skip: 'mosquitto not found');
    return;
  }

  late Mosquitto broker;
  late FaultProxy proxy;
  setUp(() async {
    broker = await Mosquitto.start();
    proxy = await FaultProxy.start(broker.port);
  });
  tearDown(() async {
    await proxy.close();
    await broker.dispose();
  });

  test(
      'a client on MemoryTransport bridged to mosquitto: round trip, peer '
      'close, reconnect on a fresh transport', () async {
    final bridges = <Bridge>[];
    final c = MqttClient(
      host: 'unused',
      clientId: 'bridge',
      reconnectManager: fastReconnect(),
      transportFactory: () {
        final b = Bridge(proxy.port, wholeBuffer: bridges.length.isOdd);
        bridges.add(b);
        return b.transport;
      },
    );
    await c.connect();
    final inbox = Inbox(c);
    await c.subscribe('br/t',
        options: const MqttSubscriptionOptions(qos: MqttQos.exactlyOnce));
    await c.publish('br/t', bytes('one'), qos: MqttQos.exactlyOnce);
    await inbox.waitFor(1);

    // The broker side goes away: the bridge reports it with injectDone.
    proxy.cutAll();
    await waitUntil(
        () => bridges.length >= 2 && c.state == MqttConnectionState.connected);
    expect(bridges.first.transport.isClosed, isTrue);
    await c.publish('br/t', bytes('two'), qos: MqttQos.atLeastOnce);
    await inbox.waitFor(2);

    // A transport-level error is reported with injectError.
    bridges.last.transport.injectError(const SocketException('link failed'));
    await waitUntil(
        () => bridges.length >= 3 && c.state == MqttConnectionState.connected);
    await c.close();
    for (final b in bridges) {
      b.stop();
    }
  });

  test(
      'MemoryTransport contract: connect after close, add while '
      'disconnected, injections after close are ignored', () async {
    final t = MemoryTransport();
    expect(t.isConnected, isFalse);
    expect(() => t.add(Uint8List(1)), throwsA(isA<MqttTransportException>()));
    await t.connect();
    expect(t.isConnected, isTrue);
    t.add(Uint8List.fromList([1, 2]));
    expect(t.outgoing, hasLength(1));
    await t.flush();
    await t.close();
    expect(t.isClosed, isTrue);
    t.injectError(StateError('ignored'));
    t.injectDone();
    await expectLater(t.connect(), throwsStateError);
  });
}
