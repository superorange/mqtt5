import 'dart:typed_data';

import 'package:mqtt5/src/client/mqtt_authenticator.dart';
import 'package:mqtt5/src/client/mqtt_client.dart';
import 'package:mqtt5/src/client/mqtt_connection_state.dart';
import 'package:mqtt5/src/exception/mqtt_exception.dart';
import 'package:mqtt5/src/packet/auth.dart';
import 'package:mqtt5/src/packet/connack.dart';
import 'package:mqtt5/src/packet/connect.dart';
import 'package:mqtt5/src/packet/mqtt_packet.dart';
import 'package:mqtt5/src/packet/mqtt_packet_codec.dart';
import 'package:mqtt5/src/packet/mqtt_reason_code.dart';
import 'package:mqtt5/src/property/mqtt_property.dart';
import 'package:mqtt5/src/transport/memory_transport.dart';
import 'package:test/test.dart';

void main() {
  test('enhanced authentication handshake', () async {
    final transport = MemoryTransport();
    final authenticator = _EchoAuthenticator();
    final client = MqttClient(
      host: 'h',
      transportFactory: () => transport,
      authenticator: authenticator,
    );

    final connectFuture = client.connect(
      authenticationMethod: 'SCRAM',
      authenticationData: Uint8List.fromList([0x01]),
    );

    final connect = await _nextPacket(transport) as MqttConnectPacket;
    expect(connect.properties, contains(const AuthenticationMethod('SCRAM')));
    expect(
      connect.properties,
      contains(isA<AuthenticationData>()),
    );

    transport.inject(
      MqttPacketCodec.encode(
        MqttAuthPacket(
          reasonCode: MqttReasonCode.continueAuthentication,
          properties: [
            const AuthenticationMethod('SCRAM'),
            AuthenticationData([0x01]),
          ],
        ),
      ),
    );

    final auth = await _nextPacket(transport) as MqttAuthPacket;
    expect(auth.reasonCode, MqttReasonCode.continueAuthentication);
    expect(
      auth.properties,
      contains(isA<AuthenticationData>()),
    );

    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(sessionPresent: false),
      ),
    );
    await connectFuture;
    expect(client.state, MqttConnectionState.connected);
    expect(authenticator.calls, 1);

    await client.disconnect();
  });

  test('CONNACK continue-authentication reason is a protocol error', () async {
    final transport = MemoryTransport();
    final client = MqttClient(
      host: 'h',
      transportFactory: () => transport,
      autoReconnect: false,
    );

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      Uint8List.fromList([0x20, 0x03, 0x00, 0x18, 0x00]),
    );

    await expectLater(
      connectFuture,
      throwsA(isA<MqttProtocolException>()),
    );
    expect(client.state, MqttConnectionState.disconnected);
  });

  test('fatal CONNACK rejection surfaces without retrying', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          reasonCode: MqttReasonCode.notAuthorized,
        ),
      ),
    );

    await expectLater(
      connectFuture,
      throwsA(isA<MqttServerRejectedException>()),
    );
    expect(client.state, MqttConnectionState.disconnected);
  });

  test('server moved raises MqttServerMovedException', () async {
    final transport = MemoryTransport();
    final client = MqttClient(host: 'h', transportFactory: () => transport);

    final connectFuture = client.connect();
    await _nextPacket(transport);
    transport.inject(
      MqttPacketCodec.encode(
        const MqttConnackPacket(
          sessionPresent: false,
          reasonCode: MqttReasonCode.useAnotherServer,
          properties: [ServerReference('other.example.com:1883')],
        ),
      ),
    );

    await expectLater(
      connectFuture,
      throwsA(
        isA<MqttServerMovedException>().having(
          (e) => e.serverReference,
          'serverReference',
          'other.example.com:1883',
        ),
      ),
    );
  });
}

final class _EchoAuthenticator implements MqttAuthenticator {
  int calls = 0;

  @override
  Future<MqttAuthResponse?> authenticate(MqttAuthChallenge challenge) async {
    calls++;
    return MqttAuthResponse(
      Uint8List.fromList([0x02, 0x02]),
    );
  }
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
