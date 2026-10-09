import 'dart:convert';
import 'dart:io';

import 'socket_transport.dart';

/// A TLS transport backed by [SecureSocket].
final class TlsTransport extends MqttSocketTransport {
  TlsTransport({
    required super.host,
    required super.port,
    super.timeout,
    super.sourceAddress,
    this.securityContext,
    this.onBadCertificate,
    this.supportedProtocols,
  });

  final SecurityContext? securityContext;
  final bool Function(X509Certificate certificate)? onBadCertificate;
  final List<String>? supportedProtocols;

  /// Connects and completes the TLS handshake within [timeout].
  ///
  /// [SecureSocket.connect] applies its `timeout` to the TCP connect only: the
  /// handshake that follows is unbounded, so a peer that accepts the socket and
  /// then stalls after ClientHello would hang the caller forever. Connecting
  /// and securing in two steps puts both legs under one deadline, and lets the
  /// source address actually take effect — `SecureSocket.connect` has no
  /// parameter for it, so the base class field was silently ignored.
  @override
  Future<Socket> openSocket() async {
    final deadline = DateTime.now().add(timeout);
    final source = sourceAddress;
    final socket = await Socket.connect(
      host,
      port,
      timeout: timeout,
      sourceAddress: source != null ? InternetAddress(source) : null,
    );

    // If the TCP connect used up the whole budget, the zero timeout below
    // fails the handshake straight away through the same path.
    final left = deadline.difference(DateTime.now());
    final remaining = left.isNegative ? Duration.zero : left;

    try {
      return await SecureSocket.secure(
        socket,
        host: host,
        context: securityContext,
        onBadCertificate: onBadCertificate,
        supportedProtocols: supportedProtocols,
      ).timeout(remaining, onTimeout: () {
        // The handshake is abandoned; drop the socket so the fd is not leaked.
        socket.destroy();
        throw SocketException(
          'TLS handshake timed out after ${timeout.inMilliseconds} ms',
          port: port,
        );
      });
    } on Object {
      socket.destroy();
      rethrow;
    }
  }

  /// Builds a [SecurityContext] from PEM-encoded material.
  ///
  /// [trustedCertificates] is the PEM bundle used to verify the server.
  /// [certificateChain] and [privateKey] are the client certificate chain and
  /// private key for mutual TLS.
  static SecurityContext createSecurityContext({
    String? trustedCertificates,
    String? certificateChain,
    String? privateKey,
    String? keyPassword,
  }) {
    if ((certificateChain == null) != (privateKey == null)) {
      throw ArgumentError(
        'certificateChain and privateKey must be provided together',
      );
    }
    if (keyPassword != null && privateKey == null) {
      throw ArgumentError.value(
        keyPassword,
        'keyPassword',
        'Requires privateKey',
      );
    }
    final context =
        SecurityContext(withTrustedRoots: trustedCertificates == null);
    if (trustedCertificates != null) {
      context.setTrustedCertificatesBytes(
        _asBytes(trustedCertificates),
      );
    }
    if (certificateChain != null) {
      context.useCertificateChainBytes(_asBytes(certificateChain));
    }
    if (privateKey != null) {
      context.usePrivateKeyBytes(
        _asBytes(privateKey),
        password: keyPassword,
      );
    }
    return context;
  }

  /// PEM is ASCII armour, but the surrounding file can carry UTF-8 comments or
  /// non-ASCII subject lines. [String.codeUnits] would hand those to BoringSSL
  /// as UTF-16 units, so encode properly instead.
  static List<int> _asBytes(String pem) => utf8.encode(pem);
}
