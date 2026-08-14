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

  @override
  Future<Socket> openSocket() {
    return SecureSocket.connect(
      host,
      port,
      context: securityContext,
      timeout: timeout,
      onBadCertificate: onBadCertificate,
      supportedProtocols: supportedProtocols,
    );
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
    final context = SecurityContext(withTrustedRoots: trustedCertificates == null);
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

  static List<int> _asBytes(String pem) => pem.codeUnits;
}
