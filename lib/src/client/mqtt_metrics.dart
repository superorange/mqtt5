/// Runtime counters exposed by the client for diagnostics.
final class MqttMetrics {
  int bytesSent = 0;
  int bytesReceived = 0;
  int packetsSent = 0;
  int packetsReceived = 0;
  int messagesPublished = 0;
  int messagesReceived = 0;
  int reconnectCount = 0;
  int protocolErrorCount = 0;

  /// Duration of the most recent PINGREQ/PINGRESP exchange.
  Duration? lastPingRtt;

  @override
  String toString() =>
      'MqttMetrics(sent: $bytesSentB, received: $bytesReceivedB, '
      'packets: $packetsSent/$packetsReceived, '
      'messages: $messagesPublished/$messagesReceived, '
      'reconnects: $reconnectCount, protocolErrors: $protocolErrorCount, '
      'pingRtt: $lastPingRtt)';

  String get bytesSentB => _human(bytesSent);

  String get bytesReceivedB => _human(bytesReceived);

  static String _human(int bytes) {
    if (bytes < 1024) {
      return '$bytes B';
    }
    if (bytes < 1024 * 1024) {
      return '${(bytes / 1024).toStringAsFixed(1)} KB';
    }
    return '${(bytes / (1024 * 1024)).toStringAsFixed(1)} MB';
  }
}
