你是一名资深 Dart、网络协议、TCP/IP、MQTT 5.0 工程师。

你的任务是：**从零实现一个生产级、完整 MQTT 5.0 客户端库，使用纯 Dart，不依赖现有 MQTT Dart 客户端库。**

目标不是 Demo，不是“能 publish/subscribe 就算完成”，而是实现一个可以长期用于真实设备控制、IoT、桌面客户端、后台服务的高可靠 MQTT 5.0 Client。

---

# 1. 核心目标

实现一个新的 Dart package，例如：

```text
mqtt5/
```

核心要求：

* Pure Dart
* 不依赖 Flutter
* TCP 使用 `dart:io Socket`
* TLS 使用 `dart:io SecureSocket`
* MQTT 5.0 为第一优先级
* 架构必须允许以后增加 WebSocket Transport
* Codec 与 Transport 完全解耦
* Protocol 与 Client 状态机解耦
* 完整支持 MQTT 5 Properties
* 完整支持 QoS 0 / 1 / 2
* 完整支持 Session Resume
* 完整实现 MQTT 5 Flow Control
* 正确实现 Receive Maximum
* 正确实现 Maximum Packet Size
* 正确实现 Topic Alias
* 正确实现 Subscription Identifier
* 正确实现所有 Reason Code
* 自动重连
* 自动恢复 Session
* 自动恢复 inflight packet
* 自动重新订阅应遵循 MQTT Session 语义，而不是简单无脑 resubscribe
* 支持 Keep Alive / PINGREQ / PINGRESP
* 支持 Last Will
* 支持 Enhanced Authentication / AUTH
* 支持 User Properties
* 支持 Request/Response pattern 所需属性
* 高并发安全
* 长时间运行稳定
* 严格处理异常 packet、断网、半开 TCP、Broker 重启

不要为了快速实现而删除 MQTT 5 的复杂部分。

---

# 2. 标准依据

协议实现以：

```text
OASIS MQTT Version 5.0
```

规范为唯一协议行为依据。

禁止凭记忆猜测协议。

涉及以下内容时必须逐项核对 MQTT 5.0 specification：

* Packet 格式
* Flags
* Remaining Length
* Variable Byte Integer
* Property Identifier
* Property 数据类型
* Property 可以出现在哪种 Packet
* Property 是否允许重复
* Reason Code
* QoS 状态迁移
* Session 状态
* DUP 行为
* Packet Identifier 生命周期
* Receive Maximum
* Maximum QoS
* Retain Available
* Topic Alias
* Maximum Packet Size
* Server Keep Alive
* Session Expiry
* Subscription Identifier
* Shared Subscription
* Wildcard Subscription
* Server Reference
* Enhanced Authentication

任何规范不确定的地方，不允许自行猜测。

---

# 3. 不允许直接依赖以下库

禁止把以下包作为 MQTT 协议实现：

```text
mqtt_client
mqtt5_client
libmosquitto
Paho MQTT
```

可以阅读它们用于：

* 对比 API
* 查找测试思路
* interoperability 验证

但核心 MQTT protocol implementation 必须由本项目自己实现。

不要复制第三方代码。

---

# 4. 推荐架构

至少拆成以下层级：

```text
lib/
├── mqtt5.dart
│
└── src/
    ├── codec/
    │   ├── mqtt_reader.dart
    │   ├── mqtt_writer.dart
    │   ├── variable_byte_integer.dart
    │   ├── mqtt_utf8.dart
    │   ├── binary_data.dart
    │   └── property_codec.dart
    │
    ├── packet/
    │   ├── mqtt_packet.dart
    │   ├── connect.dart
    │   ├── connack.dart
    │   ├── publish.dart
    │   ├── puback.dart
    │   ├── pubrec.dart
    │   ├── pubrel.dart
    │   ├── pubcomp.dart
    │   ├── subscribe.dart
    │   ├── suback.dart
    │   ├── unsubscribe.dart
    │   ├── unsuback.dart
    │   ├── pingreq.dart
    │   ├── pingresp.dart
    │   ├── disconnect.dart
    │   └── auth.dart
    │
    ├── property/
    │   ├── mqtt_property.dart
    │   ├── property_identifier.dart
    │   └── ...
    │
    ├── transport/
    │   ├── mqtt_transport.dart
    │   ├── tcp_transport.dart
    │   └── tls_transport.dart
    │
    ├── session/
    │   ├── mqtt_session.dart
    │   ├── inflight_store.dart
    │   ├── packet_identifier_pool.dart
    │   ├── outgoing_qos1.dart
    │   ├── outgoing_qos2.dart
    │   ├── incoming_qos2.dart
    │   └── subscription_store.dart
    │
    ├── client/
    │   ├── mqtt_client.dart
    │   ├── connection_manager.dart
    │   ├── keep_alive_manager.dart
    │   ├── reconnect_manager.dart
    │   └── flow_controller.dart
    │
    └── exception/
        └── mqtt_exception.dart
```

实际结构可以优化，但必须保持以下边界：

```text
Transport
   ↓
Byte Stream
   ↓
Codec
   ↓
MQTT Packet
   ↓
Protocol / Session State Machine
   ↓
Public Client API
```

不能把 Socket、Packet decode、Session、业务 callback 全塞在一个巨大 Client class 中。

---

# 5. Transport 设计

设计：

```dart
abstract interface class MqttTransport {
  Stream<Uint8List> get incoming;

  Future<void> connect();

  void add(Uint8List data);

  Future<void> flush();

  Future<void> close();

  bool get isConnected;
}
```

第一阶段实现：

```text
TcpTransport
TlsTransport
```

为以后：

```text
WebSocketTransport
MemoryTransport
MockTransport
```

保留能力。

Codec 不允许知道底层是不是 TCP。

---

# 6. TCP Byte Stream 必须正确处理

TCP 没有 packet boundary。

必须正确处理：

```text
一次 socket event 只收到半个 MQTT packet

一次 socket event 收到：
packet A + packet B

一次 socket event 收到：
packet A + packet B + 半个 packet C

Remaining Length 跨多个 TCP chunk
```

必须实现增量 decoder。

绝对禁止假设：

```dart
socket.listen((data) {
  decodeOnePacket(data);
});
```

一段 Socket data 就等于一个 MQTT Packet。

---

# 7. Remaining Length

完整实现 MQTT Variable Byte Integer：

```text
0 - 268435455
```

包括：

* encode
* decode
* incomplete input
* malformed encoding
* 超过 4 字节
* 非法 value

写完整单元测试。

---

# 8. MQTT UTF-8

MQTT UTF-8 String 不能简单等同于：

```dart
utf8.decode()
```

必须根据 MQTT 5.0 UTF-8 rules 做合法性检查。

至少处理：

* malformed UTF-8
* null character
* prohibited Unicode code points
* surrogate
* control character 相关规范要求

错误必须转换为正确的 Protocol Error / Malformed Packet 行为。

---

# 9. Packet Model

实现 MQTT 5 全部 Control Packet：

```text
CONNECT
CONNACK

PUBLISH

PUBACK
PUBREC
PUBREL
PUBCOMP

SUBSCRIBE
SUBACK

UNSUBSCRIBE
UNSUBACK

PINGREQ
PINGRESP

DISCONNECT
AUTH
```

Fixed Header flags 必须严格验证。

例如：

```text
PUBREL
SUBSCRIBE
UNSUBSCRIBE
```

固定 flags 不能接受非法值。

---

# 10. MQTT 5 Properties

不要做成简单：

```dart
Map<int, dynamic>
```

应该提供强类型模型。

例如：

```dart
sealed class MqttProperty {}

final class SessionExpiryInterval extends MqttProperty {
  final int seconds;
}

final class ReceiveMaximum extends MqttProperty {
  final int value;
}

final class UserProperty extends MqttProperty {
  final String name;
  final String value;
}
```

必须完整覆盖 MQTT 5 Properties，包括但不限于：

```text
Payload Format Indicator
Message Expiry Interval
Content Type
Response Topic
Correlation Data
Subscription Identifier
Session Expiry Interval
Assigned Client Identifier
Server Keep Alive
Authentication Method
Authentication Data
Request Problem Information
Will Delay Interval
Request Response Information
Response Information
Server Reference
Reason String
Receive Maximum
Topic Alias Maximum
Topic Alias
Maximum QoS
Retain Available
User Property
Maximum Packet Size
Wildcard Subscription Available
Subscription Identifier Available
Shared Subscription Available
```

需要建立 Property metadata：

```text
identifier
type
allowedPackets
repeatable
validation
```

不要在几十个 decoder 中手写重复 switch。

可以通过 declarative metadata 驱动 codec 和 validation。

---

# 11. CONNECT

完整支持：

```text
Client Identifier
Clean Start
Keep Alive
Username
Password

Will Flag
Will QoS
Will Retain

Will Properties

Session Expiry Interval
Receive Maximum
Maximum Packet Size
Topic Alias Maximum
Request Response Information
Request Problem Information
User Property
Authentication Method
Authentication Data
```

---

# 12. CONNACK

必须处理：

```text
Session Present
Reason Code
Assigned Client Identifier
Server Keep Alive
Receive Maximum
Maximum QoS
Retain Available
Maximum Packet Size
Assigned Client ID
Topic Alias Maximum
Wildcard Subscription Available
Subscription Identifier Available
Shared Subscription Available
Server Reference
Authentication Method
Authentication Data
User Property
Reason String
```

注意：

不能只是把这些值解析出来。

必须真正让它们影响客户端行为。

---

# 13. Receive Maximum

这是重点。

严格按照 MQTT 5 实现 outgoing QoS1 / QoS2 flow control。

假设 Broker：

```text
Receive Maximum = 10
```

最多只能同时存在 10 个：

```text
尚未完成 ACK 流程的 QoS1 / QoS2 PUBLISH
```

超过的 publish 必须进入发送队列。

例如：

```text
PUBLISH 1
...
PUBLISH 10

等待 ACK

PUBACK 1
↓
允许 PUBLISH 11
```

QoS0 不受该限制。

本地也需要正确处理 Client 自己声明给 Broker 的 Receive Maximum。

必须写：

```text
Receive Maximum = 1
```

这种极端测试。

---

# 14. Packet Identifier Pool

Packet Identifier：

```text
1 - 65535
```

必须：

* 正确分配
* inflight 未结束前不能重复
* ACK 完成后释放
* reconnect/session resume 后正确恢复
* exhaustion 时等待，而不是产生重复 ID

单独实现：

```dart
PacketIdentifierPool
```

不要散落在 publish/subscribe 中。

---

# 15. QoS 0

实现：

```text
PUBLISH
```

无需 packet identifier。

正确处理 retain / properties。

---

# 16. QoS 1

完整状态：

```text
PUBLISH
   ↓
等待 PUBACK
```

处理：

* Packet Identifier
* DUP
* reconnect
* retransmit
* Session resume
* PUBACK Reason Code
* PUBACK Properties

客户端断线后不能简单把所有消息都当新消息重新 publish。

---

# 17. QoS 2

必须严格实现：

```text
PUBLISH QoS2
   ↓
PUBREC
   ↓
PUBREL
   ↓
PUBCOMP
```

建立明确状态机，例如：

```dart
enum OutgoingQos2State {
  publishSent,
  pubRecReceived,
  pubRelSent,
}
```

并实现 incoming QoS2 去重状态。

重点测试：

```text
发送 PUBLISH 后断线
收到 PUBREC 后断线
发送 PUBREL 后断线
收到重复 PUBLISH
收到重复 PUBREL
Session Resume
```

不能因为 reconnect 导致 QoS2 消息重复交付给应用。

---

# 18. DUP

严格按照 MQTT 5 语义设置 DUP。

不能把：

```text
reconnect 后重发
```

统一理解为“全部 DUP=true”。

不同 Packet、不同 QoS、不同 Session 状态必须按照规范实现。

---

# 19. Session

支持：

```text
Clean Start
Session Expiry Interval
Session Present
```

Session 状态至少包含：

```text
subscriptions
outgoing QoS1 inflight
outgoing QoS2 inflight
incoming QoS2 state
packet identifiers
```

要明确区分：

```text
TCP connection
MQTT connection
MQTT session
```

它们不是一回事。

---

# 20. 自动重连

实现生产级 reconnect manager。

支持：

```text
fixed delay
exponential backoff
max delay
jitter
```

例如默认：

```text
1s
2s
4s
8s
16s
30s
30s
...
```

加随机 jitter。

必须处理：

```text
DNS 失败
Connection refused
Connection timeout
TCP reset
Broker restart
Wi-Fi 短暂断网
网卡切换
SecureSocket error
PING timeout
```

---

# 21. 半开 TCP

不能依赖：

```text
Socket.isConnected
```

来判断 Broker 还活着。

必须使用：

```text
MQTT Keep Alive
PINGREQ
PINGRESP
timeout
```

检测 dead connection。

PING 超时后：

```text
close transport
↓
connection state = disconnected
↓
进入 reconnect
```

---

# 22. Keep Alive

必须遵循 MQTT 5 Keep Alive 规则。

支持：

```text
Client Keep Alive
Server Keep Alive
```

Server Keep Alive 如果 CONNACK 指定，需要覆盖客户端原来的 keep alive。

KeepAlive manager 应根据实际 outbound traffic 决定是否需要发送 PINGREQ。

---

# 23. Topic Alias

完整实现：

```text
Topic Alias Maximum
Topic Alias
```

区分：

```text
Client → Server aliases
Server → Client aliases
```

维护独立映射。

严格处理：

```text
alias = 0
超过 maximum
未知 alias
空 topic + alias
```

以及 reconnect 后 alias 生命周期。

---

# 24. Maximum Packet Size

如果 Broker 返回：

```text
Maximum Packet Size
```

客户端不能发送超过限制的 MQTT Packet。

注意限制的是：

```text
整个 MQTT Control Packet
```

而不是只限制 payload。

发送前应能计算编码后的 packet size。

接收方向同样需要保护。

避免恶意 packet 导致巨大内存分配。

---

# 25. Maximum QoS

如果 Broker：

```text
Maximum QoS = 1
```

则客户端禁止发送 QoS2。

Public API 应立即抛出清晰异常。

---

# 26. Retain Available

如果：

```text
Retain Available = 0
```

客户端不能发送：

```text
RETAIN = 1
```

的 PUBLISH。

---

# 27. Subscription Options

完整支持：

```text
QoS
No Local
Retain As Published
Retain Handling
```

API 示例：

```dart
await client.subscribe(
  'device/+/status',
  options: const MqttSubscriptionOptions(
    qos: MqttQos.atLeastOnce,
    noLocal: true,
    retainAsPublished: true,
    retainHandling: MqttRetainHandling.sendAtSubscribe,
  ),
);
```

---

# 28. Subscription Identifier

完整支持：

```text
Subscription Identifier
```

包括：

* SUBSCRIBE 发送
* PUBLISH 接收
* 一个 PUBLISH 对应多个 Subscription Identifier

不要只解析第一个。

---

# 29. Shared Subscription / Wildcard

客户端允许：

```text
$share/group/topic/+
```

但需要根据 CONNACK：

```text
Shared Subscription Available
Wildcard Subscription Available
```

限制客户端行为。

---

# 30. Publish API

建议 API：

```dart
Future<MqttPublishResult> publish(
  String topic,
  Uint8List payload, {
  MqttQos qos = MqttQos.atMostOnce,
  bool retain = false,
  List<MqttProperty> properties = const [],
});
```

对于 QoS1/QoS2：

Future 应能够表示协议 ACK 已完成。

不要让：

```dart
await publish()
```

只代表“写进 Socket buffer”。

需要明确 API 语义。

如果需要，同时提供：

```text
enqueue
sent
acknowledged
```

不同事件。

---

# 31. Incoming Message

设计：

```dart
final class MqttMessage {
  final String topic;
  final Uint8List payload;
  final MqttQos qos;
  final bool retain;
  final bool duplicate;
  final List<MqttProperty> properties;
}
```

提供：

```dart
Stream<MqttMessage> get messages;
```

也可以支持按 subscription 分流，但核心不要与 UI framework 耦合。

---

# 32. Connection State

提供明确状态：

```dart
enum MqttConnectionState {
  disconnected,
  connecting,
  authenticating,
  connected,
  disconnecting,
  reconnecting,
}
```

提供：

```dart
Stream<MqttConnectionState>
```

不能靠 callback 到处维护 bool。

---

# 33. Disconnect

区分：

```text
用户主动 disconnect
网络异常 disconnect
Broker DISCONNECT
protocol error
keepalive timeout
authentication failure
server moved
```

返回：

```text
Reason Code
Reason String
Server Reference
User Properties
```

---

# 34. Server Redirection

处理 MQTT5：

```text
Use Another Server
Server Moved
Server Reference
```

至少完整暴露。

架构允许以后配置：

```text
followServerReference = true/false
```

默认不要未经用户允许无限重定向。

---

# 35. Enhanced Authentication

实现：

```text
Authentication Method
Authentication Data
AUTH packet
Continue Authentication
Re-authenticate
```

把认证过程设计成 callback/interface，而不是硬编码用户名密码。

例如：

```dart
abstract interface class MqttAuthenticator {
  Future<MqttAuthResponse> authenticate(
    MqttAuthChallenge challenge,
  );
}
```

---

# 36. TLS

使用：

```dart
SecureSocket
SecurityContext
```

Public API 支持：

```text
CA
client certificate
private key
SNI
certificate callback
ALPN 如果需要
```

不要自己实现 TLS。

---

# 37. 内存与性能

MQTT packet parser 要避免不必要拷贝。

但：

```text
正确性 > 微优化
```

先保证协议正确。

同时避免：

```text
每收到一个 byte 都创建对象
巨大 payload 多次 copy
无限 buffer
```

要有：

```text
maximumIncomingPacketSize
maximumBufferedBytes
```

保护。

---

# 38. 并发

Dart 单 isolate 下也可能发生 async race。

例如：

```text
publish()
disconnect()
reconnect()
PUBACK arrives
timeout fires
```

状态必须序列化。

考虑内部使用：

```text
event loop / command queue
```

确保协议状态修改有唯一入口。

不要到处异步直接修改：

```text
_inflight
_session
_connection
```

---

# 39. Public API 不暴露内部实现

用户不应该需要操作：

```text
raw Socket
raw packet id
codec buffer
```

建议最终使用体验：

```dart
final client = MqttClient(
  clientId: 'cmd-001',
  host: '192.168.1.100',
  port: 1883,
);

client.messages.listen((message) {
  print(message.topic);
});

await client.connect(
  cleanStart: false,
  keepAlive: const Duration(seconds: 30),
  sessionExpiryInterval: const Duration(hours: 1),
);

await client.subscribe(
  'device/+/status',
  options: const MqttSubscriptionOptions(
    qos: MqttQos.atLeastOnce,
  ),
);

await client.publish(
  'device/U1-001/action',
  utf8.encode('{"action":"pause"}'),
  qos: MqttQos.atLeastOnce,
  properties: [
    MqttUserProperty('traceId', '123'),
  ],
);
```

---

# 40. Error Model

不要只有：

```dart
Exception('error')
```

至少设计：

```text
MqttException
MqttTransportException
MqttProtocolException
MqttMalformedPacketException
MqttConnectionException
MqttAuthenticationException
MqttServerRejectedException
MqttPacketTooLargeException
MqttFlowControlException
```

并保存 MQTT Reason Code。

---

# 41. 单元测试

必须大量写测试。

核心 Codec 的目标是接近：

```text
100% branch coverage
```

尤其：

```text
Variable Byte Integer
Properties
Packet encode/decode
QoS state machine
Packet Identifier
Receive Maximum
Topic Alias
Session
```

禁止：

```text
先把全部代码写完，最后补测试
```

每完成一个协议模块立即测试。

---

# 42. Golden Packet Test

对每种 MQTT packet 建立：

```text
对象
↓
encode
↓
固定 hex bytes
```

和：

```text
固定 hex bytes
↓
decode
↓
对象
```

测试。

例如保存：

```text
CONNECT packet hex
CONNACK packet hex
PUBLISH packet hex
SUBSCRIBE packet hex
```

便于检查协议级回归。

---

# 43. Fragmentation Test

必须测试：

一个 MQTT Packet 的 bytes 按以下方式切碎：

```text
1 byte + rest
2 byte + rest
每次 1 byte
随机 chunk
```

decoder 都必须正确恢复 packet。

同时测试：

```text
100 个 packet 合并到一个 TCP chunk
```

---

# 44. Fuzz Test

为 decoder 增加随机数据测试。

输入：

```text
随机 bytes
损坏 Remaining Length
非法 flags
非法 property
非法 UTF8
截断 packet
巨大长度
重复禁止重复的 property
```

要求：

```text
不能 crash
不能无限循环
不能无限分配内存
```

只能：

```text
返回明确协议错误
```

---

# 45. Broker 集成测试

安装并使用至少：

```text
Mosquitto
```

做真实 interoperability。

如果环境允许，再加入：

```text
EMQX
```

测试。

测试不能只依赖 Mock。

---

# 46. 必须测试以下真实场景

```text
正常连接

Broker 未启动

Broker 启动后连接

连接成功后 Broker kill

Broker restart

TCP 突然断开

连接中途断网

断网后恢复

连接中途切换网卡

客户端 idle

PINGREQ / PINGRESP

Broker 不回应 PINGREQ

QoS1 publish

QoS1 publish 后断线

QoS2 各阶段断线

Session Present = true

Session Present = false

Receive Maximum = 1

大量 QoS1 inflight

大量 QoS2 inflight

Maximum Packet Size

Topic Alias

Subscription Identifier

Last Will

Clean Start true

Clean Start false

Session Expiry = 0

Session Expiry > 0

Authentication failure

Broker DISCONNECT
```

---

# 47. Soak Test

提供一个：

```text
tool/soak_test.dart
```

可以：

```text
持续运行
持续 subscribe
周期性 publish
统计 reconnect
统计 RTT
统计丢包
统计 duplicate
统计 ACK timeout
统计当前 inflight
统计 memory
```

用于：

```text
24 小时
72 小时
7 天
```

长期运行验证。

不要把“运行十分钟没崩”叫稳定。

---

# 48. Chaos Test

提供脚本或测试方法随机执行：

```text
kill broker
restart broker
drop TCP
delay ACK
断开网络
恢复网络
随机 disconnect
```

验证客户端状态机。

---

# 49. 日志

内部日志使用抽象 logger。

至少区分：

```text
trace
debug
info
warning
error
```

可观察：

```text
CONNECT
CONNACK
PUBLISH packet id
PUBACK
QoS2 state transition
reconnect
session resume
flow-control queue
PING
DISCONNECT reason
```

默认不能疯狂打印 payload。

---

# 50. Metrics

内部至少允许暴露：

```text
bytes sent
bytes received

packets sent
packets received

messages published
messages received

reconnect count
ping RTT

outgoing inflight count
queued publish count

protocol error count
```

方便以后生产环境诊断。

---

# 51. README

最终 README 必须包含：

```text
设计目标
支持的平台
MQTT 5 Feature Matrix
Quick Start
TLS
QoS
Session
Reconnect
Properties
Subscription
Troubleshooting
Limitations
```

Feature Matrix 不能虚假标记支持。

只有通过实现和测试的功能才能：

```text
✅ Supported
```

---

# 52. Benchmark

写 benchmark：

```text
Packet encode
Packet decode
small publish
1 KB payload
10 KB payload
100 KB payload
高频 packet parsing
```

先记录数据，不允许为了 benchmark 破坏代码质量。

---

# 53. 实现顺序

不要一次生成整个项目。

严格按阶段：

## Phase 1

```text
package scaffold
core types
byte reader/writer
Variable Byte Integer
UTF8
```

完成并测试。

## Phase 2

```text
Property system
Property codec
Property validation
```

完成并测试。

## Phase 3

```text
所有 Packet encode/decode
```

完成并测试。

## Phase 4

```text
MemoryTransport
incremental packet decoder
```

完成并测试。

## Phase 5

```text
TcpTransport
TlsTransport
```

完成并测试。

## Phase 6

```text
CONNECT
CONNACK
connection state
keepalive
```

与 Mosquitto 联调。

## Phase 7

```text
SUBSCRIBE
UNSUBSCRIBE
PUBLISH QoS0
```

## Phase 8

```text
QoS1
Packet Identifier
```

## Phase 9

```text
QoS2
incoming/outgoing state machine
```

## Phase 10

```text
Session
Reconnect
Session Resume
```

## Phase 11

```text
Receive Maximum
Maximum Packet Size
Maximum QoS
Retain Available
Flow Control
```

## Phase 12

```text
Topic Alias
Subscription Identifier
Shared Subscription
```

## Phase 13

```text
AUTH
Will
Server Redirect
剩余 MQTT5 能力
```

## Phase 14

```text
fuzz
chaos
soak
benchmark
documentation
```

---

# 54. 开发纪律

每个 Phase：

```text
设计
↓
测试
↓
实现
↓
运行测试
↓
与规范核对
↓
提交
```

不要：

```text
一次性写 10000 行
然后统一 debug
```

---

# 55. 每次发现 bug

禁止直接猜 fix。

必须先：

```text
构造最小复现
↓
增加失败测试
↓
定位状态机/协议原因
↓
修复
↓
确认测试通过
↓
确认没有破坏其他状态
```

---

# 56. 不允许“假完成”

以下情况不能标记完成：

```text
能连接 Mosquitto
```

不代表 MQTT5 完成。

```text
能 publish
```

不代表 QoS1 完成。

```text
能收到 PUBREC
```

不代表 QoS2 完成。

```text
能重连
```

不代表 Session Resume 完成。

```text
解析了 Receive Maximum
```

不代表支持 Receive Maximum。

只有真正改变行为并通过测试，才能算支持。

---

# 57. 特别关注现有 Dart MQTT 库容易缺失的问题

我们实现这个库的目的之一，就是避免“API 看起来支持 MQTT5，但实际没有完全执行协议语义”。

因此重点确保：

```text
Receive Maximum 真正 enforce

Maximum Packet Size 真正 enforce

Maximum QoS 真正 enforce

Retain Available 真正 enforce

Topic Alias 真正管理

Session 真正恢复

QoS1/QoS2 inflight 真正恢复

Subscription Identifier 真正支持

Server Keep Alive 真正覆盖客户端 Keep Alive

Reason Code 真正影响行为
```

---

# 58. 稳定性优先级

本项目优先级：

```text
1. 协议正确性
2. 状态机正确性
3. 断线恢复正确性
4. 长期稳定性
5. API 清晰
6. 性能
7. 代码数量
```

禁止为了少写代码牺牲 MQTT5 行为。

---

# 59. 最终目标

最终我们希望得到的不是：

```text
another Dart MQTT library
```

而是：

```text
一个 MQTT 5.0 first-class 的 Dart protocol engine
```

它应该可以承担：

```text
IoT Device Control
Desktop Client
CLI
Backend Service
Embedded Controller Gateway
Long-lived Connection
```

并能够持续运行数天、数周。

---

# 60. 现在开始

先不要直接实现整个 MQTT Client。

第一步：

1. 阅读当前 repository。
2. 如果是空项目，建立 package skeleton。
3. 创建架构设计文档。
4. 将 MQTT5 功能拆成 Feature Matrix。
5. 列出 MQTT5 全部 Control Packets。
6. 列出 MQTT5 全部 Properties。
7. 列出关键状态机。
8. 创建 Phase 1 测试。
9. 从最底层 codec 开始实现。

每个阶段都必须运行：

```text
dart analyze
dart test
```

发现任何 warning/error 必须解决。

不要为了让我确认而停下来。

遇到可以从规范、现有代码、测试、Broker 行为中自行判断的问题，直接调查并继续。

只有存在真正无法从技术事实判断的产品决策时，才需要提出问题。

持续执行直到当前阶段达到：

```text
代码完成
测试通过
analyze 通过
设计与实现一致
```

然后进入下一阶段。

