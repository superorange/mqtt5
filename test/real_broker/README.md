# Real-broker test suite

Every test here talks to a real MQTT 5 server over real sockets. Nothing is
mocked; `MemoryTransport` is never used.

* **mosquitto 2.x** is started per test or per group on a free port with a
  generated config. Its `log_type all` output and the third-party
  `mosquitto_sub` / `mosquitto_pub` clients are used as independent witnesses
  of what crossed the wire. `test/support/mosquitto_tools.dart` locates the
  installation — from `PATH` and the usual Homebrew and distro locations, or
  from `MQTT5_MOSQUITTO_PREFIX` when the layout is neither.
* **EMQX 5.8.6** (`emqx/emqx:5.8.6`, Docker) is a second, independent server
  implementation. Containers are named `mqtt5-test-emqx-*`, labelled
  `owner=mqtt5-tests`, bound to `127.0.0.1` only and removed afterwards.
* `support/fault_proxy.dart` sits between client and broker to drop, delay,
  black-hole, cut, rewrite or inject packets. It parses frames with its own
  minimal reader (`support/wire.dart`), never with the library's codec.
* `support/plugin/ext_auth.c` is a mosquitto plugin implementing two enhanced
  authentication methods (compiled with `cc` at test time).

| file | covers |
| --- | --- |
| `connect_test.dart` | CONNECT/CONNACK, capabilities, keep alive, credentials, sessions |
| `pubsub_test.dart` | QoS flows, properties, subscription options, aliases, flow control |
| `session_fault_test.dart` | retransmission, resume, broker restarts, dead links |
| `will_test.dart` | Will message variants |
| `auth_test.dart` | enhanced authentication and re-authentication |
| `tls_test.dart` | TLS, mutual TLS, certificate failures |
| `emqx_test.dart` | cross-validation on EMQX |
| `wire_fault_test.dart` | how the client handles a broker that breaks the protocol |
| `api_codec_test.dart` | API validation, codec in the server role, topic matching |
| `known_bugs_test.dart` | open defects (fail until fixed) |

Run (serially — several tests are timing sensitive):

```sh
fvm dart test test/real_broker -j 1                 # everything
fvm dart test test/real_broker -j 1 -x known-bug    # expected to be green
fvm dart test test/real_broker -j 1 -t known-bug    # open defects only
```
