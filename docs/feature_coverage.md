# zenoh-c feature coverage of asterism-zenoh

Which zenoh-c 1.10.1 features the Ruby binding exposes (asterism-zenoh 0.3.0).
"Yes" = usable from Ruby, "Partial" = usable with fixed settings or a subset,
"No" = not exposed, with the reason. The Ruby-like layer of the `asterism`
gem builds on the same methods. The mruby gem (picoruby-asterism-zenoh) has
the 0.2.0 part of the API; its column says whether zenoh-pico could offer
the feature. Everything added in 0.3.0 is CRuby only for now.

| Area | zenoh-c | asterism-zenoh (CRuby) | Ruby | Possible on the boards (zenoh-pico) |
|---|---|---|---|---|
| Session: client / peer / listen | `z_open` | Yes | `Session.open(loc, mode:, listen:)` | Yes (done) |
| Session configuration | `zc_config_insert_json5`, `zc_config_from_str`, `zc_config_from_file` | Yes (0.3.0) | `config: {"key/path" => value}` (Ruby values sent as JSON), `config: "<JSON5>"`, `config_file:`. TLS, QUIC, WebSocket, authentication and timeouts are reachable this way; TLS is tested between two sessions | Partial (zenoh-pico has its own, smaller set of keys; no TLS on ESP32) |
| Scouting (finding peers / routers) | `z_scout`, multicast scouting, gossip | Yes (0.3.0) | `Asterism::Zenoh.scout(what:, timeout:)` -> `Hello` (zid, whatami, locators); `Session.open(scouting: true)` | Yes (off on purpose: the locator is given) |
| Session information | `z_info_zid`, `z_info_peers_zid`, `z_info_routers_zid`, `z_info_transports`, `z_info_links` | Yes (0.3.0) | `zid`, `peers` (count, as before), `peer_zids`, `router_zids`, `transports`, `links` | Yes (zid lists); transports / links: No |
| put | `z_put` | Yes (0.3.0) | `put(key, payload, attachment:, encoding:, priority:, congestion_control:, express:, reliability:, timestamp:, allowed_destination:)` | Partial (encoding, priority, congestion control, express: yes) |
| delete | `z_delete` | Yes (0.3.0) | `session.delete(key, ...)` | Yes |
| Declared publisher | `z_declare_publisher`, `z_publisher_put`, `z_publisher_delete` | Yes (0.3.0) | `session.publisher(key, ...)` -> `put`, `delete`, `close` | Yes |
| Matching status (is anyone listening?) | `z_*_get_matching_status`, `z_*_declare_matching_listener` | Yes (0.3.0) | `matching?`, `matching_listener` -> `MatchingListener#each_pending` (polled) | Partial (zenoh-pico has it behind a build option) |
| Subscriber | `z_declare_subscriber` + FIFO channel | Yes | `each_pending { \|k, v, a\| }` as before; `each_sample` -> `Sample` (kind, encoding, timestamp, priority, congestion_control, express, reliability, source_zid) | Yes (done; the fields: Partial) |
| get | `z_get` | Yes | timeout, parameters, payload, attachment, target, consolidation; 0.3.0: encoding, priority, congestion control, express, accept_replies; `each_result` -> `Reply` with error replies | Yes (done) |
| Declared querier | `z_declare_querier`, `z_querier_get` | Yes (0.3.0) | `session.querier(key, target:, consolidation:, timeout_ms:, ...)` -> `get(params, payload, attachment:, encoding:)`, `matching?`, `matching_listener` | Yes |
| Queryable | `z_declare_queryable`, `z_query_reply`, `z_query_reply_err`, `z_query_reply_del` | Yes (0.3.0) | `reply(..., encoding:, timestamp:, priority:, congestion_control:, express:)`, `reply_err(payload, encoding:)`, `reply_del(key)`, `query.encoding` | Yes |
| Liveliness | tokens, subscriber, get | Yes | `liveliness`, `liveliness_watch`, `liveliness_get` | Yes (done) |
| Encoding (content types) | `z_encoding_from_str`, `z_encoding_to_string` | Yes (0.3.0) | Strings ("application/json", "text/plain;charset=utf-8"); default "zenoh/bytes" | Yes |
| Timestamps (HLC) | `z_timestamp_new`, `z_sample_timestamp`, timestamping | Yes (0.3.0) | `session.new_timestamp`, `Timestamp` (`ntp64`, `id`, `to_time`, Comparable), `put(timestamp: true)`, `Session.open(timestamping: true)` | Partial |
| Key expression operations | `z_keyexpr_intersects`, `includes`, `relation_to`, `join`, `concat`, `canonize`, `z_declare_keyexpr` | Yes (0.3.0) | `KeyExpr.new(str, autocanonize:)`, `intersects?`, `includes?`, `relation_to`, `join`, `concat`, `KeyExpr.canonize`, `KeyExpr.valid?`; `session.declare_keyexpr(key)`. A KeyExpr goes wherever a key String does | Yes |
| Advanced publisher / subscriber | `ze_declare_advanced_publisher`, `ze_declare_advanced_subscriber`, publisher detection, sample miss listener | Yes (0.3.0) | `advanced_publisher(key, cache:, sample_miss_detection:, publisher_detection:)`, `advanced_subscriber(key, history:, recovery:, subscriber_detection:)`, `detect_publishers`, `miss_listener`. A late rmw_zenoh subscriber with transient-local durability got the cached values | Partial (RAM to be measured) |
| Transport / link events | `z_declare_transport_events_listener`, `z_declare_link_events_listener` | Yes (0.3.0) | `session.transport_events(history:)`, `link_events` -> `EventListener#each_pending` (polled) of `TransportEvent` / `LinkEvent` | No |
| Logging | `zc_init_log_from_env_or`, `zc_try_init_log_from_env` | Yes (0.3.0) | `Asterism::Zenoh.init_log(level)`; written to standard output by zenoh-c; `RUST_LOG` wins | No |
| Error text | `zc_get_last_error` | Yes (0.3.0) | appended to the `Asterism::Zenoh::Error` messages of failed opens and declarations | No |
| Serialization helpers | `ze_serialize_*`, `ze_deserialize_*` | No | Not needed: MessagePack (objects) and CDR (ROS 2) are used; payloads stay Strings | Partial |
| Publication cache / querying subscriber | `ze_declare_publication_cache`, `ze_declare_querying_subscriber` | No | The older form of the advanced publisher / subscriber, which covers it | Partial |
| Shared memory | `z_shm_*` | No | Same-host only, and a Ruby String is copied anyway, so it would gain nothing | No |
| Background declarations | `z_declare_background_*` | No | Not needed: every object is closed by `close`, by its session or when it is freed | Partial |
| Cancellation of gets, source info on puts | `cancellation_token`, `source_info` | No | Rarely needed; a get ends by its time limit | No |

How receiving works, for all of the above: zenoh-c's callbacks copy what
arrives into a queue of plain C memory (no Ruby, no GVL), dropping the
oldest entry when it is full; Ruby takes the entries out by polling
(`each_pending`, `each_sample`, `each_result`). This holds for the matching
listeners, the transport / link events and the missed samples too.

Notes:

- The prebuilt zenoh-c that `gem install` downloads contains all of the above
  (the "unstable" API included).
- An advanced publisher with a cache and without `sample_miss_detection`
  needs a session with timestamping (`Session.open(..., timestamping: true)`).
- For ROS 2 transient-local, publish on the rmw_zenoh topic key with an
  advanced publisher (`cache:`, `publisher_detection: true`,
  `sample_miss_detection: true`) and declare the liveliness token with
  durability transient local in its QoS (`":1:,10:,:,:,,"`).
  `Asterism::ROS` does not do this by itself yet.
