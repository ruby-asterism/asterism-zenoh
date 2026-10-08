# zenoh-c feature coverage of asterism-zenoh

Which zenoh-c 1.10.1 features the Ruby binding exposes (asterism-zenoh 0.2.0).
"Yes" = usable from Ruby, "Partial" = usable with fixed settings or a subset,
"No" = not exposed yet. The Ruby-like layer of the `asterism` gem builds on
the same methods. The mruby gem (picoruby-asterism-zenoh) has the same
Ruby API; its column says whether zenoh-pico could offer the feature.

| Area | zenoh-c | asterism-zenoh (CRuby) | Notes | Possible on the boards (zenoh-pico) |
|---|---|---|---|---|
| Session: client / peer / listen | `z_open` | Yes | `Session.open(loc, mode:, listen:)` | Yes (done) |
| Session configuration | `z_config_*`, `zc_config_insert_json5`, config files | Partial | Only the settings the binding needs are set internally (connect / listen, scouting off, timeouts). No way to pass your own configuration yet, so TLS / QUIC / WebSocket links, authentication and timeouts are not reachable | Partial (no TLS on ESP32) |
| Scouting (finding peers / routers) | `z_scout`, multicast scouting | No | Disabled to match the boards | Yes (disabled on purpose) |
| Session information | `z_info_zid`, `z_info_peers_zid`, `z_info_routers_zid` | Partial | `zid`, `peers` (count) | Yes |
| put | `z_put` | Partial | payload (String) and attachment. Encoding, priority, congestion control, express, reliability, timestamp and destination are fixed | Partial |
| delete | `z_delete` | No | | Yes |
| Declared publisher | `z_declare_publisher`, `z_publisher_put` | No | `put` only | Yes |
| Matching status (is anyone listening?) | `z_publisher_declare_matching_listener`, `z_publisher_get_matching_status` | No | | Partial |
| Subscriber | `z_declare_subscriber` + FIFO channel | Yes | key, payload, attachment; block / Enumerator API in `asterism`. Encoding, timestamp, priority and sample kind of a sample are not exposed | Yes (done) |
| get | `z_get` | Yes | timeout, parameters, payload, attachment, target, consolidation. Encoding not exposed | Yes (done) |
| Declared querier | `z_declare_querier`, `z_querier_get` | No | `get` only | Yes |
| Queryable | `z_declare_queryable`, `z_query_reply` | Partial | complete, reply with attachment. `z_query_reply_err` and `z_query_reply_del` not exposed | Partial (done for the exposed part) |
| Liveliness | tokens, subscriber, get | Yes | `liveliness`, `liveliness_watch`, `liveliness_get` | Yes (done) |
| Encoding (content types) | `z_encoding_*` | No | Payloads are bytes; Asterism uses MessagePack / CDR on top | Yes |
| Timestamps (HLC) | `z_timestamp_*`, `z_sample_timestamp` | No | | Partial |
| Key expression operations | `z_keyexpr_intersects`, `includes`, `join`, `concat`, declared key expressions | No | Key expressions are plain Strings | Yes |
| Serialization helpers | `ze_serialize_*`, `ze_deserialize_*` | No | Not needed: MessagePack (objects) and CDR (ROS 2) are used | Partial |
| Advanced publisher / subscriber (history, recovery, publisher detection) | `ze_declare_advanced_publisher`, `ze_declare_advanced_subscriber` | No | Needed for ROS 2 transient-local QoS | Partial (RAM to be measured) |
| Publication cache / querying subscriber | `ze_declare_publication_cache`, `ze_declare_querying_subscriber` | No | Older form of the above | Partial |
| Transport / link events | `z_declare_transport_events_listener`, `z_declare_link_events_listener` | No | Disconnects are detected through `poll` instead | No |
| Shared memory | `z_shm_*` | No | Same-host only | No |
| Background declarations | `z_declare_background_*` | No | Not needed; objects are closed when freed | Partial |
| Logging | `zc_init_log*` | No | | No |

Notes:

- The prebuilt zenoh-c that `gem install` downloads contains all of the above
  (the "unstable" API included), so exposing a feature needs only binding work.
- The most useful next step is passing a user configuration (JSON5 or a file)
  to `Session.open`: it unlocks TLS / QUIC / WebSocket links, authentication and
  multicast scouting without new methods.
