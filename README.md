# asterism-zenoh

An unofficial Zenoh binding for CRuby, `Asterism::Zenoh`, over the prebuilt
[zenoh-c](https://github.com/eclipse-zenoh/zenoh-c) 1.10.1. Its Ruby API is
the one of Asterism's mruby / PicoRuby binding (over zenoh-pico), so the
same Ruby code runs on a PC and on the boards.

```ruby
require "asterism/zenoh"

s = Asterism::Zenoh::Session.open("tcp/192.0.2.2:7447")   # client of a router
sub = s.subscribe("demo/in")
loop do
  break unless s.poll                       # false once the connection is lost
  s.put("demo/out", "hello")
  sub.each_pending { |key, payload, attachment| puts "#{key}: #{payload}" }
  sleep 0.05
end
```

## Feature coverage

Supported: sessions (client, peer, listening, multicast scouting), any
zenoh configuration (a Hash of keys, a JSON5 String or file: TLS, QUIC,
WebSocket, authentication, timeouts), put / delete with encoding, priority,
congestion control, express, reliability and timestamps, declared
publishers and queriers with matching status, subscribers with the full
sample (kind, encoding, timestamp, ...), get / queryable with error and
delete replies, liveliness, the advanced publisher / subscriber (history
for late subscribers, recovery, publisher detection; ROS 2 transient
local), transport and link events, key expression operations and declared
key expressions, HLC timestamps, zenoh-c's log.

Not exposed: shared memory, background declarations, zenoh's own
serializer (MessagePack and CDR are used instead), the publication cache /
querying subscriber (the older form of the advanced ones), cancelling a
get.

The table, with the reasons and what the boards' zenoh-pico could offer:
[docs/feature_coverage.md](docs/feature_coverage.md). The calls added in
0.3.0 are CRuby only.

## API

The same calls, arguments and results as the mruby / PicoRuby gem
([picoruby-asterism-zenoh](https://github.com/ruby-asterism/picoruby-asterism-zenoh)):

| Call | Notes |
|---|---|
| `Session.open(locator = nil, mode: :client, listen: nil) { \|s\| }` | client of a router, or `mode: :peer` connecting to `locator` and/or listening on `listen:`. `Error` when nobody answers within `CONNECT_TIMEOUT_MS`. With a block: closes the session after it and returns the block's value. Releases the GVL while connecting |
| `session.put(key, payload, attachment: nil)` | `payload` / `attachment` are Strings (bytes). Releases the GVL |
| `session.subscribe(key, depth: 16)` -> `Subscriber` | `each_pending { \|key, payload, attachment\| }` (or an Array), `pending` / `received` / `dropped`, `close` / `closed?`. `depth` may also be positional |
| `session.get(key, timeout: 2.0, params: nil, payload: nil, attachment: nil, target: :all, consolidation: :none, depth: DEFAULT_GET_DEPTH)` -> `Get` | `timeout:` in seconds, or `timeout_ms:`. `depth:` the replies kept until taken (see Queue depths). Returns at once; `each_reply { \|key, payload, attachment\| }`, `done?`, `pending` / `received` / `dropped` / `errors`. `consolidation: :none` (every reply) differs from zenoh's `:auto` on purpose: services and the object layer want every reply |
| `session.queryable(key, depth: 16, complete: false)` -> `Queryable` | `each_pending { \|q\| }` (each query finished after the block) or an Array of `Query` |
| `q.key` / `params` / `payload` / `attachment`, `q.reply(key, payload, attachment: nil)`, `q.finish` / `finished?` | `q.reply(payload)` answers on the query's key (see Deprecations) |
| `session.liveliness(key)` -> `LivelinessToken` | `close` / `closed?` |
| `session.liveliness_watch(key, depth: DEFAULT_WATCH_DEPTH)` -> `LivelinessWatch` | `each_pending { \|key, alive\| }`, `pending` / `received` / `dropped`; the tokens alive now come first, in one burst |
| `session.liveliness_get(key, timeout: 2.0, depth: DEFAULT_GET_DEPTH)` -> `Get` | or `timeout_ms:` |
| `session.poll(steps = 8)` / `closed?` / `close` / `zid` / `connection_count` | `connection_count`: the routers (client) or peers (peer mode) connected now |
| `Asterism::Error` > `Asterism::Zenoh::Error` > `Asterism::Zenoh::ClosedError` | `ClosedError`: the session is closed or its connection was lost. `Error#code`: zenoh-c's result code when there was one |
| `CONNECT_TIMEOUT_MS`, `SEND_TIMEOUT_MS` (3000), `PEER` / `PEER_SUPPORTED` (true), `MAX_PEERS`, `BACKEND` (`:zenoh_c`), `BACKEND_VERSION` (= `C_VERSION`), `DEFAULT_TIMEOUT` (2.0 s), `DEFAULT_DEPTH` (16), `DEFAULT_GET_DEPTH` / `DEFAULT_WATCH_DEPTH` (1024), `MAX_DEPTH` (65536), `VERSION` | `MAX_PEERS` is zenoh-c's `transport/unicast/max_sessions` (1000); on the boards `BACKEND` is `:zenoh_pico` |
| `Asterism.deprecations = :warn / :raise / :silent` | how deprecated calls are reported (below) |

Units: a keyword without a unit suffix is seconds (`timeout:`,
`connect_timeout:`, `query_timeout:`); anything else carries its unit
(`timeout_ms:`, `CONNECT_TIMEOUT_MS`). Giving a time limit twice
(`timeout:` and `timeout_ms:`) raises `ArgumentError`.

Keys come back as UTF-8 Strings, payloads and attachments as binary
(ASCII-8BIT) Strings.

### Added in 0.3.0 (CRuby only)

Every 0.2.0 call works as before; these are new keywords and methods.

| Call | Notes |
|---|---|
| `Session.open(locator = nil, mode:, listen:, scouting:, timestamping:, config:, config_file:, connect_timeout:)` | `config:` a Hash (`{"transport/link/tls/root_ca_certificate" => "ca.pem"}`, Ruby values sent as JSON) or a JSON5 String; `config_file:` a JSON5 file. Lowest first: zenoh's defaults, the gem's own settings (no scouting, the time limits), the file / String, the arguments, the Hash. `scouting: true` needs no locator. `connect_timeout:` (seconds, 0.4.0) sets `connect/timeout_ms` |
| `Asterism::Zenoh.scout(what: [:router, :peer], timeout: 1.0, config: nil)` (or `timeout_ms:`) | Array of `Hello` (`zid`, `whatami`, `locators`) |
| `session.put(key, payload, attachment:, encoding:, priority:, congestion_control:, express:, reliability:, timestamp:, allowed_destination:)` | `priority:` `:real_time` .. `:background` (or 1..7), `congestion_control:` `:drop` / `:block` / `:block_first`, `reliability:` `:reliable` / `:best_effort`, `timestamp:` `true` or a `Timestamp`, `allowed_destination:` `:any` / `:remote` / `:session_local` |
| `session.delete(key, ...)` | the same options without payload, attachment and encoding |
| `session.publisher(key, encoding:, priority:, ...)` -> `Publisher` | `put(payload, attachment:, encoding:, timestamp:)`, `delete(timestamp:)`, `matching?`, `matching_listener(depth: 16)`, `close` / `closed?` |
| `session.querier(key, target:, consolidation:, timeout: 2.0, ...)` -> `Querier` | `timeout:` seconds or `timeout_ms:`; `get(params: nil, payload: nil, attachment:, encoding:, depth:)` -> `Get`, `matching?`, `matching_listener`, `close` |
| `sub.each_sample { \|sample\| }` | `Sample` (`key`, `payload`, `attachment`, `kind`, `encoding`, `timestamp`, `priority`, `congestion_control`, `express`, `reliability`, `source_zid`); same queue as `each_pending` |
| `get.each_result { \|reply\| }` | `Reply` (`ok?` / `error?`, `key`, `payload`, `encoding`, `kind`, `timestamp`, `replier_zid`); error replies included. `each_reply` still leaves them out |
| `session.get(..., encoding:, priority:, congestion_control:, express:, accept_replies:)` | |
| `q.reply(..., encoding:, timestamp:, priority:, congestion_control:, express:)`, `q.reply_error(payload, encoding:)`, `q.reply_delete(key = nil)`, `q.encoding` | `reply_err` / `reply_del` are the same (kept) |
| `session.advanced_publisher(key, cache:, sample_miss_detection:, publisher_detection:, ...)` -> `AdvancedPublisher` | as `Publisher`. `cache: N` keeps the last N samples for late subscribers |
| `session.advanced_subscriber(key, depth: 16, history:, recovery:, subscriber_detection:, query_timeout:)` -> `AdvancedSubscriber` | as `Subscriber`, plus `detect_publishers` (a `LivelinessWatch`) and `miss_listener` (`Miss`: `source_zid`, `source_eid`, `count`) |
| `session.transport_events(depth: 16, history: false)`, `session.link_events(...)` -> `EventListener` | `each_pending` gives `TransportEvent` / `LinkEvent` (`kind` `:added` / `:removed`, `zid`, ...) |
| `session.peer_zids`, `router_zids`, `transports`, `links` | the IDs, `Transport` and `Link` values connected now |
| `session.new_timestamp` -> `Timestamp` | `ntp64`, `id`, `to_time`, Comparable. From the session's HLC with `timestamping: true` (strictly increasing); otherwise from the system clock, so two in a row may be equal |
| `Asterism::Zenoh::KeyExpr.new(str, autocanonize: false)` | `intersects?`, `includes?`, `relation_to` (`:disjoint` / `:intersects` / `:includes` / `:equals`), `join`, `concat`, `==`; `KeyExpr.canonize(str)`, `KeyExpr.valid?(str)`. Accepted wherever a key String is |
| `session.declare_keyexpr(key)` -> `KeyExpr` | declared on the session (sent as a number afterwards); `undeclare` |
| `Asterism::Zenoh.init_log(level = nil)` | zenoh-c's log on standard output (`"info"`, `"debug"`, or a filter); `RUST_LOG` wins |

Listeners (`MatchingListener`, `EventListener`) are polled like the
subscribers: `each_pending` (yields or returns an Array), `pending`,
`received`, `dropped`, `close` / `closed?`. The values are `Data` objects
(`lib/asterism/zenoh/values.rb`), so they work with pattern matching; the
boolean members also have predicates (`express?`, `multicast?`,
`streamed?`).

```ruby
Z = Asterism::Zenoh
s = Z::Session.open("tls/192.0.2.2:7447",
                    config: { "transport/link/tls/root_ca_certificate" => "ca.pem" })
pub = s.publisher("demo/temp", encoding: "text/plain", priority: :data_high)
watch = pub.matching_listener
sub = s.subscribe("demo/**")
loop do
  watch.each_pending { |listening| puts "listened to: #{listening}" }
  pub.put("21.5", timestamp: true)
  sub.each_sample do |sm|
    case sm
    in {kind: :delete, key:} then puts "#{key} deleted"
    in {encoding: "application/json", payload:} then p payload
    else puts "#{sm.key} at #{sm.timestamp&.to_time}"
    end
  end
  sleep 0.1
end

# A late subscriber gets the last values (ROS 2's transient local works this way)
latched = s.advanced_publisher("demo/mode", cache: 1, sample_miss_detection: true)
latched.put("eco")
late = s.advanced_subscriber("demo/mode", history: true)

Z.scout(what: :peer, timeout: 1.0).each { |h| puts "#{h.zid} #{h.locators}" }
Z::KeyExpr.new("demo/*").includes?("demo/temp")   # => true
```

`require "asterism/zenoh/global"` defines `Zenoh = Asterism::Zenoh` for
those who want the short name; nothing defines it by default.

## Deprecations (0.4.0) and what 1.0 changes

0.4.0 only adds; every 0.3.0 call still works. The old forms below warn
once per name (`warn`; on the boards `puts` when there is no `warn`).
`Asterism.deprecations = :raise` (or `ASTERISM_DEPRECATIONS=raise` in the
environment) raises `Asterism::DeprecationError` instead, which is what
the tests and CI use; `:silent` turns the warnings off.

| Deprecated | Use | 1.0 |
|---|---|---|
| `get(key, timeout_ms, params, payload)` (the time as a positional argument) | `get(key, timeout: 2.0, params:, payload:)` or `timeout_ms:` | removed (positional depth stays) |
| a Float there (`get(key, 2.0)` waits 2 ms) | `timeout: 2.0` | removed; warns with its own message now |
| `liveliness_get(key, timeout_ms)` | `liveliness_get(key, timeout: 1.0)` | removed |
| `session.peers` | `session.connection_count` | removed |
| `q.reply(payload)` on a query whose key differs from the queryable's own plain key | `q.reply(key, payload)` | answers on the queryable's own key when it has no wildcard (as the CRuby block API of the `asterism` gem does) |

Thread-safety: a `Session` and everything declared on it may be used from
several threads (below); each queued entry goes to exactly one taker. A
session opened before `fork` is unusable in the child: its calls raise
`ClosedError` ("open a new session after fork"); the parent's session is
not touched. Ractors are not supported.

## How receiving works

zenoh-c runs the protocol on its own threads. Every subscriber, liveliness
watch, queryable and get has a zenoh-c FIFO channel; the gem's callback in
front of it (plain C: no Ruby, no GVL) counts what arrives and, when the
channel is full, drops the oldest entry, like the mruby gem's ring. The
application takes the entries out with `each_pending` / `each_reply` from
its own thread. Nothing calls into Ruby behind its back, and `poll` does not
need to run for data to arrive: it only checks the connection.

Waiting calls (`Session.open`, `put`, `get`, `liveliness_get`, `close`)
release the GVL. A `Session` and its objects may be used from several Ruby
threads: the calls that keep the GVL are serialized by it, the ones that
release it by a lock of the session, and a session that closes (by `close`
or because the connection was lost) is closed for every thread before
zenoh-c lets it go; calls made after that raise `Asterism::Zenoh::Error`.
Each entry of a queue is taken by exactly one `each_pending` /
`each_reply`. The gem itself starts no Ruby thread: blocks, a receiving
thread and Enumerators on top of this API are in the CRuby layer of the
`asterism` gem.

## Behaviour kept from the mruby gem

- **No scouting** unless asked for (`scouting: true`): the locator is
  given. A peer that only connects does not listen.
- **Remote only**: a session's own puts do not reach its own subscribers,
  and its gets do not reach its own queryables (zenoh-pico's behaviour;
  Asterism calls its own objects in place).
- **Losing the connection**: a client session is closed when it has no
  router left, a peer session that only connects when it has no peer left;
  a listening (or scouting) session stays open. From then on `poll` is false, `closed?`
  true, and `put` raises `Asterism::Zenoh::Error`. No reconnection.
- **Full queues drop the oldest** entry and count it in `dropped` (a dropped
  query is finished, so its requester gets no answer from it).

### Queue depths

Every receiving object has a bounded queue; what does not fit is dropped,
oldest first, and counted in `dropped`. A router answers a wildcard get or
liveliness get, and a new liveliness watch, with everything at once, so
those queues are deep by default:

| | CRuby (zenoh-c) | Boards (zenoh-pico) |
|---|---|---|
| `subscribe`, `queryable`, listeners (`DEFAULT_DEPTH`) | 16 | 16 |
| `get`, `liveliness_get`, `querier.get` (`DEFAULT_GET_DEPTH`) | 1024 | 16 |
| `liveliness_watch` (`DEFAULT_WATCH_DEPTH`) | 1024 | 16 |
| largest `depth:` (`MAX_DEPTH`) | 65536 | 1024 |

On a PC a deep queue costs nothing until it fills (zenoh-c's FIFO and the
gem's lists grow as entries come). On a board a get's queue is allocated
when the get is sent; pass `depth:` when a wildcard can match more than 16
answers. Check `dropped` after a get whose answers matter; the asterism
gem's CRuby API warns once when one of its gets or watches dropped
something (`Asterism.warn_once`).

Differences: `C_VERSION` instead of `PICO_VERSION`; `MAX_PEERS` is zenoh-c's
limit, not 3; the time limits of gets are kept by zenoh-c (exact, not
checked once a second); liveliness watches may also report the session's
own tokens.

## Installing

```
gem install asterism-zenoh       # or `gem install asterism`, which depends on it
```

Needs CRuby 3.2+ and a C compiler; nothing else (no Rust, no git, curl or
unzip). `gem install` compiles the C extension against the official
prebuilt zenoh-c release pinned in `ZENOH_C_PIN`:

1. With `ZENOH_C_DIR` set (or `gem install asterism-zenoh --
   --with-zenoh-c-dir=DIR`), that zenoh-c (its `include/` and `lib/`) is
   used and nothing is downloaded.
2. Otherwise extconf.rb downloads the release archive for the machine from
   `https://github.com/eclipse-zenoh/zenoh-c/releases/download/<tag>/`
   (Ruby's net/http; proxies from the usual environment variables), checks
   it against the sha256 pinned for that machine, and unpacks `include/`
   and the shared library with Ruby's zlib. An archive whose sha256 differs
   is not used.
3. Machines without a pinned release, and machines that cannot download
   it, stop with what to do instead (`ZENOH_C_DIR`, or a mirror).

Pinned machines: x86_64 and aarch64 Linux (glibc and musl), x86_64 and
arm64 macOS. The glibc builds of zenoh-c need glibc 2.34 or newer (Ubuntu
22.04, Debian 12 and later). macOS is handled by extconf (the dylib's install
name is rewritten to `@rpath`) but has not been tested yet. `ASTERISM_ZENOH_C_MIRROR=<base>` downloads
`<base>/<tag>/<asset>` instead (an http(s) or `file://` URL, or a local
directory), for a mirror or a machine without access to GitHub.

The installed gem has, in `lib/asterism/`, the extension, zenoh-c's shared
library next to it (found through the rpath: `$ORIGIN` on Linux,
`@loader_path` on macOS), and `zenoh-c/` with the text of the Apache
License 2.0, zenoh-c's NOTICE.md and `SOURCE` (which archive was used, and
its sha256). The downloaded archive is not kept. `gem uninstall` removes
all of it.

## Building and testing

In the repository (needs CRuby 3.2+ and a C compiler):

```
rake                  # zenoh_c:fetch, compile, test
rake zenoh_c:fetch    # download the release pinned in ZENOH_C_PIN to vendor/zenoh-c (sha256 checked)
rake compile          # build lib/asterism/asterism_zenoh.so (+ libzenohc.so)
rake test             # two sessions over a local peer link; no router needed
ASTERISM_TEST_ROUTER=tcp/127.0.0.1:7447 rake test   # the same through a zenohd router
ZENOH_C_DIR=/path/to/zenoh-c rake compile           # use another zenoh-c (include/ and lib/)
rake gem              # build pkg/asterism-zenoh-<version>.gem
```

`rake zenoh_c:fetch` uses the same code as the installation
(`ext/asterism_zenoh/zenoh_c.rb`). The extension links zenoh-c's shared
library, which `rake compile` copies next to it, so `ruby -I lib` works
without installing anything.

The object layer, the ROS 2 node and the message types on top of this
binding are the `asterism` gem
([ruby-asterism/asterism](https://github.com/ruby-asterism/asterism)).

## License

MIT (see LICENSE) for everything in this repository except
`licenses/zenoh-c/` (zenoh-c's notices and the Apache License text, below).
The C extension
(`ext/asterism_zenoh/zenoh.c`) is this gem's own code: it calls zenoh-c's
API and copies no code from zenoh-c's examples or headers.

### zenoh-c is not in this repository or in the gem file

[zenoh-c](https://github.com/eclipse-zenoh/zenoh-c) (Eclipse Zenoh's C
binding, Copyright ZettaScale Technology) is offered under the Eclipse
Public License 2.0 or the Apache License, Version 2.0 (EPL-2.0 OR
Apache-2.0). This gem uses it under the **Apache License, Version 2.0**.
Neither this repository nor the gem file contains it: `gem install` (and
`rake zenoh_c:fetch`) downloads the official prebuilt release pinned in
`ZENOH_C_PIN` (sha256 checked) on the user's machine, from zenoh-c's own
GitHub releases (or a mirror the user names).

The prebuilt release archives contain only `include/` and `lib/`, not
zenoh-c's LICENSE or NOTICE.md. So the gem carries, in `licenses/zenoh-c/`,
the text of the Apache License, Version 2.0 (`LICENSE-APACHE`) and zenoh-c's
NOTICE.md at the pinned tag, unchanged, and installs both next to the
shared library (`lib/asterism/zenoh-c/`). The gemspec lists `MIT` and
`Apache-2.0`: MIT for this gem's own code, Apache-2.0 for zenoh-c, which
the installed gem holds.

### Distribution notes

A package that **carries** zenoh-c itself (a prebuilt, platform-specific
gem with the shared library inside, a container image, an archive of an
installed gem or a built `lib/`) is a redistribution of zenoh-c under the
Apache License, Version 2.0, and must carry:

- the text of the Apache License, Version 2.0, and zenoh-c's NOTICE.md
  (its notices, including the Eclipse trademark notice), unchanged: the
  installed `lib/asterism/zenoh-c/` has both;
- the licenses and notices of the Rust crates compiled into the shared
  library (zenoh and its dependencies, listed in zenoh-c's Cargo.lock;
  zenoh-c's NOTICE.md does not list them). Collect them from that
  Cargo.lock for the pinned release with a tool such as cargo-about
  before publishing such a package.
