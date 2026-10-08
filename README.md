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

## API

The same calls, arguments and results as the mruby / PicoRuby gem
([picoruby-asterism-zenoh](https://github.com/ruby-asterism/picoruby-asterism-zenoh)):

| Call | Notes |
|---|---|
| `Session.open(locator = nil, mode: :client, listen: nil)` | client of a router, or `mode: :peer` connecting to `locator` and/or listening on `listen:`. `Error` when nobody answers within `CONNECT_TIMEOUT_MS`. Releases the GVL while connecting |
| `session.put(key, payload, attachment: nil)` | `payload` / `attachment` are Strings (bytes). Releases the GVL |
| `session.subscribe(key, depth = 16)` -> `Subscriber` | `each_pending { \|key, payload, attachment\| }` (or an Array), `pending` / `received` / `dropped`, `close` / `closed?` |
| `session.get(key, timeout_ms = 2000, params = nil, payload = nil, attachment: nil, target: :all, consolidation: :none)` -> `Get` | returns at once; `each_reply { \|key, payload, attachment\| }`, `done?`, `pending` / `received` / `dropped` / `errors` |
| `session.queryable(key, depth = 16, complete: false)` -> `Queryable` | `each_pending { \|q\| }` (each query finished after the block) or an Array of `Query` |
| `q.key` / `params` / `payload` / `attachment`, `q.reply([key,] payload, attachment: nil)`, `q.finish` / `finished?` | |
| `session.liveliness(key)` -> `LivelinessToken` | `close` / `closed?` |
| `session.liveliness_watch(key, depth = 16)` -> `LivelinessWatch` | `each_pending { \|key, alive\| }`; the tokens alive now come first |
| `session.liveliness_get(key, timeout_ms = 2000)` -> `Get` | |
| `session.poll(steps = 8)` / `closed?` / `close` / `zid` / `peers` | |
| `Asterism::Zenoh::Error` | |
| `CONNECT_TIMEOUT_MS`, `SEND_TIMEOUT_MS` (3000), `PEER` (true), `MAX_PEERS`, `C_VERSION` | `MAX_PEERS` is zenoh-c's `transport/unicast/max_sessions` (1000); `C_VERSION` stands for the mruby gem's `PICO_VERSION` |

Keys come back as UTF-8 Strings, payloads and attachments as binary
(ASCII-8BIT) Strings.

`require "asterism/zenoh/global"` defines `Zenoh = Asterism::Zenoh` for
those who want the short name; nothing defines it by default.

## How receiving works

zenoh-c runs the protocol on its own threads. Every subscriber, liveliness
watch, queryable and get has a zenoh-c FIFO channel; the gem's callback in
front of it (plain C: no Ruby, no GVL) counts what arrives and, when the
channel is full, drops the oldest entry, like the mruby gem's ring. The
application takes the entries out with `each_pending` / `each_reply` from
its own thread. Nothing calls into Ruby behind its back, and `poll` does not
need to run for data to arrive: it only checks the connection.

Waiting calls (`Session.open`, `put`, `get`, `liveliness_get`, `close`)
release the GVL. One `Session` and its objects are meant to be used from
one Ruby thread at a time, as on the boards.

## Behaviour kept from the mruby gem

- **No scouting**: the locator is given. A peer that only connects does not
  listen.
- **Remote only**: a session's own puts do not reach its own subscribers,
  and its gets do not reach its own queryables (zenoh-pico's behaviour;
  Asterism calls its own objects in place).
- **Losing the connection**: a client session is closed when it has no
  router left, a peer session that only connects when it has no peer left;
  a listening session stays open. From then on `poll` is false, `closed?`
  true, and `put` raises `Asterism::Zenoh::Error`. No reconnection.
- **Full queues drop the oldest** entry and count it in `dropped` (a dropped
  query is finished, so its requester gets no answer from it).

Differences: `C_VERSION` instead of `PICO_VERSION`; `MAX_PEERS` is zenoh-c's
limit, not 3; the time limits of gets are kept by zenoh-c (exact, not
checked once a second); liveliness watches may also report the session's
own tokens.

## Building and testing

Needs CRuby 3.2+, a C compiler, `curl` and `unzip`. No Rust: zenoh-c is the
official prebuilt release.

```
rake                  # zenoh_c:fetch, compile, test
rake zenoh_c:fetch    # download the release pinned in ZENOH_C_PIN to vendor/zenoh-c (sha256 checked)
rake compile          # build lib/asterism/asterism_zenoh.so (+ libzenohc.so)
rake test             # two sessions over a local peer link; no router needed
ASTERISM_TEST_ROUTER=tcp/127.0.0.1:7447 rake test   # the same through a zenohd router
ZENOH_C_DIR=/path/to/zenoh-c rake compile           # use another zenoh-c (include/ and lib/)
```

The extension links `libzenohc.so`, which is copied next to it
(`rpath $ORIGIN`), so `ruby -I lib` works without installing anything.
Prebuilt zenoh-c is pinned for x86_64 Linux only for now.

The object layer, the ROS 2 node and the message types on top of this
binding are the `asterism` gem
([ruby-asterism/asterism](https://github.com/ruby-asterism/asterism)).

## License

MIT (see LICENSE) for everything in this repository. The C extension
(`ext/asterism_zenoh/zenoh.c`) is this gem's own code: it calls zenoh-c's
API and copies no code from zenoh-c's examples or headers.

### zenoh-c is not in this repository

[zenoh-c](https://github.com/eclipse-zenoh/zenoh-c) (Eclipse Zenoh's C
binding, Copyright ZettaScale Technology) is offered under the Eclipse
Public License 2.0 or the Apache License, Version 2.0 (EPL-2.0 OR
Apache-2.0). This gem uses it under the **Apache License, Version 2.0**.
It is not part of this repository or of the gem's source package:
`rake zenoh_c:fetch` downloads the official prebuilt release pinned in
`ZENOH_C_PIN` (sha256 checked) into `vendor/`, which git ignores, and
`rake compile` copies its `libzenohc.so` next to the extension (also
ignored). Nothing of zenoh-c is committed or packaged.

### Distribution notes

The source gem carries no zenoh-c, so it needs nothing more than LICENSE.
A package that **does** carry zenoh-c (a prebuilt gem with `libzenohc.so`,
a container image, an archive of a built `lib/`) is a redistribution of
zenoh-c under the Apache License, Version 2.0, and must also carry:

- the text of the Apache License, Version 2.0 (zenoh-c's LICENSE holds it,
  together with the EPL-2.0 text);
- zenoh-c's NOTICE.md (its notices, including the Eclipse trademark
  notice), unchanged;
- the licenses and notices of the Rust crates compiled into
  `libzenohc.so` (zenoh and its dependencies, listed in zenoh-c's
  Cargo.lock; zenoh-c's NOTICE.md does not list them). Collect them from
  that Cargo.lock for the pinned release with a tool such as cargo-about
  before publishing such a package.

The prebuilt release archive pinned now contains only `include/` and `lib/`,
not LICENSE or NOTICE.md; take those from the zenoh-c repository at the
pinned tag.
