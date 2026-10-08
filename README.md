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
