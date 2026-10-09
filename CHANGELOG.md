# Changelog

All notable changes to asterism-zenoh. The mruby / PicoRuby binding
(picoruby-asterism-zenoh) carries the same version number from 0.4.0 on.
Nothing has been published to rubygems.org yet; the versions below are
the ones in this repository.

## 0.4.0

The first step of the API review toward 1.0 (the asterism gem's
docs/api_review.md). Additions and deprecations only: every 0.3.0 call
works as before.

Added

- Time limits in seconds: `timeout:` on `get`, `liveliness_get` and
  `querier`, `query_timeout:` on `advanced_subscriber`, and
  `connect_timeout:` on `Session.open` (it sets `connect/timeout_ms`).
  `timeout_ms:` stays next to `timeout:`. Giving a time limit twice raises
  `ArgumentError`, as does `scout(timeout:, timeout_ms:)`.
- Keywords next to the positional optionals: `get(key, params:, payload:)`,
  `depth:` on `subscribe`, `queryable`, `liveliness_watch`,
  `advanced_subscriber`, the listeners and the event streams, and
  `Querier#get(params:, payload:)`.
- `Session#connection_count` (the routers of a client session, or the peers
  of a peer session).
- One error tree: `Asterism::Error` (defined here, reopened by the asterism
  gem) > `Asterism::Zenoh::Error` > `Asterism::Zenoh::ClosedError` (the
  session is closed or its connection was lost). `Error#code` is zenoh-c's
  result code when the failure had one.
- `Session.open { |s| }` closes the session after the block.
- `Query#reply_error` / `reply_delete` (the spelled-out `reply_err` /
  `reply_del`).
- `BACKEND` (`:zenoh_c`), `BACKEND_VERSION`, `PEER_SUPPORTED`,
  `DEFAULT_TIMEOUT` (2.0 s).
- `express?`, `multicast?` and `streamed?` on the values that have those
  members.
- A session used in a forked child raises `ClosedError` ("open a new
  session after fork") instead of hanging on zenoh-c's threads, which do
  not survive `fork`.
- `Asterism.deprecated` and `Asterism.deprecations = :warn / :raise /
  :silent` (also `ASTERISM_DEPRECATIONS`): the one helper every Asterism
  gem uses to warn once per name.

Deprecated (each warns once; removed or changed in 1.0)

- The time limit as a positional argument: `get(key, timeout_ms, params,
  payload)` and `liveliness_get(key, timeout_ms)`. A Float there warns
  with its own message: it is milliseconds and truncated (`get(key, 2.0)`
  waits 2 ms).
- `Session#peers`: use `connection_count`.
- `Query#reply(payload)` when the query's key differs from the queryable's
  own key and that key has no wildcard: from 1.0 such a reply answers on
  the queryable's key.

## 0.3.0

- Configuration (`config:` Hash or JSON5 String, `config_file:`; TLS and
  the rest of zenoh's keys), scouting, `delete`, declared publishers and
  queriers with matching status, the full sample and reply values (`Data`),
  error and delete replies, encodings, timestamps, key expressions, the
  advanced publisher / subscriber, transport and link events, zenoh-c's
  log. All CRuby only.
- CI on Linux and macOS, Ruby 3.2 to 4.0, and an installed-gem check.

## 0.2.0

- A session and its objects may be used from several Ruby threads.

## 0.1.0

- The CRuby binding over the prebuilt zenoh-c, split out of the asterism
  repository: the API of the mruby / PicoRuby binding (sessions, put /
  subscribe, get / queryable, liveliness, attachments).
