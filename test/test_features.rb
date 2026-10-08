# The calls added in 0.3.0, between two CRuby sessions: configuration
# (Hash, String, file, TLS), scouting, delete, declared publishers and
# matching, queriers, reply_err / reply_del, encodings, timestamps, key
# expressions, the advanced publisher / subscriber, transport and link
# events, and the log.
require_relative "helper"
require "json"
require "openssl"
require "open3"
require "rbconfig"
require "tmpdir"

class TestFeatures < Minitest::Test
  include TestHelper

  def setup
    @a, @b = open_pair
    @settle = -> { sleep 0.3 }
  end

  def teardown
    @b&.close
    @a&.close
  end

  def test_sample_fields_and_delete
    sub = @a.subscribe("c7/s/**")
    @settle.call
    ts = @b.new_timestamp
    @b.put("c7/s/x", '{"a":1}', encoding: "application/json", priority: :data_high,
                                congestion_control: :block, express: true, timestamp: ts, attachment: "at")
    @b.delete("c7/s/x", timestamp: true)
    assert wait_for { sub.pending == 2 }
    put, del = sub.each_sample
    assert_equal "c7/s/x", put.key
    assert_equal '{"a":1}', put.payload
    assert_equal "at", put.attachment
    assert_equal :put, put.kind
    assert put.put?
    assert_equal "application/json", put.encoding
    assert_equal :data_high, put.priority
    assert_equal :block, put.congestion_control
    assert put.express
    assert_equal :reliable, put.reliability
    assert_equal ts, put.timestamp
    assert_equal :delete, del.kind
    assert del.delete?
    assert_equal "", del.payload
    refute_nil del.timestamp
    assert_operator del.timestamp, :>, ts
    # each_pending (0.2.0) and each_sample take from the same queue.
    @b.put("c7/s/y", "v")
    assert wait_for { sub.pending == 1 }
    assert_equal [["c7/s/y", "v", nil]], sub.each_pending
    case put
    in {kind: :put, encoding: "application/json", key:}
      assert_equal "c7/s/x", key
    end
  end

  def test_put_arguments
    assert_raises(ArgumentError) { @b.put("c7/x", "v", priority: :bogus) }
    assert_raises(ArgumentError) { @b.put("c7/x", "v", priority: 9) }
    assert_raises(ArgumentError) { @b.put("c7/x", "v", congestion_control: :bogus) }
    assert_raises(ArgumentError) { @b.put("c7/x", "v", reliability: :bogus) }
    assert_raises(ArgumentError) { @b.put("c7/x", "v", allowed_destination: :bogus) }
    assert_raises(TypeError) { @b.put("c7/x", "v", timestamp: 3) }
    assert_raises(ArgumentError) { @b.delete("c7/x", attachment: "a") }
    @b.put("c7/x", "v", priority: 1, reliability: :best_effort, allowed_destination: :remote, encoding: nil)
  end

  def test_timestamps
    # Without timestamping the session has no HLC and zenoh takes the
    # system clock, so two timestamps in a row may be equal where the clock
    # is coarse (macOS: microseconds). Strictly increasing needs the HLC.
    t1 = @a.new_timestamp
    t2 = @a.new_timestamp
    assert_operator t1, :<=, t2
    hlc = Z::Session.open(nil, mode: :peer, listen: "tcp/127.0.0.1:#{TestHelper.free_port}", timestamping: true)
    begin
      ts = Array.new(200) { hlc.new_timestamp }
      ts.each_cons(2) { |x, y| assert_operator x, :<, y }
      assert_equal hlc.zid, ts[0].id
    ensure
      hlc.close
    end
    assert_equal @a.zid, t1.id
    assert_in_delta Time.now.to_f, t1.to_time.to_f, 5
    assert_kind_of Integer, t1.ntp64
    assert_match(/\A\d{4}-\d\d-\d\dT.*Z\/\h+\z/, t1.to_s)
    assert_raises(NoMethodError) { Z::Timestamp.new }
  end

  def test_publisher_and_matching
    pub = @b.publisher("c7/p/x", encoding: "text/plain", priority: :interactive_low)
    ml = pub.matching_listener
    refute pub.matching?
    sub = @a.subscribe("c7/p/**")
    assert wait_for { pub.matching? }
    assert wait_for { ml.pending >= 1 }
    assert_equal [true], ml.each_pending
    pub.put("one", attachment: "a1")
    pub.put("two", encoding: "application/json")
    pub.delete
    assert wait_for { sub.pending == 3 }
    s1, s2, s3 = sub.each_sample
    assert_equal ["one", "text/plain", :interactive_low, "a1"], [s1.payload, s1.encoding, s1.priority, s1.attachment]
    assert_equal "application/json", s2.encoding
    assert_equal :delete, s3.kind
    sub.close
    assert wait_for { ml.pending >= 1 }
    assert_equal [false], ml.each_pending
    ml.close
    assert ml.closed?
    pub.close
    assert pub.closed?
    assert_raises(Z::Error) { pub.put("x") }
  end

  def test_querier_and_reply_kinds
    qa = @a.queryable("c7/q/**")
    q = @b.querier("c7/q/**", timeout_ms: 1500, target: :all, consolidation: :none)
    assert wait_for { q.matching? }
    g = q.get("p=1", "body", attachment: "qa", encoding: "text/plain")
    assert wait_for { qa.pending == 1 }
    qa.each_pending do |query|
      assert_equal "p=1", query.params
      assert_equal "body", query.payload
      assert_equal "text/plain", query.encoding
      ts = @a.new_timestamp
      query.reply("c7/q/a", "[1]", encoding: "application/json", timestamp: ts)
      query.reply_del("c7/q/b")
      query.reply_err("no such thing", encoding: "text/plain")
    end
    assert wait_for { g.done? }
    replies = g.each_result
    ok, del, err = replies
    assert_equal ["c7/q/a", "[1]", "application/json", :put], [ok.key, ok.payload, ok.encoding, ok.kind]
    refute_nil ok.timestamp
    assert ok.ok?
    assert_equal ["c7/q/b", :delete], [del.key, del.kind]
    assert err.error?
    assert_equal ["no such thing", "text/plain", nil], [err.payload, err.encoding, err.key]
    assert_equal 1, g.errors
    assert_equal @a.zid, ok.replier_zid
    # each_reply (0.2.0) leaves the error replies out.
    g2 = q.get
    assert wait_for { qa.pending == 1 }
    qa.each_pending do |query|
      query.reply_err("bad")
      query.reply("c7/q/z", "fine")
    end
    assert wait_for { g2.done? }
    assert_equal [["c7/q/z", "fine", nil]], g2.each_reply
    assert_equal 1, g2.errors
    assert_equal 0, g2.pending
    q.close
    assert q.closed?
    assert_raises(Z::Error) { q.get }
    assert_raises(ArgumentError) { @b.querier("c7/q", target: :bogus) }
  end

  def test_query_reply_arguments
    qa = @a.queryable("c7/qa")
    @settle.call
    g = @b.get("c7/qa", 1000)
    assert wait_for { qa.pending == 1 }
    qa.each_pending do |query|
      assert_nil query.encoding
      assert_raises(ArgumentError) { query.reply("x", timestamp: true) }
      assert_raises(ArgumentError) { query.reply("x", priority: :bogus) }
      assert_raises(TypeError) { query.reply_err(1) }
      query.reply("ok", priority: :real_time, express: true, congestion_control: :drop)
    end
    assert wait_for { g.done? }
    assert_equal "ok", g.each_reply[0][1]
  end

  def test_key_expressions
    ke = Z::KeyExpr.new("c7/k/*")
    assert ke.intersects?("c7/k/x")
    assert ke.includes?("c7/k/x")
    refute ke.includes?("c7/k/**")
    refute ke.intersects?("c7/j/x")
    assert_equal :equals, ke.relation_to("c7/k/*")
    assert_equal :includes, ke.relation_to("c7/k/x")
    assert_equal :intersects, ke.relation_to("c7/**/x")
    assert_equal :disjoint, ke.relation_to("other")
    assert_equal "c7/k/*/more", ke.join("more").to_s
    assert_equal "c7/k/*/a/b", ke.join(Z::KeyExpr.new("a/b")).to_s
    assert_equal "c7/kx", Z::KeyExpr.new("c7/k").concat("x").to_s
    assert_equal "a/**", Z::KeyExpr.canonize("a/**/**")
    assert Z::KeyExpr.valid?("a/b")
    refute Z::KeyExpr.valid?("a/**/**")
    refute Z::KeyExpr.valid?("a//b")
    assert_raises(ArgumentError) { Z::KeyExpr.new("a//b") }
    assert_raises(ArgumentError) { Z::KeyExpr.new("a/**/**") }
    assert_equal "a/**", Z::KeyExpr.new("a/**/**", autocanonize: true).to_s
    assert_equal Z::KeyExpr.new("a/b"), "a/b"
    assert_equal Z::KeyExpr.new("a/b"), Z::KeyExpr.new("a/b")
    assert_equal Z::KeyExpr.new("a/b").hash, Z::KeyExpr.new("a/b").hash
    refute ke.declared?
    # A KeyExpr is accepted where a key is.
    sub = @a.subscribe(Z::KeyExpr.new("c7/k/**"))
    dk = @b.declare_keyexpr("c7/k/declared")
    assert dk.declared?
    @settle.call
    @b.put(dk, "via declared")
    @b.publisher(dk).put("via publisher")
    assert wait_for { sub.pending == 2 }
    assert_equal [["c7/k/declared", "via declared", nil], ["c7/k/declared", "via publisher", nil]], sub.each_pending
    dk.undeclare
    refute dk.declared?
    @b.put(dk, "still a key")
    assert wait_for { sub.pending == 1 }
  end

  def test_advanced_publisher_history
    pub = @b.advanced_publisher("c7/adv/x", cache: 3, publisher_detection: true,
                                            sample_miss_detection: { heartbeat: :periodic, heartbeat_ms: 500 })
    5.times { |i| pub.put("v#{i}") }
    @settle.call
    # A subscriber that comes later gets the last 3 from the cache.
    sub = @a.advanced_subscriber("c7/adv/**", 16, history: { detect_late_publishers: true },
                                                  recovery: { heartbeat: true }, subscriber_detection: true)
    assert wait_for { sub.pending >= 3 }
    assert_equal %w[v2 v3 v4], sub.each_sample.map(&:payload)
    pub.put("v5")
    assert wait_for { sub.pending >= 1 }
    assert_equal ["v5"], sub.each_pending.map { |e| e[1] }
    watch = sub.detect_publishers
    assert wait_for { watch.pending >= 1 }
    key, alive = watch.each_pending[0]
    assert alive
    assert_includes key, "c7/adv/x"
    misses = sub.miss_listener
    assert_equal [], misses.each_pending
    assert pub.matching?
    pub.close
    assert wait_for { watch.pending >= 1 }
    refute watch.each_pending[0][1]
    misses.close
    watch.close
    sub.close
    assert sub.closed?
    assert_raises(Z::Error) { sub.miss_listener }
    assert_raises(ArgumentError) { @b.advanced_publisher("c7/adv/y", cache: { bogus: 1 }) }
    assert_raises(ArgumentError) { @a.advanced_subscriber("c7/adv/y", history: { bogus: 1 }) }
  end

  def test_transport_and_link_events
    skip "peer link only" if TestHelper.router
    loc = "tcp/127.0.0.1:#{TestHelper.free_port}"
    l = Z::Session.open(nil, mode: :peer, listen: loc)
    tev = l.transport_events
    lev = l.link_events
    c = Z::Session.open(loc, mode: :peer)
    assert wait_for { tev.pending >= 1 && lev.pending >= 1 }
    t = tev.each_pending[0]
    assert t.added?
    assert_equal [c.zid, :peer], [t.zid, t.whatami]
    k = lev.each_pending[0]
    assert k.added?
    assert_equal c.zid, k.zid
    assert_equal loc, k.src
    assert_equal [c.zid], l.peer_zids
    assert_equal [], l.router_zids
    assert_equal [c.zid], l.transports.map(&:zid)
    assert_equal [c.zid], l.links.map(&:zid)
    hist = l.transport_events(history: true)
    assert wait_for { hist.pending >= 1 }
    assert_equal c.zid, hist.each_pending[0].zid
    c.close
    assert wait_for { tev.pending >= 1 && lev.pending >= 1 }
    assert tev.each_pending[0].removed?
    assert lev.each_pending[0].removed?
    tev.close
    assert tev.closed?
  ensure
    c&.close
    l&.close
  end
end

class TestConfig < Minitest::Test
  include TestHelper

  def test_config_hash_listen_and_timeout
    loc = "tcp/127.0.0.1:#{TestHelper.free_port}"
    a = Z::Session.open(mode: :peer, config: { "listen/endpoints" => [loc] })
    b = Z::Session.open(loc, mode: :peer, config: { "connect/timeout_ms" => 1000 })
    assert wait_for { a.peers == 1 }
    refute a.closed?
    # The time limit of the config Hash wins over the gem's own 3 s.
    t = Time.now
    assert_raises(Z::Error) do
      Z::Session.open("tcp/127.0.0.1:#{TestHelper.free_port}", config: { "connect/timeout_ms" => 300 })
    end
    assert_operator Time.now - t, :<, 2.0
    assert_raises(ArgumentError) { Z::Session.open(loc, config: { "no/such/key" => 1 }) }
    assert_raises(TypeError) { Z::Session.open(loc, config: 3) }
  ensure
    b&.close
    a&.close
  end

  def test_timestamping_and_a_plain_cache
    loc = "tcp/127.0.0.1:#{TestHelper.free_port}"
    a = Z::Session.open(nil, mode: :peer, listen: loc)
    b = Z::Session.open(loc, mode: :peer, timestamping: true)
    # A cache without sample_miss_detection needs timestamping.
    e = assert_raises(Z::Error) { a.advanced_publisher("c7/ts/x", cache: 2) }
    assert_match(/timestamping/, e.message)
    pub = b.advanced_publisher("c7/ts/x", cache: 2)
    sub = a.subscribe("c7/ts/x")
    sleep 0.3
    pub.put("stamped")
    b.put("c7/ts/x", "also stamped")
    assert wait_for { sub.pending == 2 }
    sub.each_sample.each { |s| assert_equal b.zid, s.timestamp.id }
  ensure
    b&.close
    a&.close
  end

  def test_config_string_and_file
    loc = "tcp/127.0.0.1:#{TestHelper.free_port}"
    a = Z::Session.open(config: "{mode: 'peer', listen: {endpoints: ['#{loc}']}}")
    Dir.mktmpdir do |dir|
      path = File.join(dir, "zenoh.json5")
      File.write(path, "{mode: 'peer', connect: {endpoints: ['#{loc}']}}")
      b = Z::Session.open(config_file: path)
      assert wait_for { a.peers == 1 && b.peers == 1 }
      b.close
      assert_raises(ArgumentError) { Z::Session.open(config_file: File.join(dir, "missing.json5")) }
      assert_raises(ArgumentError) { Z::Session.open(config_file: path, config: "{}") }
    end
  ensure
    a&.close
  end

  # A self-signed CA and a certificate for localhost, made here.
  def make_certs(dir)
    key = OpenSSL::PKey::RSA.new(2048)
    ca = OpenSSL::X509::Certificate.new
    ca.version = 2
    ca.serial = 1
    ca.subject = ca.issuer = OpenSSL::X509::Name.parse("/CN=asterism test CA")
    ca.public_key = key.public_key
    ca.not_before = Time.now - 60
    ca.not_after = Time.now + 3600
    ef = OpenSSL::X509::ExtensionFactory.new(ca, ca)
    ca.add_extension(ef.create_extension("basicConstraints", "CA:TRUE", true))
    ca.add_extension(ef.create_extension("keyUsage", "keyCertSign,cRLSign", true))
    ca.sign(key, OpenSSL::Digest.new("SHA256"))
    skey = OpenSSL::PKey::RSA.new(2048)
    cert = OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = 2
    cert.subject = OpenSSL::X509::Name.parse("/CN=localhost")
    cert.issuer = ca.subject
    cert.public_key = skey.public_key
    cert.not_before = Time.now - 60
    cert.not_after = Time.now + 3600
    ef = OpenSSL::X509::ExtensionFactory.new(cert, ca)
    cert.add_extension(ef.create_extension("subjectAltName", "DNS:localhost,IP:127.0.0.1", false))
    cert.add_extension(ef.create_extension("extendedKeyUsage", "serverAuth", false))
    cert.sign(key, OpenSSL::Digest.new("SHA256"))
    files = { ca: ca, cert: cert }.to_h { |k, v| [k, File.join(dir, "#{k}.pem")] }
    File.write(files[:ca], ca.to_pem)
    File.write(files[:cert], cert.to_pem)
    files[:key] = File.join(dir, "key.pem")
    File.write(files[:key], skey.private_to_pem)
    files
  end

  def test_tls_between_two_sessions
    Dir.mktmpdir do |dir|
      f = make_certs(dir)
      loc = "tls/localhost:#{TestHelper.free_port}"
      a = Z::Session.open(nil, mode: :peer, listen: loc,
                               config: { "transport/link/tls/listen_private_key" => f[:key],
                                         "transport/link/tls/listen_certificate" => f[:cert] })
      b = Z::Session.open(loc, mode: :peer, config: { "transport/link/tls/root_ca_certificate" => f[:ca] })
      sub = a.subscribe("c7/tls")
      sleep 0.3
      b.put("c7/tls", "over tls")
      assert wait_for { sub.pending == 1 }
      assert_equal "over tls", sub.each_pending[0][1]
      assert b.links[0].dst.start_with?("tls/")
      # Without the CA the certificate is not trusted.
      assert_raises(Z::Error) { Z::Session.open(loc, mode: :peer, config: { "connect/timeout_ms" => 1000 }) }
    ensure
      b&.close
      a&.close
    end
  end
end

class TestScouting < Minitest::Test
  include TestHelper

  def test_scout_and_scouting_peers
    loc = "tcp/127.0.0.1:#{TestHelper.free_port}"
    a = Z::Session.open(nil, mode: :peer, listen: loc, scouting: true)
    found = Z.scout(what: :peer, timeout: 1.5)
    if found.none? { |h| h.zid == a.zid }
      skip "multicast scouting gets no answer here (no multicast on this network: containers, VPNs, " \
           "some WSL2 setups); found #{found.size} other(s)"
    end
    h = found.find { |x| x.zid == a.zid }
    assert_equal :peer, h.whatami
    assert_includes h.locators, loc
    # A session with scouting on and no locator finds the other by itself.
    b = Z::Session.open(mode: :peer, scouting: true)
    assert wait_for(5) { b.peer_zids.include?(a.zid) }
    assert_raises(ArgumentError) { Z.scout(what: :bogus) }
  ensure
    b&.close
    a&.close
  end
end

class TestLog < Minitest::Test
  # zenoh-c's logger writes to standard output.
  def test_init_log
    lib = File.expand_path("../lib", __dir__)
    code = <<~RUBY
      require "asterism/zenoh"
      Asterism::Zenoh.init_log("debug")
      s = Asterism::Zenoh::Session.open(nil, mode: :peer, listen: "tcp/127.0.0.1:0")
      s.close
    RUBY
    env = { "RUST_LOG" => nil }
    out, err, st = Open3.capture3(env, RbConfig.ruby, "-I", lib, "-e", code)
    assert st.success?, err
    assert_match(/DEBUG/, out)
    assert_match(/zenoh::net::runtime/, out)
    # nil: RUST_LOG only, and it is not set: nothing.
    out, err, st = Open3.capture3(env, RbConfig.ruby, "-I", lib, "-e", code.sub('"debug"', "nil"))
    assert st.success?, err
    assert_equal "", out
    # RUST_LOG wins over the level given.
    out, _err, st = Open3.capture3({ "RUST_LOG" => "error" }, RbConfig.ruby, "-I", lib, "-e", code)
    assert st.success?
    refute_match(/DEBUG/, out)
  end
end
