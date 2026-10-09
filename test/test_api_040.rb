# What 0.4.0 adds (docs of the asterism gem: docs/api_review.md): time
# limits in seconds, keywords next to the positional optionals, the error
# tree, connection_count, the backend constants, Session.open with a block,
# fork detection, and the deprecation warnings of the old forms.
require_relative "helper"

class TestApi040 < Minitest::Test
  include TestHelper

  def setup
    @a, @b = open_pair
    @settle = -> { sleep 0.3 }
  end

  def teardown
    @b.close if @b
    @a.close if @a
  end

  def test_error_tree
    assert_operator Z::Error, :<, Asterism::Error
    assert_operator Asterism::Error, :<, StandardError
    assert_operator Z::ClosedError, :<, Z::Error
    assert_operator Asterism::DeprecationError, :<, Asterism::Error
  end

  def test_error_code
    port = TestHelper.free_port
    e = assert_raises(Z::Error) { Z::Session.open("tcp/127.0.0.1:#{port}") }
    assert_kind_of Integer, e.code
    assert_operator e.code, :<, 0
    assert_nil Z::Error.new("x").code
  end

  def test_closed_error
    @a.close
    e = assert_raises(Z::ClosedError) { @a.put("c8/x", "y") }
    assert_match(/closed/, e.message)
    assert_raises(Z::ClosedError) { @a.get("c8/x") }
  end

  def test_backend_constants
    assert_equal :zenoh_c, Z::BACKEND
    assert_equal Z::C_VERSION, Z::BACKEND_VERSION
    assert_equal Z::PEER, Z::PEER_SUPPORTED
    assert_equal 2.0, Z::DEFAULT_TIMEOUT
    assert_equal "0.4.0", Z::VERSION
  end

  def test_connection_count_and_peers
    assert wait_for { @b.connection_count == 1 }
    warned = with_deprecations do
      assert_equal 1, @b.peers
      assert_equal 1, @b.peers
    end
    assert_equal 1, warned.size, "once per name"
    assert_match(/Session#peers is deprecated.*connection_count/, warned[0])
  end

  def test_get_timeout_in_seconds_and_keywords
    qa = @a.queryable("c8/q/**", depth: 4)
    @settle.call
    g = @b.get("c8/q/one", timeout: 1.5, params: "p=1", payload: "body", attachment: "att")
    assert wait_for { qa.pending == 1 }
    q = qa.each_pending[0]
    assert_equal ["p=1", "body", "att"], [q.params, q.payload, q.attachment]
    q.reply("c8/q/one", "ok")
    q.finish
    assert wait_for { g.done? }
    assert_equal "ok", g.each_reply[0][1]
    # nobody answers: the get ends within its time limit
    t = Time.now
    g2 = @b.get("c8/none", timeout: 0.2)
    assert wait_for(3) { g2.done? }
    assert_operator Time.now - t, :<, 2.5
  end

  def test_time_given_twice
    assert_raises(ArgumentError) { @b.get("c8/x", timeout: 1, timeout_ms: 1000) }
    assert_raises(ArgumentError) { @b.liveliness_get("c8/x", timeout: 1, timeout_ms: 1000) }
    assert_raises(ArgumentError) { Z.scout(timeout: 1, timeout_ms: 1000) }
    assert_raises(ArgumentError) { @b.querier("c8/x", timeout: 1, timeout_ms: 1000) }
    with_deprecations do
      assert_raises(ArgumentError) { @b.get("c8/x", 1000, timeout: 1) }
    end
    assert_raises(ArgumentError) { @b.get("c8/x", timeout: 0.0001) }
    assert_raises(TypeError) { @b.get("c8/x", timeout: "2") }
  end

  def test_positional_time_warns_once
    warned = with_deprecations do
      @b.get("c8/x", 100)
      @b.get("c8/x", 100, nil, "payload")
      @b.liveliness_get("c8/x", 100)
    end
    assert_equal 2, warned.size
    assert_match(/Session#get with the time limit as a positional argument/, warned[0])
    assert_match(/Session#liveliness_get/, warned[1])
  end

  def test_positional_float_warns_about_seconds
    warned = with_deprecations { @b.get("c8/x", 2.0) }
    assert_equal 1, warned.size
    assert_match(/Float/, warned[0])
    assert_match(/waits 2 ms/, warned[0])
    assert_match(/timeout: \(seconds\)/, warned[0])
  end

  def test_deprecations_modes
    Asterism.reset_deprecations
    assert_raises(Asterism::DeprecationError) { @b.connection_count && @b.peers }
    Asterism.deprecations = :silent
    _, err = capture_io { @b.peers }
    assert_equal "", err
    assert_raises(ArgumentError) { Asterism.deprecations = :loud }
  ensure
    Asterism.deprecations = :raise
  end

  def test_depth_keywords
    sub = @a.subscribe("c8/d", depth: 2)
    w = @a.liveliness_watch("c8/alive/**", depth: 2)
    qa = @a.queryable("c8/dq", depth: 2, complete: true)
    assert_raises(ArgumentError) { @a.subscribe("c8/d", 2, depth: 2) }
    assert_raises(ArgumentError) { @a.subscribe("c8/d", depth: 0) }
    @settle.call
    3.times { |i| @b.put("c8/d", i.to_s) }
    assert wait_for { sub.dropped == 1 }
    assert_equal %w[1 2], sub.each_pending.map { |e| e[1] }
    [sub, w, qa].each(&:close)
    ev = @a.transport_events(depth: 4)
    ev.close
    pub = @a.publisher("c8/p")
    pub.matching_listener(depth: 4).close
    pub.close
  end

  def test_one_argument_reply_warns_when_it_will_change
    plain = @a.queryable("c8/r/plain")
    wild = @a.queryable("c8/w/**")
    @settle.call
    g1 = @b.get("c8/r/*", timeout: 1.0)
    g2 = @b.get("c8/w/x", timeout: 1.0)
    assert wait_for { plain.pending == 1 && wild.pending == 1 }
    warned = with_deprecations do
      plain.each_pending { |q| q.reply("a") }  # the query's key is a pattern
      wild.each_pending { |q| q.reply("b") }   # the queryable's key has a wildcard: no change in 1.0
    end
    assert_equal 1, warned.size
    assert_match(/Query#reply\(payload\)/, warned[0])
    assert wait_for { g1.done? && g2.done? }
    assert_equal "b", g2.each_reply[0][1]
  end

  def test_reply_error_and_delete
    qa = @a.queryable("c8/e")
    @settle.call
    g = @b.get("c8/e", timeout: 1.0)
    assert wait_for { qa.pending == 1 }
    qa.each_pending do |q|
      q.reply_error("bad")
      q.reply_delete
    end
    assert wait_for { g.done? }
    got = g.each_result
    assert(got.any? { |r| r.error? && r.payload == "bad" })
    assert(got.any? { |r| r.ok? && r.kind == :delete })
  end

  def test_open_with_block_and_connect_timeout
    port = TestHelper.free_port
    loc = "tcp/127.0.0.1:#{port}"
    kept = nil
    v = Z::Session.open(nil, mode: :peer, listen: loc, connect_timeout: 1.0) do |s|
      kept = s
      refute s.closed?
      :done
    end
    assert_equal :done, v
    assert kept.closed?
    t = Time.now
    assert_raises(Z::Error) { Z::Session.open("tcp/127.0.0.1:#{TestHelper.free_port}", connect_timeout: 0.5) }
    assert_operator Time.now - t, :<, 2.5
    assert_raises(ArgumentError) do
      Z::Session.open(loc, connect_timeout: 1, config: { "connect/timeout_ms" => 100 })
    end
    assert_raises(ArgumentError) { Z::Session.open(loc, connect_timeout: 1, config: "{}") }
  end

  def test_querier_timeout_in_seconds
    qr = @b.querier("c8/qr", timeout: 0.2)
    t = Time.now
    g = qr.get(params: "x=1")
    assert wait_for(3) { g.done? }
    assert_operator Time.now - t, :<, 2.5
    qr.close
  end

  def test_value_predicates
    assert Z::Sample.new(key: "k", payload: "p", express: true).express?
    refute Z::Sample.new(key: "k", payload: "p").express?
    assert Z::Transport.new("z", :peer, true, true).multicast?
    refute Z::Link.new("z", "a", "b", 1, false, :reliable, [], nil, nil).streamed?
  end

  def test_session_after_fork
    skip "no fork" unless Process.respond_to?(:fork)
    rd, wr = IO.pipe
    pid = fork do
      rd.close
      begin
        @b.put("c8/fork", "x")
        wr.write("no error")
      rescue Z::ClosedError => e
        wr.write("closed: #{e.message}")
      end
      wr.write(@b.closed? ? " closed?" : " open?")
      wr.close
      exit!(0)
    end
    wr.close
    got = rd.read
    Process.wait(pid)
    assert_match(/\Aclosed: .*after fork closed\?\z/, got)
    refute @b.closed?, "the parent's session is untouched"
  end
end
