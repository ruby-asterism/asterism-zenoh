# Asterism::Zenoh between two CRuby sessions: put / subscribe, get /
# queryable, liveliness, attachments, the bounded queues, and closing.
require_relative "helper"

class TestZenoh < Minitest::Test
  include TestHelper

  def setup
    @a, @b = open_pair
    # Declarations travel to the other side; give them a moment.
    @settle = -> { sleep 0.3 }
  end

  def teardown
    @b.close if @b
    @a.close if @a
  end

  def test_constants
    assert_equal "1.10.1", Z::C_VERSION
    assert_equal 3000, Z::CONNECT_TIMEOUT_MS
    assert_equal 3000, Z::SEND_TIMEOUT_MS
    assert Z::PEER
    assert Z::MAX_PEERS.positive?
    refute Object.const_defined?(:Zenoh), "no top-level Zenoh without asterism/zenoh/global"
  end

  def test_session_info
    assert @a.poll
    assert @b.poll(8)
    refute @a.closed?
    assert_match(/\A\h+\z/, @a.zid)
    refute_equal @a.zid, @b.zid
    assert_equal 1, @b.connection_count
  end

  def test_put_subscribe_with_attachment
    sub = @a.subscribe("c1/test/**")
    @settle.call
    @b.put("c1/test/x", "hello \xff".b, attachment: "\x01\x02".b)
    @b.put("c1/test/y", "plain")
    assert wait_for { sub.pending == 2 }
    got = sub.each_pending
    assert_equal [["c1/test/x", "hello \xff".b, "\x01\x02".b], ["c1/test/y", "plain", nil]], got
    assert_equal Encoding::ASCII_8BIT, got[0][1].encoding
    assert_equal Encoding::UTF_8, got[0][0].encoding
    assert_equal 2, sub.received
    assert_equal 0, sub.pending
    @b.put("c1/test/z", "blk")
    assert wait_for { sub.pending == 1 }
    seen = []
    n = sub.each_pending { |k, v, att| seen << [k, v, att] }
    assert_equal 1, n
    assert_equal [["c1/test/z", "blk", nil]], seen
  end

  def test_own_puts_are_not_delivered
    sub = @b.subscribe("c1/own")
    @a.subscribe("c1/own")
    @settle.call
    @b.put("c1/own", "x")
    sleep 0.3
    assert_equal 0, sub.pending
  end

  def test_full_queue_drops_the_oldest
    sub = @a.subscribe("c1/drop", 2)
    @settle.call
    5.times { |i| @b.put("c1/drop", i.to_s) }
    assert wait_for { sub.received == 5 }
    assert_equal 2, sub.pending
    assert_equal 3, sub.dropped
    assert_equal %w[3 4], sub.each_pending.map { |e| e[1] }
  end

  def test_get_queryable
    qa = @a.queryable("c1/q/**")
    @settle.call
    g = @b.get("c1/q/one", timeout: 2.0, params: "a=1;b=2", payload: "body", attachment: "qatt")
    assert wait_for { qa.pending == 1 }
    n = qa.each_pending do |q|
      assert_equal "c1/q/one", q.key
      assert_equal "a=1;b=2", q.params
      assert_equal "body", q.payload
      assert_equal "qatt", q.attachment
      q.reply("r1", attachment: "ratt")
      q.reply("c1/q/one", "r2")
    end
    assert_equal 1, n
    assert wait_for { g.done? }
    assert_equal [["c1/q/one", "r1", "ratt"], ["c1/q/one", "r2", nil]], g.each_reply
    assert_equal 0, g.errors
  end

  def test_query_kept_without_block_and_finish
    qa = @a.queryable("c1/k/**")
    @settle.call
    g = @b.get("c1/k/x")
    assert wait_for { qa.pending == 1 }
    qs = qa.each_pending
    assert_equal 1, qs.size
    q = qs[0]
    refute q.finished?
    refute g.done?
    q.reply("late")
    q.finish
    assert q.finished?
    assert_raises(Z::Error) { q.reply("again") }
    assert wait_for { g.done? }
    assert_equal "late", g.each_reply[0][1]
  end

  def test_get_nobody_answers_ends_at_once
    t = Time.now
    g = @b.get("c1/nobody/here", timeout_ms: 2000)
    assert wait_for(1.5) { g.done? }
    assert Time.now - t < 1.5
    assert_equal [], g.each_reply
  end

  def test_get_times_out
    qa = @a.queryable("c1/slow")
    @settle.call
    t = Time.now
    g = @b.get("c1/slow", timeout: 0.3)
    assert wait_for(3) { g.done? }
    took = Time.now - t
    assert took >= 0.25, "done after #{took}s"
    assert_equal [], g.each_reply
    qa.close
  end

  def test_target_and_complete
    plain = @a.queryable("c1/t/x")
    complete = @a.queryable("c1/t/**", 16, complete: true)
    @settle.call
    g = @b.get("c1/t/x", target: :all_complete, consolidation: :none)
    assert wait_for { complete.pending == 1 }
    sleep 0.2
    assert_equal 0, plain.pending, "a queryable that is not complete gets no ALL_COMPLETE query"
    complete.each_pending { |q| q.reply("c1/t/x", "ok") }
    assert wait_for { g.done? }
    assert_equal "ok", g.each_reply[0][1]
    assert_raises(ArgumentError) { @b.get("c1/t/x", timeout_ms: 100, target: :bogus) }
    assert_raises(ArgumentError) { @b.get("c1/t/x", timeout_ms: 100, consolidation: :bogus) }
  end

  def test_liveliness
    w = @a.liveliness_watch("c1/alive/**")
    @settle.call
    tok = @b.liveliness("c1/alive/b")
    assert wait_for { w.pending >= 1 }
    assert_equal [["c1/alive/b", true]], w.each_pending
    lg = @a.liveliness_get("c1/alive/**")
    assert wait_for { lg.done? }
    assert_equal [["c1/alive/b", "", nil]], lg.each_reply
    tok.close
    assert tok.closed?
    assert wait_for { w.pending >= 1 }
    assert_equal [["c1/alive/b", false]], w.each_pending
    # A watch started later reports the tokens alive now first.
    tok2 = @b.liveliness("c1/alive/c")
    @settle.call
    w2 = @a.liveliness_watch("c1/alive/**")
    assert wait_for { w2.pending >= 1 }
    assert_equal [["c1/alive/c", true]], w2.each_pending
    tok2.close
  end

  def test_arguments
    assert_raises(ArgumentError) { @a.subscribe("c1/bad//key") }
    assert_raises(ArgumentError) { @a.subscribe("c1/x", 0) }
    assert_raises(ArgumentError) { @a.get("c1/x", timeout_ms: 0) }
    assert_raises(TypeError) { @a.put("c1/x", 1) }
    assert_raises(TypeError) { @a.put("c1/x", "v", attachment: 1) }
    assert_raises(ArgumentError) { @a.put("c1/x", "v", bogus: 1) }
    assert_raises(ArgumentError) { Z::Session.open("tcp/127.0.0.1:1", listen: "tcp/127.0.0.1:2") }
    assert_raises(ArgumentError) { Z::Session.open(nil) }
    assert_raises(ArgumentError) { Z::Session.open("tcp/127.0.0.1:1", mode: :router) }
    assert_raises(NoMethodError) { Z::Session.new }
  end

  def test_open_fails_without_anyone_to_talk_to
    t = Time.now
    assert_raises(Z::Error) { Z::Session.open("tcp/127.0.0.1:#{TestHelper.free_port}") }
    assert Time.now - t < Z::CONNECT_TIMEOUT_MS / 1000.0 + 2
  end

  def test_close_and_after
    sub = @a.subscribe("c1/after")
    @settle.call
    @b.put("c1/after", "kept")
    assert wait_for { sub.pending == 1 }
    @a.close
    @a.close # idempotent
    assert @a.closed?
    refute @a.poll
    assert sub.closed?
    assert_equal [["c1/after", "kept", nil]], sub.each_pending, "values received before close stay readable"
    assert_raises(Z::Error) { @a.put("c1/after", "x") }
    assert_raises(Z::Error) { @a.subscribe("c1/after") }
    assert_equal 0, @a.connection_count
  end
end

# The connecting side notices that the other side went away (the rule of
# the mruby gem: a session that only connects closes when it has nobody
# left; poll false, closed? true, put raises).
class TestZenohLost < Minitest::Test
  include TestHelper

  def test_connecting_peer_closes_when_the_listener_goes
    skip "peer link only" if TestHelper.router
    loc = "tcp/127.0.0.1:#{TestHelper.free_port}"
    a = Z::Session.open(nil, mode: :peer, listen: loc)
    b = Z::Session.open(loc, mode: :peer)
    assert b.poll
    a.close
    assert wait_for(5) { !b.poll }
    assert b.closed?
    assert_raises(Z::Error) { b.put("c1/x", "y") }
  end

  def test_listener_stays_open_without_peers
    skip "peer link only" if TestHelper.router
    loc = "tcp/127.0.0.1:#{TestHelper.free_port}"
    a = Z::Session.open(nil, mode: :peer, listen: loc)
    b = Z::Session.open(loc, mode: :peer)
    assert wait_for { a.connection_count == 1 }
    b.close
    assert wait_for { a.connection_count == 0 }
    assert a.poll
    refute a.closed?
    a.close
  end
end
