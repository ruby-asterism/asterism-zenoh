# The queues of gets, liveliness gets and liveliness watches: a router
# answers a wildcard in one burst, so the defaults hold a burst of far more
# than 16, depth: bounds it, and dropped counts what did not fit.
require_relative "helper"

class TestDepth < Minitest::Test
  include TestHelper

  BURST = 40

  def setup
    @a, @b = open_pair
    @settle = -> { sleep 0.3 }
  end

  def teardown
    @b.close if @b
    @a.close if @a
  end

  # Answers the one query qa gets with BURST replies.
  def answer_burst(qa, prefix)
    assert wait_for { qa.pending == 1 }
    qa.each_pending do |q|
      BURST.times { |i| q.reply("#{prefix}/k#{i}", i.to_s) }
    end
  end

  def test_constants
    assert_equal 16, Z::DEFAULT_DEPTH
    assert_equal 1024, Z::DEFAULT_GET_DEPTH
    assert_equal 1024, Z::DEFAULT_WATCH_DEPTH
    assert_operator Z::MAX_DEPTH, :>=, Z::DEFAULT_GET_DEPTH
  end

  def test_get_burst_is_kept_by_default
    qa = @a.queryable("dp/g/**")
    @settle.call
    g = @b.get("dp/g/**")
    answer_burst(qa, "dp/g")
    assert wait_for { g.done? }
    replies = g.each_reply
    assert_equal BURST, replies.size
    assert_equal (0...BURST).map(&:to_s), replies.map { |r| r[1] }
    assert_equal 0, g.dropped
  end

  def test_get_depth_bounds_the_queue
    qa = @a.queryable("dp/h/**")
    @settle.call
    g = @b.get("dp/h/**", depth: 8)
    answer_burst(qa, "dp/h")
    assert wait_for { g.done? }
    replies = g.each_reply
    assert_equal 8, replies.size
    assert_equal BURST - 8, g.dropped
    assert_equal (BURST - 8...BURST).map(&:to_s), replies.map { |r| r[1] }, "the oldest go"
    assert_raises(ArgumentError) { @b.get("dp/h/**", depth: 0) }
    assert_raises(ArgumentError) { @b.get("dp/h/**", depth: Z::MAX_DEPTH + 1) }
  end

  def test_querier_get_depth
    qa = @a.queryable("dp/q/**")
    qr = @b.querier("dp/q/**", timeout: 2.0)
    @settle.call
    g = qr.get(depth: 4)
    answer_burst(qa, "dp/q")
    assert wait_for { g.done? }
    assert_equal 4, g.each_reply.size
    assert_equal BURST - 4, g.dropped
    g2 = qr.get
    answer_burst(qa, "dp/q")
    assert wait_for { g2.done? }
    assert_equal BURST, g2.each_reply.size
    assert_equal 0, g2.dropped
    qr.close
  end

  def test_liveliness_get_burst
    tokens = (0...BURST).map { |i| @a.liveliness("dp/lv/t#{i}") }
    @settle.call
    g = @b.liveliness_get("dp/lv/**")
    assert wait_for { g.done? }
    assert_equal BURST, g.each_reply.size
    assert_equal 0, g.dropped
    small = @b.liveliness_get("dp/lv/**", timeout: 2.0, depth: 5)
    assert wait_for { small.done? }
    assert_equal 5, small.each_reply.size
    assert_equal BURST - 5, small.dropped
    tokens.each(&:close)
  end

  def test_liveliness_watch_burst
    tokens = (0...BURST).map { |i| @a.liveliness("dp/lw/t#{i}") }
    @settle.call
    w = @b.liveliness_watch("dp/lw/**")
    small = @b.liveliness_watch("dp/lw/**", depth: 4)
    assert wait_for { w.received == BURST && small.received == BURST }
    assert_equal BURST, w.each_pending.size
    assert_equal 0, w.dropped
    assert_equal 4, small.each_pending.size
    assert_equal BURST - 4, small.dropped
    [w, small].each(&:close)
    tokens.each(&:close)
  end

  def test_warn_once
    o = Object.new
    _, err = capture_io do
      assert Asterism.warn_once(o, "asterism: lost 3")
      refute Asterism.warn_once(o, "asterism: lost 4")
      assert Asterism.warn_once(Object.new, "asterism: lost 5")
    end
    assert_equal ["asterism: lost 3", "asterism: lost 5"], err.split("\n")
  end
end
