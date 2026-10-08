# One session used from several Ruby threads at once, and closed (or lost)
# while they use it: the calls either work or raise Asterism::Zenoh::Error,
# never crash. Asterism's CRuby layer polls a session from a receiving
# thread while the application puts and gets from its own.
require_relative "helper"

class TestZenohThreads < Minitest::Test
  include TestHelper

  ROUNDS = 30

  def test_concurrent_use_and_close
    ROUNDS.times do |i|
      a, b = open_pair
      sub = a.subscribe("c5/race/**", 64)
      # b is used by the threads; it is closed under them, or (peer link)
      # loses its only peer when a closes.
      victim = i.odd? ? b : a
      user = b
      pub = user.publisher("c5/race/p")
      qr = user.querier("c5/race/q", timeout_ms: 50)
      workers = [
        -> { pub.put("z" * 32, timestamp: true) },
        -> { qr.get.each_result },
        -> { user.delete("c5/race/x") },
        -> { user.put("c5/race/x", "y" * 64) },
        -> { user.poll && user.peers },
        -> { user.liveliness("c5/race/t#{rand(1000)}").close },
        -> { user.get("c5/race/q", 50).each_reply },
        -> { sub.each_pending }
      ]
      stop = false
      threads = workers.map do |w|
        Thread.new do
          until stop
            begin
              break if w.call == false
            rescue Asterism::Zenoh::Error
              break
            end
          end
        end
      end
      sleep(rand * 0.02)
      victim.close
      sleep 0.01
      stop = true
      threads.each(&:join)
      assert victim.closed?
      a.close
      b.close
      assert a.closed? && b.closed?
    end
  end

  def test_two_threads_take_from_one_subscriber
    a, b = open_pair
    sub = a.subscribe("c5/share", 256)
    sleep 0.3
    got = Queue.new
    stop = false
    takers = 2.times.map do
      Thread.new do
        until stop
          sub.each_pending { |_k, v, _att| got << v }
          sleep 0.001
        end
      end
    end
    100.times { |i| b.put("c5/share", i.to_s) }
    assert wait_for { got.size == 100 }
    stop = true
    takers.each(&:join)
    seen = []
    seen << got.pop until got.empty?
    assert_equal (0...100).map(&:to_s).sort, seen.sort, "each sample taken exactly once"
  ensure
    b&.close
    a&.close
  end
end
