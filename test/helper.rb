# Shared by the tests. Two sessions talk over a local peer link (one
# listens on a free port, the other connects), so no router is needed. With
# ASTERISM_TEST_ROUTER=tcp/host:7447 both are clients of that router instead.
require "minitest/autorun"
require "socket"
require "asterism/zenoh"

module TestHelper
  Z = Asterism::Zenoh

  def self.free_port
    s = TCPServer.new("127.0.0.1", 0)
    port = s.addr[1]
    s.close
    port
  end

  def self.router
    r = ENV["ASTERISM_TEST_ROUTER"]
    r.nil? || r.empty? ? nil : r
  end

  # [listening or client session, connecting or client session]
  def open_pair
    if (r = TestHelper.router)
      [Z::Session.open(r), Z::Session.open(r)]
    else
      loc = "tcp/127.0.0.1:#{TestHelper.free_port}"
      a = Z::Session.open(nil, mode: :peer, listen: loc)
      b = Z::Session.open(loc, mode: :peer)
      [a, b]
    end
  end

  # Waits (polling) until the block is true or the time is up.
  def wait_for(timeout = 3.0)
    t = Time.now
    until yield
      return false if Time.now - t > timeout
      sleep 0.01
    end
    true
  end
end
