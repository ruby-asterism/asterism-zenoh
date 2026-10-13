# The newcomer (rake test_reconnect): after the router restarted, checks
# that what stay.rb declared before the restart is there again.
require "asterism/zenoh"
Z = Asterism::Zenoh
s = Z::Session.open(ARGV[0])
ok = true
wait = ->(g) { t = Time.now; sleep 0.02 until g.done? || Time.now - t > 3 }
g = s.liveliness_get("rc/a", timeout: 2.0)
wait.call(g)
n = g.each_reply.size
puts "token: #{n}"
ok &&= n == 1
g = s.get("rc/q", timeout: 2.0)
wait.call(g)
r = g.each_reply
puts "query: #{r.map { |e| e[1] }.inspect}"
ok &&= r.size == 1 && r[0][1] == "answer"
sleep 0.3
s.put("rc/x", "hello")
s.put("rc/x", "stop")
sleep 0.5
s.close
exit(ok ? 0 : 1)
