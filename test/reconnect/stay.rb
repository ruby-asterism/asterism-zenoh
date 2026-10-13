# The side that stays (rake test_reconnect): a client of the router at
# ARGV[0] declares a liveliness token, a subscriber and a queryable, then
# polls while the router restarts. Prints "ready", "lost" and "back" (with
# milliseconds since start), answers queries on rc/q, and ends on the value
# "stop" on rc/x with "summary <received> <answered>".
require "asterism/zenoh"
Z = Asterism::Zenoh
t0 = Time.now
ms = -> { ((Time.now - t0) * 1000).round }
s = Z::Session.open(ARGV[0])
tok = s.liveliness("rc/a")
sub = s.subscribe("rc/x")
qa = s.queryable("rc/q")
got = []
answered = 0
state = :up
puts "ready auto_reconnect=#{Z::AUTO_RECONNECT}"
$stdout.flush
until Time.now - t0 > 90
  unless s.poll
    puts "closed #{ms.call}"
    break
  end
  now = s.reconnecting? ? :reconnecting : :up
  if now != state
    puts(now == :reconnecting ? "lost #{ms.call}" : "back #{ms.call}")
    $stdout.flush
    s.put("rc/y", "dropped") if now == :reconnecting
    state = now
  end
  qa.each_pending.each do |q|
    q.reply("rc/q", "answer")
    q.finish
    answered += 1
  end
  sub.each_pending.each { |e| got << e[1] }
  break if got.include?("stop")
  sleep 0.005
end
puts "summary #{got.size} #{answered} #{tok.closed? ? 'token-closed' : 'token-open'}"
$stdout.flush
s.close
