# Checks an installed asterism-zenoh gem (run by CI after `gem install
# --local pkg/asterism-zenoh-<version>.gem`, from outside the repository and
# without -I): that `require` loads the installed extension and zenoh-c, and
# that two sessions over a local peer link get a put through. Not one of
# the `rake test` files (those run against the repository's lib/).
#
#   ruby test/installed_check.rb
require "socket"
require "asterism/zenoh"

def check(cond, what)
  abort "installed_check: FAILED: #{what}" unless cond
  puts "ok: #{what}"
end

Z = Asterism::Zenoh
spec = Gem.loaded_specs["asterism-zenoh"]
check(spec, "loaded as a gem (not from a source tree)")
ext = $LOADED_FEATURES.find { |f| f.match?(%r{/asterism/asterism_zenoh\.(so|bundle|dylib)\z}) }
check(ext && (ext.start_with?(spec.full_gem_path) || ext.start_with?(spec.extension_dir)),
      "extension from the installed gem: #{ext}")
lib = %w[libzenohc.so libzenohc.dylib].map { |n| File.join(File.dirname(ext), n) }.find { |f| File.exist?(f) }
check(lib, "zenoh-c next to the extension: #{lib}")
check(File.exist?(File.join(File.dirname(ext), "zenoh-c", "LICENSE-APACHE")), "zenoh-c license installed")
check(Z::C_VERSION == "1.10.1", "C_VERSION #{Z::C_VERSION}")

srv = TCPServer.new("127.0.0.1", 0)
port = srv.addr[1]
srv.close
loc = "tcp/127.0.0.1:#{port}"
a = Z::Session.open(nil, mode: :peer, listen: loc)
b = Z::Session.open(loc, mode: :peer)
begin
  sub = a.subscribe("installed/check/**")
  got = []
  t = Time.now
  # Declarations travel to the other side; repeat the put until it arrives.
  while got.empty? && Time.now - t < 10
    b.put("installed/check/x", "hello".b, attachment: "att".b)
    sleep 0.2
    got.concat(sub.each_pending)
  end
  check(got.first == ["installed/check/x", "hello".b, "att".b], "put / subscribe over a peer link: #{got.first.inspect}")
  check(Z::KeyExpr.new("installed/*").includes?("installed/check"), "KeyExpr")
  check(b.new_timestamp.to_time.is_a?(Time), "Timestamp")
ensure
  b.close
  a.close
end
puts "installed_check: all ok (#{RUBY_DESCRIPTION})"
