# asterism-zenoh: Asterism::Zenoh for CRuby, a C extension over zenoh-c.
#
#   rake                 fetch zenoh-c, build the extension, run the tests
#   rake zenoh_c:fetch   download the release pinned in ZENOH_C_PIN (sha256 checked)
#   rake compile         build lib/asterism/asterism_zenoh.so (+ libzenohc.so)
#   rake test            two sessions over a local peer link (no router needed);
#                        ASTERISM_TEST_ROUTER=tcp/host:7447 runs them through a router
#   rake clean           remove the build products (vendor/ stays)
#   rake gem             build pkg/asterism-zenoh-<version>.gem
require "fileutils"
require "rbconfig"
require_relative "ext/asterism_zenoh/zenoh_c"

ROOT = __dir__
VENDOR = File.join(ROOT, "vendor")
# ZENOH_C_DIR points the build at another zenoh-c (include/ and lib/), the
# same variable extconf.rb reads; then nothing is fetched.
ZENOH_C_DIR = ENV["ZENOH_C_DIR"].to_s.empty? ? File.join(VENDOR, "zenoh-c") : File.expand_path(ENV["ZENOH_C_DIR"])
EXT_SRC = File.join(ROOT, "ext/asterism_zenoh")
EXT_BUILD = File.join(ROOT, "tmp/ext")
EXT_LIB = File.join(ROOT, "lib/asterism")
DLEXT = RbConfig::CONFIG["DLEXT"]

namespace :zenoh_c do
  desc "Download the pinned prebuilt zenoh-c into vendor/zenoh-c (sha256 checked)"
  task :fetch do
    next unless ENV["ZENOH_C_DIR"].to_s.empty?
    key = ZenohCFetch.platform_key
    sha = ZenohCFetch.pin["sha256.#{key}"]
    stamp = File.join(ZENOH_C_DIR, ".pin")
    next if sha && File.exist?(stamp) && File.read(stamp).strip == sha
    begin
      got = ZenohCFetch.fetch(ZENOH_C_DIR, key)
    rescue ZenohCFetch::Error => e
      abort e.message
    end
    File.write(stamp, got["sha256"] + "\n")
    puts "zenoh-c #{got['tag']} (#{key}) in #{ZENOH_C_DIR}"
  end
end

desc "Build the asterism-zenoh C extension"
task compile: "zenoh_c:fetch" do
  FileUtils.mkdir_p(EXT_BUILD)
  Dir.chdir(EXT_BUILD) do
    sh({ "ZENOH_C_DIR" => ZENOH_C_DIR }, RbConfig.ruby, File.join(EXT_SRC, "extconf.rb"))
    sh "make", "-s"
  end
  FileUtils.mkdir_p(EXT_LIB)
  FileUtils.cp(File.join(EXT_BUILD, "asterism_zenoh.#{DLEXT}"), EXT_LIB)
  FileUtils.cp(File.join(ZENOH_C_DIR, "lib", ZenohCFetch.lib_name), EXT_LIB)
end

desc "Run the tests (two sessions over a local peer link, no router needed)"
task test: :compile do
  Dir.glob(File.join(ROOT, "test/test_*.rb")).sort.each do |t|
    sh RbConfig.ruby, "-I", File.join(ROOT, "lib"), t
  end
end

desc "Reconnection over a router that restarts (needs docker and the zenohd image of zenoh-c's release)"
task test_reconnect: :compile do
  require "socket"
  require "timeout"
  port = TCPServer.new("127.0.0.1", 0).then { |sv| sv.addr[1].tap { sv.close } }
  image = ENV["ZENOHD_IMAGE"] || "eclipse/zenoh:#{File.read(File.join(ROOT, "ZENOH_C_PIN"))[/^tag:\s*(\S+)/, 1]}"
  name = "cruby-reconnect-#{Process.pid}"
  sh "docker run -d --rm --name #{name} -p 127.0.0.1:#{port}:7447 #{image} " \
     "--no-multicast-scouting -l tcp/0.0.0.0:7447 > /dev/null"
  locator = "tcp/127.0.0.1:#{port}"
  lib = File.join(ROOT, "lib")
  stay = nil
  begin
    sleep 1.5
    stay = IO.popen([RbConfig.ruby, "-I", lib, File.join(ROOT, "test/reconnect/stay.rb"), locator], err: [:child, :out])
    line = Timeout.timeout(10) { stay.gets }
    abort "stay.rb did not start: #{line.inspect}" unless line&.start_with?("ready")
    puts "stay: #{line}"
    sh "docker restart -t 1 #{name} > /dev/null"
    out = []
    Timeout.timeout(30) do
      while (l = stay.gets)
        puts "stay: #{l}"
        out << l
        break if l.start_with?("back", "closed")
      end
    end
    abort "no reconnection" unless out.last.to_s.start_with?("back")
    ok = system(RbConfig.ruby, "-I", lib, File.join(ROOT, "test/reconnect/newcomer.rb"), locator)
    rest = Timeout.timeout(15) { stay.read }
    puts rest.lines.map { "stay: #{_1}" }.join
    abort "the declarations did not come back" unless ok && rest.include?("summary 2 1 token-open")
    puts "reconnection: ok"
  ensure
    if stay
      begin
        Process.kill("TERM", stay.pid)
      rescue Errno::ESRCH
        nil
      end
      stay.close
    end
    system("docker rm -f #{name} > /dev/null 2>&1")
  end
end

desc "Remove the build products (vendor/ stays)"
task :clean do
  FileUtils.rm_rf(File.join(ROOT, "tmp"))
  FileUtils.rm_f(Dir.glob(File.join(EXT_LIB, "*.{#{DLEXT},so,dylib}")))
end

desc "Build the gem file into pkg/ (only builds; publishing is a separate, manual step)"
task :gem do
  require_relative "lib/asterism/zenoh/version"
  FileUtils.mkdir_p(File.join(ROOT, "pkg"))
  Dir.chdir(ROOT) do
    sh "gem", "build", "asterism-zenoh.gemspec", "--output", "pkg/asterism-zenoh-#{Asterism::Zenoh::VERSION}.gem"
  end
end

task default: :test
