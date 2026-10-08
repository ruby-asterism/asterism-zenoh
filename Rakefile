# asterism-zenoh: Asterism::Zenoh for CRuby, a C extension over zenoh-c.
#
#   rake                 fetch zenoh-c, build the extension, run the tests
#   rake zenoh_c:fetch   download the release pinned in ZENOH_C_PIN (sha256 checked)
#   rake compile         build lib/asterism/asterism_zenoh.so (+ libzenohc.so)
#   rake test            two sessions over a local peer link (no router needed);
#                        ASTERISM_TEST_ROUTER=tcp/host:7447 runs them through a router
#   rake clean           remove the build products (vendor/ stays)
require "digest"
require "fileutils"
require "rbconfig"

ROOT = __dir__
VENDOR = File.join(ROOT, "vendor")
# ZENOH_C_DIR points the build at another zenoh-c (include/ and lib/), the
# same variable extconf.rb reads; then nothing is fetched.
ZENOH_C_DIR = ENV["ZENOH_C_DIR"].to_s.empty? ? File.join(VENDOR, "zenoh-c") : File.expand_path(ENV["ZENOH_C_DIR"])
EXT_SRC = File.join(ROOT, "ext/asterism_zenoh")
EXT_BUILD = File.join(ROOT, "tmp/ext")
EXT_LIB = File.join(ROOT, "lib/asterism")
DLEXT = RbConfig::CONFIG["DLEXT"]

def pin
  @pin ||= File.readlines(File.join(ROOT, "ZENOH_C_PIN")).each_with_object({}) do |l, h|
    next if l.start_with?("#") || !l.include?(":")
    k, v = l.split(":", 2)
    h[k.strip] = v.strip
  end
end

def platform_key
  cpu = RbConfig::CONFIG["host_cpu"]
  os = RbConfig::CONFIG["host_os"]
  return "x86_64-linux" if cpu =~ /x86_64|amd64/ && os =~ /linux/
  abort "no prebuilt zenoh-c is pinned for #{cpu}-#{os} (ZENOH_C_PIN; or set ZENOH_C_DIR)"
end

namespace :zenoh_c do
  desc "Download the pinned prebuilt zenoh-c into vendor/zenoh-c"
  task :fetch do
    next unless ENV["ZENOH_C_DIR"].to_s.empty?
    key = platform_key
    asset = pin["asset.#{key}"] or abort "ZENOH_C_PIN has no asset for #{key}"
    sha = pin["sha256.#{key}"] or abort "ZENOH_C_PIN has no sha256 for #{key}"
    stamp = File.join(ZENOH_C_DIR, ".pin")
    next if File.exist?(stamp) && File.read(stamp).strip == sha
    url = "#{pin['repo']}/releases/download/#{pin['tag']}/#{asset}"
    FileUtils.mkdir_p(VENDOR)
    zip = File.join(VENDOR, asset)
    unless File.exist?(zip) && Digest::SHA256.file(zip).hexdigest == sha
      sh "curl", "-sSfL", "-o", zip, url
    end
    got = Digest::SHA256.file(zip).hexdigest
    abort "sha256 mismatch for #{asset}: #{got} (pinned #{sha})" unless got == sha
    FileUtils.rm_rf(ZENOH_C_DIR)
    FileUtils.mkdir_p(ZENOH_C_DIR)
    sh "unzip", "-q", "-o", zip, "-d", ZENOH_C_DIR
    abort "the archive has no include/zenoh.h" unless File.exist?(File.join(ZENOH_C_DIR, "include/zenoh.h"))
    File.write(stamp, sha + "\n")
    puts "zenoh-c #{pin['tag']} (#{key}) in #{ZENOH_C_DIR}"
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
  FileUtils.cp(File.join(ZENOH_C_DIR, "lib/libzenohc.so"), EXT_LIB)
end

desc "Run the tests (two sessions over a local peer link, no router needed)"
task test: :compile do
  Dir.glob(File.join(ROOT, "test/test_*.rb")).sort.each do |t|
    sh RbConfig.ruby, "-I", File.join(ROOT, "lib"), t
  end
end

desc "Remove the build products (vendor/ stays)"
task :clean do
  FileUtils.rm_rf(File.join(ROOT, "tmp"))
  FileUtils.rm_f(Dir.glob(File.join(EXT_LIB, "*.{#{DLEXT},so}")))
end

task default: :test
