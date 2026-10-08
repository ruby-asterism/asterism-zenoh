# Finds the prebuilt zenoh-c release for this machine, downloads it and
# checks it against ZENOH_C_PIN. Used by extconf.rb (at `gem install`) and by
# the Rakefile (`rake zenoh_c:fetch`). Standard library only: net/http for
# the download (redirects followed, proxies from the usual environment
# variables), zlib for the zip archive (no unzip, curl or git needed).
#
# Environment:
#   ZENOH_C_DIR             use this zenoh-c (include/ and lib/) instead
#   ASTERISM_ZENOH_C_MIRROR download from this base instead of the GitHub
#                           releases: <base>/<tag>/<asset>. An http(s) or
#                           file:// URL, or a local directory.
require "digest"
require "fileutils"
require "net/http"
require "rbconfig"
require "tmpdir"
require "uri"
require "zlib"

module ZenohCFetch
  class Error < StandardError; end

  PIN_FILE = File.expand_path("../../ZENOH_C_PIN", __dir__)
  MIRROR_ENV = "ASTERISM_ZENOH_C_MIRROR"
  MAX_REDIRECTS = 10

  module_function

  def pin
    @pin ||= File.readlines(PIN_FILE).each_with_object({}) do |l, h|
      next if l.start_with?("#") || !l.include?(":")
      k, v = l.split(":", 2)
      h[k.strip] = v.strip
    end
  end

  # The pinned machines, as the keys of ZENOH_C_PIN (asset.<key>).
  def supported
    pin.keys.grep(/\Aasset\./).map { |k| k.delete_prefix("asset.") }
  end

  # The key of this machine in ZENOH_C_PIN, or nil when it is not known.
  def platform_key(cpu = RbConfig::CONFIG["host_cpu"], os = RbConfig::CONFIG["host_os"])
    arch = case cpu
           when /\A(x86_64|amd64|x64)\z/ then "x86_64"
           when /\A(aarch64|arm64)\z/ then "aarch64"
           end
    return nil unless arch
    case os
    when /linux-musl/ then "#{arch}-linux-musl"
    when /linux/ then "#{arch}-linux"
    when /darwin/ then "#{arch}-darwin"
    end
  end

  # The shared library's file name on this machine.
  def lib_name(os = RbConfig::CONFIG["host_os"])
    os =~ /darwin/ ? "libzenohc.dylib" : "libzenohc.so"
  end

  def help
    "Point the build at a zenoh-c of your own (its include/ and lib/, release #{pin['tag']}):\n" \
      "  ZENOH_C_DIR=/path/to/zenoh-c gem install asterism-zenoh\n" \
      "  (or: gem install asterism-zenoh -- --with-zenoh-c-dir=/path/to/zenoh-c)\n" \
      "Prebuilt zenoh-c releases: #{pin['repo']}/releases/tag/#{pin['tag']}\n" \
      "To download from a mirror instead: #{MIRROR_ENV}=<base> (fetches <base>/#{pin['tag']}/<asset>)."
  end

  def url_for(asset)
    base = ENV[MIRROR_ENV].to_s
    base = "#{pin['repo']}/releases/download" if base.empty?
    "#{base.chomp('/')}/#{pin['tag']}/#{asset}"
  end

  # Downloads the pinned release for `key`, checks its sha256 and unpacks
  # include/ and the shared library into `dest`. Returns a description of
  # what was fetched. Raises Error with a message for the user.
  def fetch(dest, key = platform_key, log: $stdout)
    unless key && pin["asset.#{key}"]
      raise Error, "no prebuilt zenoh-c is pinned for this machine " \
                   "(#{RbConfig::CONFIG['host_cpu']}-#{RbConfig::CONFIG['host_os']}); " \
                   "pinned: #{supported.join(', ')}.\n#{help}"
    end
    asset = pin["asset.#{key}"]
    sha = pin["sha256.#{key}"] or raise Error, "ZENOH_C_PIN has no sha256 for #{key}"
    url = url_for(asset)
    Dir.mktmpdir("zenoh-c") do |tmp|
      zip = File.join(tmp, asset)
      log&.puts "fetching zenoh-c #{pin['tag']} (#{key}): #{url}"
      begin
        download(url, zip)
      rescue StandardError => e
        raise Error, "cannot download #{url}: #{e.message} (#{e.class})\n#{help}"
      end
      got = Digest::SHA256.file(zip).hexdigest
      unless got == sha
        raise Error, "sha256 mismatch for #{asset}: got #{got}, pinned #{sha} (ZENOH_C_PIN). " \
                     "Not using it.\n#{help}"
      end
      log&.puts "sha256 #{got} OK"
      FileUtils.rm_rf(dest)
      unzip(zip, dest) { |name| name.start_with?("include/") || name == "lib/#{lib_name}" }
    end
    %w[include/zenoh.h].push("lib/#{lib_name}").each do |f|
      raise Error, "the archive #{asset} has no #{f}" unless File.file?(File.join(dest, f))
    end
    { "tag" => pin["tag"], "platform" => key, "asset" => asset, "sha256" => sha, "url" => url }
  end

  def download(url, path, redirects = MAX_REDIRECTS)
    uri = URI(url)
    if uri.scheme.nil? || uri.scheme == "file"
      src = uri.scheme ? URI.decode_www_form_component(uri.path) : url
      raise Error, "no such file: #{src}" unless File.file?(src)
      return FileUtils.cp(src, path)
    end
    raise Error, "unsupported URL scheme: #{uri.scheme}" unless %w[http https].include?(uri.scheme)
    Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https",
                                        open_timeout: 30, read_timeout: 120) do |http|
      http.request_get(uri.request_uri) do |res|
        case res
        when Net::HTTPRedirection
          raise Error, "too many redirects" if redirects <= 0
          loc = res["location"] or raise Error, "redirect without a location"
          return download(URI.join(url, loc).to_s, path, redirects - 1)
        when Net::HTTPSuccess
          File.open(path, "wb") { |f| res.read_body { |chunk| f.write(chunk) } }
        else
          raise Error, "HTTP #{res.code} #{res.message}"
        end
      end
    end
  end

  # Extracts the entries for which the block is true. Reads the central
  # directory; stored and deflated entries; checks each entry's CRC-32.
  def unzip(zip, dest)
    File.open(zip, "rb") do |f|
      size = f.size
      tail = [size, 65_557].min
      f.seek(size - tail)
      buf = f.read(tail)
      eocd = buf.rindex("PK\x05\x06".b) or raise Error, "not a zip archive: #{zip}"
      count, cd_size, cd_off = buf[eocd + 10, 10].unpack("vVV")
      raise Error, "zip64 archives are not supported" if cd_off == 0xFFFFFFFF
      f.seek(cd_off)
      cd = f.read(cd_size)
      pos = 0
      count.times do
        raise Error, "bad zip central directory" unless cd[pos, 4] == "PK\x01\x02".b
        method, _t, _d, crc, csize, usize, nlen, elen, clen = cd[pos + 10, 24].unpack("vvvVVVvvv")
        local = cd[pos + 42, 4].unpack1("V")
        name = cd[pos + 46, nlen]
        pos += 46 + nlen + elen + clen
        next if name.end_with?("/") || !yield(name)
        raise Error, "unsafe path in zip: #{name}" if name.start_with?("/") || name.split("/").include?("..")
        f.seek(local)
        lh = f.read(30)
        raise Error, "bad zip local header: #{name}" unless lh[0, 4] == "PK\x03\x04".b
        f.seek(local + 30 + lh[26, 2].unpack1("v") + lh[28, 2].unpack1("v"))
        data = f.read(csize)
        data = case method
               when 0 then data
               when 8 then Zlib::Inflate.new(-Zlib::MAX_WBITS).inflate(data)
               else raise Error, "zip entry #{name}: compression method #{method} is not supported"
               end
        raise Error, "zip entry #{name}: size mismatch" unless data.bytesize == usize
        raise Error, "zip entry #{name}: CRC mismatch" unless Zlib.crc32(data) == crc
        out = File.join(dest, name)
        FileUtils.mkdir_p(File.dirname(out))
        File.binwrite(out, data)
      end
    end
  end
end
