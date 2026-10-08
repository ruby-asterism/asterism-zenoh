# Builds the C extension of asterism-zenoh against a prebuilt zenoh-c.
#
# Where zenoh-c comes from:
# 1. ZENOH_C_DIR (or `gem install asterism-zenoh -- --with-zenoh-c-dir=DIR`):
#    a zenoh-c of your own, its include/ and lib/. The Rakefile passes its
#    vendor/zenoh-c this way.
# 2. Otherwise the official prebuilt release pinned in ZENOH_C_PIN for this
#    machine is downloaded and checked against the pinned sha256
#    (zenoh_c.rb). Machines without a pinned release, and machines that
#    cannot download it, stop here with what to do instead.
#
# zenoh-c is linked as a shared library. `make install` puts it next to the
# extension (lib/asterism/ of the installed gem), with the text of the
# Apache License 2.0 and zenoh-c's NOTICE.md in lib/asterism/zenoh-c/; the
# extension finds it through its rpath ($ORIGIN on Linux, @loader_path on
# macOS). Uninstalling the gem removes all of it.
require "mkmf"
require_relative "zenoh_c"

LIB = ZenohCFetch.lib_name
DARWIN = RbConfig::CONFIG["host_os"].match?(/darwin/)
LICENSES = File.expand_path("../../licenses/zenoh-c", __dir__)
STAMP = ".zenoh-c-installed"

zdir = with_config("zenoh-c-dir") || ENV["ZENOH_C_DIR"]
if zdir && !zdir.empty?
  zdir = File.expand_path(zdir)
  source = "zenoh-c from ZENOH_C_DIR (not fetched by this gem)\n"
  fetched = false
  message "using zenoh-c in #{zdir}\n"
else
  zdir = File.expand_path("zenoh-c")
  begin
    got = ZenohCFetch.fetch(zdir)
  rescue ZenohCFetch::Error => e
    abort "asterism-zenoh: #{e.message}"
  end
  source = got.map { |k, v| "#{k}: #{v}\n" }.join
  fetched = true
  message "fetched zenoh-c #{got['tag']} (#{got['platform']}), sha256 #{got['sha256']} OK\n"
end
inc = File.join(zdir, "include")
lib = File.join(zdir, "lib")
unless File.exist?(File.join(inc, "zenoh.h")) && File.exist?(File.join(lib, LIB))
  abort "asterism-zenoh: zenoh-c not found in #{zdir} (needs include/zenoh.h and lib/#{LIB}).\n" +
        ZenohCFetch.help
end
File.write("zenoh-c-SOURCE", source)

$INCFLAGS << " -I#{inc}"
$CFLAGS << " -std=gnu11 -Wall -Wno-unused-parameter"
$LDFLAGS << " -L#{lib}"
$LDFLAGS << (DARWIN ? " -Wl,-rpath,@loader_path" : " -Wl,-rpath,'$$ORIGIN'")
have_library("pthread") || abort("asterism-zenoh: pthread is needed")
have_library("zenohc", "z_open", "zenoh.h") || abort("asterism-zenoh: cannot link #{lib}/#{LIB}")

create_makefile("asterism/asterism_zenoh")

# The prebuilt dylib names itself by the path it was built at; refer to it
# through the rpath instead (and sign the changed bundle again).
postlink = []
if DARWIN
  id = `otool -D #{lib}/#{LIB}`.lines.last.to_s.strip
  abort "asterism-zenoh: cannot read the install name of #{lib}/#{LIB} (otool)" if id.empty?
  unless id.start_with?("@rpath/")
    postlink << "\t$(Q) install_name_tool -change '#{id}' '@rpath/#{LIB}' $@"
    postlink << "\t-$(Q) codesign --force --sign - $@"
  end
end

mk = File.read("Makefile")
unless postlink.empty?
  mk.sub!(/^(\$\(TARGET_SO\):.*\n(?:\t.*\n)*?\t.*\$\(LDSHARED\).*\n)/) { $1 + postlink.join("\n") + "\n" } or
    abort "asterism-zenoh: no link rule in the Makefile"
end
mk << <<~MAKE

  # zenoh-c next to the extension, with its license text and notices.
  ZENOHC_LIB = #{lib}/#{LIB}
  ZENOHC_DOCS = #{LICENSES}/LICENSE-APACHE #{LICENSES}/NOTICE.md
  install-so: install-zenoh-c
  install-zenoh-c: $(TARGET_SO)
  \t$(Q) $(MAKEDIRS) $(RUBYARCHDIR)/zenoh-c
  \t$(INSTALL_PROG) $(ZENOHC_LIB) $(RUBYARCHDIR)
  \t$(INSTALL_DATA) $(ZENOHC_DOCS) $(RUBYARCHDIR)/zenoh-c
  \t$(INSTALL_DATA) zenoh-c-SOURCE $(RUBYARCHDIR)/zenoh-c/SOURCE
MAKE
if fetched
  # RubyGems runs `make clean` before building and again after installing;
  # the downloaded zenoh-c goes with the second one only.
  mk << <<~MAKE
    \t$(Q) touch #{STAMP}
    clean: clean-zenoh-c
    clean-zenoh-c:
    \t-$(Q) if test -f #{STAMP}; then $(RM_RF) zenoh-c zenoh-c-SOURCE #{STAMP}; fi
  MAKE
end
File.write("Makefile", mk)
