# Builds the C extension of asterism-zenoh against a prebuilt zenoh-c.
#
# zenoh-c is looked up in $ZENOH_C_DIR, else in vendor/zenoh-c of the
# asterism-zenoh repository (`rake zenoh_c:fetch` puts the pinned release there,
# see ZENOH_C_PIN). It is linked as a shared library: libzenohc.so is copied
# next to the extension (`rake compile` does it) and found through
# rpath $ORIGIN, so the built lib/ directory works wherever it is.
require "mkmf"

repo = File.expand_path("../..", __dir__)
zdir = ENV["ZENOH_C_DIR"] || File.join(repo, "vendor", "zenoh-c")
inc = File.join(zdir, "include")
lib = File.join(zdir, "lib")
unless File.exist?(File.join(inc, "zenoh.h")) && File.exist?(File.join(lib, "libzenohc.so"))
  abort "zenoh-c not found in #{zdir} (run `rake zenoh_c:fetch`, or set ZENOH_C_DIR)"
end

$INCFLAGS << " -I#{inc}"
$CFLAGS << " -std=gnu11 -Wall -Wno-unused-parameter"
$LDFLAGS << " -L#{lib} -Wl,-rpath,'$$ORIGIN'"
have_library("pthread") || abort("pthread is needed")
have_library("zenohc", "z_open", "zenoh.h") || abort("cannot link libzenohc")

create_makefile("asterism/asterism_zenoh")
