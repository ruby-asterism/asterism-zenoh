require_relative "lib/asterism/zenoh/version"

Gem::Specification.new do |s|
  s.name = "asterism-zenoh"
  s.version = Asterism::Zenoh::VERSION
  s.summary = "Asterism::Zenoh: an unofficial Zenoh binding for Ruby (over zenoh-c)"
  s.description = "Sessions, put / subscribe, get / queryable, liveliness and attachments, " \
                  "received by polling. The same Ruby API as Asterism's mruby / PicoRuby binding. " \
                  "The C extension is compiled at install time against the official prebuilt " \
                  "zenoh-c release for the machine, downloaded and checked against a pinned sha256."
  s.authors = ["Katsuhiko Kageyama"]
  s.homepage = "https://github.com/ruby-asterism/asterism-zenoh"
  s.metadata = {
    "homepage_uri" => s.homepage,
    "source_code_uri" => "https://github.com/ruby-asterism/asterism-zenoh",
    "bug_tracker_uri" => "https://github.com/ruby-asterism/asterism-zenoh/issues",
    "rubygems_mfa_required" => "true"
  }
  # Both apply, each to its own files: MIT for this gem's own code; zenoh-c,
  # installed next to the extension, is used under Apache-2.0; its notices
  # (licenses/zenoh-c) are installed with it.
  s.licenses = ["MIT", "Apache-2.0"]
  s.required_ruby_version = ">= 3.2"
  # What `gem install` needs: the extension's source and extconf.rb with its
  # helper (zenoh_c.rb), ZENOH_C_PIN (assets and sha256), and the license
  # texts installed next to zenoh-c.
  s.files = Dir["lib/**/*.rb", "ext/**/*.{c,rb}", "licenses/zenoh-c/*",
                "README.md", "LICENSE", "ZENOH_C_PIN"]
  s.extensions = ["ext/asterism_zenoh/extconf.rb"]
  s.require_paths = ["lib"]
end
