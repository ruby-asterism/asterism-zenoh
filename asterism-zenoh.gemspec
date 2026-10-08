require_relative "lib/asterism/zenoh/version"

Gem::Specification.new do |s|
  s.name = "asterism-zenoh"
  s.version = Asterism::Zenoh::VERSION
  s.summary = "Asterism::Zenoh: an unofficial Zenoh binding for Ruby (over zenoh-c)"
  s.description = "Sessions, put / subscribe, get / queryable, liveliness and attachments, " \
                  "received by polling. The same Ruby API as Asterism's mruby / PicoRuby binding."
  s.authors = ["Katsuhiko Kageyama"]
  s.license = "MIT"
  s.required_ruby_version = ">= 3.2"
  s.files = Dir["lib/**/*.rb", "ext/**/*.{c,rb}", "README.md", "LICENSE", "ZENOH_C_PIN"]
  s.extensions = ["ext/asterism_zenoh/extconf.rb"]
  s.require_paths = ["lib"]
end
