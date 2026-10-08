# Asterism::Zenoh for CRuby: the Zenoh binding of Asterism, over zenoh-c.
# The same Ruby API as the mruby / PicoRuby gem (picoruby-asterism-zenoh):
# see README.md. Receiving is polled (each_pending / each_reply) from the
# application's own thread; nothing calls into Ruby behind its back.
require_relative "zenoh/version"
require_relative "asterism_zenoh"
