# Optional short name: require "asterism/zenoh/global" to write Zenoh::Session.
# Not defined by default (the top-level Zenoh is left to an official binding).
require "asterism/zenoh"

Zenoh = Asterism::Zenoh unless defined?(Zenoh)
