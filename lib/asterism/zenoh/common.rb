# The part of Asterism::Zenoh written in Ruby that is the same on every
# backend: this file is byte for byte the same as mrblib/common.rb of the
# mruby / PicoRuby binding (picoruby-asterism-zenoh). It runs on CRuby,
# mruby and PicoRuby, so it keeps to what all three have (no Regexp, no
# Data, while loops where a block would be called from C).
#
# What it adds on top of the C binding:
# - the deprecation helper (Asterism.deprecated, Asterism.deprecations=),
#   shared by every Asterism gem;
# - the time limit in seconds (timeout:) next to milliseconds (timeout_ms:)
#   on get and liveliness_get, and keywords for the positional optionals;
# - connection_count's old name peers, and the one-argument Query#reply
#   that will change in 1.0, both with a deprecation warning;
# - Error#code and the readable constant names.
module Asterism
  # The root of every Asterism error (the binding's C code defines it first).
  class Error < StandardError; end

  # Raised instead of a warning when Asterism.deprecations is :raise.
  class DeprecationError < Error; end

  # How a deprecated call is reported: :warn (once per name, the default),
  # :raise (DeprecationError; for CI) or :silent. The environment variable
  # ASTERISM_DEPRECATIONS (warn / raise / silent) sets the default.
  def self.deprecations
    if @deprecations.nil?
      mode = nil
      begin
        mode = ENV["ASTERISM_DEPRECATIONS"] if Object.const_defined?(:ENV)
      rescue StandardError
        mode = nil
      end
      @deprecations = (mode == "raise" || mode == "silent") ? mode.to_sym : :warn
    end
    @deprecations
  end

  def self.deprecations=(mode)
    m = mode.to_s.to_sym
    raise ArgumentError, "deprecations must be :warn, :raise or :silent" unless m == :warn || m == :raise || m == :silent
    @deprecations = m
  end

  # Reports that `old` is deprecated in favour of `instead`: warns once per
  # `old` per process (per VM on the boards), raises DeprecationError, or
  # stays silent (Asterism.deprecations). `why` is an optional sentence.
  def self.deprecated(old, instead, why = nil)
    mode = deprecations
    return nil if mode == :silent
    msg = "asterism: #{old} is deprecated and goes away in 1.0; use #{instead}"
    msg = "#{msg} (#{why})" if why
    raise DeprecationError, msg if mode == :raise
    @deprecated_seen ||= {}
    return nil if @deprecated_seen[old]
    @deprecated_seen[old] = true
    if respond_to?(:warn, true)
      warn(msg)
    else
      puts(msg)
    end
    nil
  end

  # The names warned about so far (for tests). @api private
  def self.deprecated_names
    (@deprecated_seen || {}).keys
  end

  # Forget the names warned about (for tests). @api private
  def self.reset_deprecations
    @deprecated_seen = {}
    nil
  end

  # A time limit in milliseconds from the ways a call may take it: timeout:
  # (seconds), timeout_ms: or the old positional milliseconds (deprecated).
  # Giving more than one raises ArgumentError. @api private
  def self.time_ms(where, timeout, timeout_ms, positional, default_ms)
    given = 0
    given += 1 unless timeout.nil?
    given += 1 unless timeout_ms.nil?
    given += 1 unless positional.nil?
    raise ArgumentError, "#{where}: give the time limit once (timeout: in seconds, or timeout_ms:)" if given > 1
    unless positional.nil?
      if positional.is_a?(Float)
        deprecated("#{where} with a Float as the positional time limit", "timeout: (seconds)",
                   "the positional time limit is milliseconds, so #{positional} waits #{positional.to_i} ms; " \
                   "a Float there is usually meant as seconds")
      else
        deprecated("#{where} with the time limit as a positional argument", "timeout: (seconds) or timeout_ms:")
      end
      return positional.to_i
    end
    unless timeout_ms.nil?
      raise TypeError, "#{where}: timeout_ms: must be a number" unless timeout_ms.is_a?(Numeric)
      return timeout_ms.round
    end
    return default_ms if timeout.nil?
    raise TypeError, "#{where}: timeout: must be a number of seconds" unless timeout.is_a?(Numeric)
    ms = (timeout * 1000).round
    raise ArgumentError, "#{where}: timeout: #{timeout} s is less than 1 ms" if ms < 1
    ms
  end

  # A queue depth given positionally (kept) or as depth: (not both).
  # @api private
  def self.depth_of(where, args, depth, default)
    raise ArgumentError, "#{where}: too many arguments" if args.size > 1
    raise ArgumentError, "#{where}: give the depth once (positional or depth:)" if args.size == 1 && !depth.nil?
    return args[0] if args.size == 1
    depth.nil? ? default : depth
  end

  module Zenoh
    # The readable name of PEER: whether this build has peer mode.
    PEER_SUPPORTED = PEER
    # The default time limit of get and liveliness_get, in seconds.
    DEFAULT_TIMEOUT = 2.0

    class Error
      # The result code of zenoh-c / zenoh-pico (a negative Integer) when the
      # failure came with one, else nil.
      attr_reader :code
    end

    class Session
      alias_method :__asterism_get, :get
      alias_method :__asterism_liveliness_get, :liveliness_get
      alias_method :__asterism_subscribe, :subscribe
      alias_method :__asterism_queryable, :queryable
      alias_method :__asterism_liveliness_watch, :liveliness_watch

      # get(key, timeout: 2.0, params: nil, payload: nil, attachment: nil,
      #     target: :all, consolidation: :none, ...) -> Get.
      # timeout: seconds; or timeout_ms:. The old positional form
      # get(key, timeout_ms, params, payload) still works (deprecated).
      def get(key, *args, timeout: nil, timeout_ms: nil, params: nil, payload: nil, **opts)
        raise ArgumentError, "get: wrong number of arguments (given #{args.size + 1}, expected 1..4)" if args.size > 3
        raise ArgumentError, "get: params given twice" if args.size > 1 && !params.nil?
        raise ArgumentError, "get: payload given twice" if args.size > 2 && !payload.nil?
        ms = ::Asterism.time_ms("Session#get", timeout, timeout_ms, args[0], 2000)
        params = args[1] if args.size > 1
        payload = args[2] if args.size > 2
        __asterism_get(key, ms, params, payload, **opts)
      end

      # liveliness_get(key, timeout: 2.0) -> Get (or timeout_ms:).
      def liveliness_get(key, *args, timeout: nil, timeout_ms: nil)
        raise ArgumentError, "liveliness_get: wrong number of arguments (given #{args.size + 1}, expected 1..2)" if args.size > 1
        __asterism_liveliness_get(key, ::Asterism.time_ms("Session#liveliness_get", timeout, timeout_ms, args[0], 2000))
      end

      # subscribe(key, depth = 16) or subscribe(key, depth: 16).
      def subscribe(key, *args, depth: nil)
        __asterism_subscribe(key, ::Asterism.depth_of("subscribe", args, depth, 16))
      end

      # queryable(key, depth = 16, complete: false) or with depth:.
      def queryable(key, *args, depth: nil, **opts)
        __asterism_queryable(key, ::Asterism.depth_of("queryable", args, depth, 16), **opts)
      end

      # liveliness_watch(key, depth = 16) or with depth:.
      def liveliness_watch(key, *args, depth: nil)
        __asterism_liveliness_watch(key, ::Asterism.depth_of("liveliness_watch", args, depth, 16))
      end

      # Deprecated: the number of connections is connection_count.
      def peers
        ::Asterism.deprecated("Session#peers", "Session#connection_count",
                              "for a client session it counts routers, not peers")
        connection_count
      end
    end

    class Query
      alias_method :__asterism_reply, :reply

      # reply(payload) answers on the query's key today. From 1.0 it answers
      # on the queryable's own key when that key has no wildcard (as the
      # CRuby block API does); a reply that would change warns now.
      def reply(*args, **kw)
        if args.size == 1
          own = @asterism_queryable_key
          if own && !own.include?("*") && !own.include?("$") && own != key
            ::Asterism.deprecated("Query#reply(payload) for a query whose key differs from the queryable's",
                                  "q.reply(key, payload)",
                                  "from 1.0 a reply with only a payload answers on the queryable's own key")
          end
        end
        __asterism_reply(*args, **kw)
      end
    end
  end
end
