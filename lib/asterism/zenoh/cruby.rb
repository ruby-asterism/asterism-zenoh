# What only the CRuby binding (zenoh-c) adds in Ruby: the backend
# constants, Session.open with a block and connect_timeout:, seconds and
# keywords on the CRuby-only calls (querier, advanced subscriber,
# listeners), the long reply names, and scout's argument check.
module Asterism
  module Zenoh
    # Which Zenoh implementation is underneath (:zenoh_pico on the boards).
    BACKEND = :zenoh_c
    # Its version (C_VERSION here, PICO_VERSION on the boards).
    BACKEND_VERSION = C_VERSION

    class Session
      class << self
        alias_method :__asterism_open, :open

        # Session.open(locator = nil, mode:, listen:, scouting:,
        # timestamping:, config:, config_file:, connect_timeout:) -> Session.
        # connect_timeout: seconds to wait for a router or peer
        # (CONNECT_TIMEOUT_MS by default; zenoh's connect/timeout_ms). With a
        # block: yields the session, closes it when the block ends (also on
        # an exception) and returns the block's value.
        def open(locator = nil, connect_timeout: nil, **opts)
          unless connect_timeout.nil?
            ms = ::Asterism.time_ms("Session.open connect_timeout:", connect_timeout, nil, nil, CONNECT_TIMEOUT_MS)
            cfg = opts[:config]
            if cfg.nil?
              opts[:config] = { "connect/timeout_ms" => ms }
            elsif cfg.is_a?(Hash)
              if cfg.key?("connect/timeout_ms") || cfg.key?(:"connect/timeout_ms")
                raise ArgumentError, "connect_timeout: and config: {\"connect/timeout_ms\" => ...} given together"
              end
              opts[:config] = cfg.merge("connect/timeout_ms" => ms)
            else
              raise ArgumentError, "connect_timeout: cannot be added to a config String; set connect/timeout_ms in it"
            end
          end
          s = __asterism_open(locator, **opts)
          return s unless block_given?
          begin
            yield s
          ensure
            s.close
          end
        end
      end

      alias_method :__asterism_querier, :querier
      alias_method :__asterism_advanced_subscriber, :advanced_subscriber
      alias_method :__asterism_transport_events, :transport_events
      alias_method :__asterism_link_events, :link_events

      # querier(key, timeout: 2.0, ...) -> Querier (or timeout_ms:).
      def querier(key, timeout: nil, timeout_ms: nil, **opts)
        ms = ::Asterism.time_ms("Session#querier", timeout, timeout_ms, nil, nil)
        opts[:timeout_ms] = ms unless ms.nil?
        __asterism_querier(key, **opts)
      end

      # advanced_subscriber(key, depth = 16, query_timeout: seconds, ...)
      # (or depth:, query_timeout_ms:).
      def advanced_subscriber(key, *args, depth: nil, query_timeout: nil, query_timeout_ms: nil, **opts)
        d = ::Asterism.depth_of("advanced_subscriber", args, depth, 16)
        ms = ::Asterism.time_ms("Session#advanced_subscriber query_timeout", query_timeout, query_timeout_ms, nil, nil)
        opts[:query_timeout_ms] = ms unless ms.nil?
        __asterism_advanced_subscriber(key, d, **opts)
      end

      # transport_events(depth = 16, history: false) or with depth:.
      def transport_events(*args, depth: nil, **opts)
        __asterism_transport_events(::Asterism.depth_of("transport_events", args, depth, 16), **opts)
      end

      # link_events(depth = 16, history: false) or with depth:.
      def link_events(*args, depth: nil, **opts)
        __asterism_link_events(::Asterism.depth_of("link_events", args, depth, 16), **opts)
      end
    end

    # matching_listener(depth = 16) or matching_listener(depth: 16).
    module MatchingDepth
      def matching_listener(*args, depth: nil)
        super(::Asterism.depth_of("matching_listener", args, depth, 16))
      end
    end
    Publisher.prepend(MatchingDepth)
    AdvancedPublisher.prepend(MatchingDepth)

    class Querier
      prepend MatchingDepth
      alias_method :__asterism_get, :get

      # get(params: nil, payload: nil, attachment:, encoding:, depth:) -> Get; the
      # positional get(params, payload) still works.
      def get(*args, params: nil, payload: nil, **opts)
        raise ArgumentError, "get: wrong number of arguments (given #{args.size}, expected 0..2)" if args.size > 2
        raise ArgumentError, "get: params given twice" if args.size > 0 && !params.nil?
        raise ArgumentError, "get: payload given twice" if args.size > 1 && !payload.nil?
        params = args[0] if args.size > 0
        payload = args[1] if args.size > 1
        __asterism_get(params, payload, **opts)
      end
    end

    class AdvancedSubscriber
      alias_method :__asterism_miss_listener, :miss_listener
      alias_method :__asterism_detect_publishers, :detect_publishers

      def miss_listener(*args, depth: nil)
        __asterism_miss_listener(::Asterism.depth_of("miss_listener", args, depth, 16))
      end

      def detect_publishers(*args, depth: nil, **opts)
        __asterism_detect_publishers(::Asterism.depth_of("detect_publishers", args, depth, 16), **opts)
      end
    end

    class Query
      # The spelled-out names of reply_err and reply_del (those stay).
      def reply_error(payload, **opts)
        reply_err(payload, **opts)
      end

      def reply_delete(key = nil, **opts)
        reply_del(key, **opts)
      end
    end
  end
end
