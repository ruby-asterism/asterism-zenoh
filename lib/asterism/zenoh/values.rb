# The values the C extension hands out (samples, replies, events), and the
# Ruby half of scouting. Plain immutable Data objects: they work with
# pattern matching (case sample in {kind: :delete, key:}).
module Asterism
  module Zenoh
    # A received sample, from Subscriber#each_sample and
    # AdvancedSubscriber#each_sample. payload and attachment are binary
    # Strings (attachment nil when there was none). kind is :put or
    # :delete; encoding a String such as "application/json"; timestamp an
    # Asterism::Zenoh::Timestamp or nil (zenoh only stamps samples when the
    # sender or a router does); priority, congestion_control and
    # reliability Symbols; source_zid the sender's Zenoh ID when it said.
    # Sample.new(key, payload, attachment) still works (the other fields
    # have defaults).
    Sample = Data.define(:key, :payload, :attachment, :kind, :encoding, :timestamp, :priority,
                         :congestion_control, :express, :reliability, :source_zid) do
      def initialize(key:, payload:, attachment: nil, kind: :put, encoding: nil, timestamp: nil, priority: nil,
                     congestion_control: nil, express: false, reliability: nil, source_zid: nil)
        super
      end

      # The payload as UTF-8.
      def text
        payload.dup.force_encoding(Encoding::UTF_8)
      end

      def put? = kind == :put
      def delete? = kind == :delete

      def to_s
        "#{key}: #{payload}"
      end
    end

    # A reply to a get, from Get#each_result: an answer (error false) with
    # the fields of a sample, or an error reply (error true: payload and
    # encoding of the error, key nil). replier_zid: who answered, when known.
    # Reply.new(key, payload, attachment) still works.
    Reply = Data.define(:key, :payload, :attachment, :kind, :encoding, :timestamp, :error, :replier_zid) do
      def initialize(key:, payload:, attachment: nil, kind: :put, encoding: nil, timestamp: nil, error: false,
                     replier_zid: nil)
        super
      end

      def text
        payload.dup.force_encoding(Encoding::UTF_8)
      end

      def ok? = !error
      def error? = error

      def to_s
        error ? "error: #{payload}" : "#{key}: #{payload}"
      end
    end

    # An answer to scouting: a router or peer (whatami :router / :peer /
    # :client), its Zenoh ID and the locators it can be reached at.
    Hello = Data.define(:zid, :whatami, :locators)

    # A connection to another peer or router (Session#transports).
    Transport = Data.define(:zid, :whatami, :qos, :multicast)

    # A transport that appeared (kind :added) or went (:removed), from
    # Session#transport_events.
    TransportEvent = Data.define(:kind, :zid, :whatami, :qos, :multicast) do
      def added? = kind == :added
      def removed? = kind == :removed
    end

    # A link (one connection of a transport; Session#links).
    Link = Data.define(:zid, :src, :dst, :mtu, :streamed, :reliability, :interfaces, :group, :auth_identifier)

    # A link that opened (kind :added) or closed (:removed), from
    # Session#link_events.
    LinkEvent = Data.define(:kind, :zid, :src, :dst, :mtu, :streamed, :reliability, :interfaces, :group,
                            :auth_identifier) do
      def added? = kind == :added
      def removed? = kind == :removed
    end

    # Samples that a publisher sent and that never arrived
    # (AdvancedSubscriber#miss_listener): count of them, from the publisher
    # source_zid / source_eid.
    Miss = Data.define(:source_zid, :source_eid, :count)

    SCOUT_WHAT = { router: 1, peer: 2, client: 4 }.freeze

    # Looks for routers and / or peers by multicast scouting for timeout
    # seconds (or timeout_ms milliseconds) and returns what answered, as an
    # Array of Hello. what: :router, :peer, :client, an Array of them, or
    # :all. config: a Hash of zenoh configuration keys, as for
    # Session.open (e.g. {"scouting/multicast/interface" => "eth0"}).
    # Multicast must be allowed on the network (it often is not in
    # containers and VPNs).
    def self.scout(what: %i[router peer], timeout: 1.0, timeout_ms: nil, config: nil)
      kinds = what == :all ? SCOUT_WHAT.keys : Array(what)
      mask = kinds.sum do |k|
        SCOUT_WHAT.fetch(k.to_sym) { raise ArgumentError, "what must be :router, :peer, :client or :all" }
      end
      ms = timeout_ms || (timeout * 1000).round
      _scout(mask, ms, config)
    end
  end
end
