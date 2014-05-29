require "consistent/version"
require "consistent_ring"

class Consistent
  class Ring

    STATUSES = {
      dead: 0,
      alive: 1,
      down: 2, 
      default: 1 << 30
    }.freeze

    def initialize(nodes = [])
      @ring = ConsistentRing.new
      @_add = []
      @_update = []
      @_replace = []

      if nodes.any?
        add(nodes)
        refresh!
      end
    end

    def get(token, cnt = nil)
      raise "token can't be nil"  unless token
      
      if cnt == :all
        @ring.get(token, 0, 1)
      else
        if cnt
          @ring.get(token, cnt, nil)
        else
          @ring.get(token, 1, nil).first
        end
      end
    end

    def add(node)
      case node
      when Array
        node.each{ |n| add_one(n) }
      when Hash
        add_one(node)
      else
        raise "Not valid node"
      end
    end

    def add!(node)
      add(node)
      refresh!
    end

    def update(node)
      case node
      when Array
        node.each{ |n| update_one(n) }
      when Hash
        update_one(node)
      else
        raise "Not valid node"
      end
    end

    def replace(nodes)
      nodes.each{ |n| replace_one(n) }
    end

    def replace!(nodes)
      replace(nodes)
      refresh!
    end

    def update!(node)
      update(node)
      refresh!
    end

    def refresh!
      @ring.add(@_add) && @_add.clear  if @_add.any?
      @ring.update(@_update) && @_update.clear  if @_update.any?
      @ring.add(@_replace) && @_replace.clear  if @_replace.any?
    end

    private

    def add_one(node)
      @_add << prepare_add(node)
    end

    def update_one(node)
      @_update << prepare_update(node)
    end

    def replace_one(node)
      @_replace << prepare_replace(node)
    end

    def prepare_add(node)
      node[:status] ||= :alive
      prepare(node)
    end

    def prepare_update(node)
      node[:status] ||= :default
      node = prepare(node)
      node.delete "weight"
      node
    end

    def prepare_replace(node)
      node[:status] ||= :alive
      prepare(node)
    end

    def prepare(node)
      valid_node = {
        "node"   => node[:node] || raise("You should declare node name"),
        "weight" => node[:weight].to_i || 100,
        "status" => status(node)
      }
    end

    def status(node)
      STATUSES[node[:status]] || raise("Bad status")
    end
  end
end
