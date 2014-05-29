require 'spec_helper'

describe Consistent::Ring do
  let(:nodes){ [ { node: "second", weight: 100, status: :alive },
                 { node: "theverylast", weight: 100, status: :alive },
                 { node: "dead", weight: 100, status: :dead } ] }
  let(:alive_nodes){ nodes.select{ |n| n[:status] == :alive }}
  let(:ring){ Consistent::Ring.new nodes }

  describe "new" do
    it "should create empty ring" do
      ring = Consistent::Ring.new
      ring.get("").must_equal nil
    end

    it "should create ring with nodes" do
      ring = Consistent::Ring.new nodes
      all = ring.get("", :all)
      all.size.must_equal 2
      all.sort.must_equal ["second", "theverylast"].sort
    end
  end

  describe "add" do
    let(:ring){ Consistent::Ring.new }

    it "should not add nodes before refresh" do
      ring.add nodes[0]
      ring.add nodes[1]
      ring.add nodes[2]
      ring.get("", :all).size.must_equal 0
      ring.refresh!
      ring.get("", :all).size.must_equal alive_nodes.size
    end

    it "should add! without refresh" do
      ring.add! nodes
      ring.get("", :all).size.must_equal alive_nodes.size
    end
  end

  describe "get" do
    it "should raise an error without token" do
      proc{ ring.get(nil) }.must_raise RuntimeError
    end

    it "should return 1 item by default with explicit cnt" do
      (String === ring.get("")).must_equal true
    end

    it "should return an Array with implicit cnt" do
      (Array === ring.get("", 1)).must_equal true
    end

    it "should return not more then asked" do
      ring.get("", 1).size.must_equal 1
      ring.get("", 2).size.must_equal alive_nodes.size
      ring.get("", 20).size.must_equal alive_nodes.size
    end

    it "should return all" do
      ring.get("", :all).size.must_equal alive_nodes.size
    end

    it "should return only alive" do
      ring.get("", :all).sort.must_equal alive_nodes.map{ |n| n[:node] }.sort
    end
  end

  describe "update" do
    it "should not update before refresh" do
      ring.update node: "theverylast", status: :dead
      ring.get("", :all).size.must_equal 2
      ring.refresh!
      ring.get("", :all).size.must_equal 1
    end

    it "should update! without refresh" do
      ring.update! node: "theverylast", status: :dead
      ring.get("", :all).size.must_equal 1
      ring.update! node: "theverylast", status: :alive
      ring.get("", :all).size.must_equal 2
    end
  end
end