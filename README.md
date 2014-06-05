# Consistent

Consistent hash ((c) Yura Sokolov aka funny_fulcon) Ruby wrapper

## Installation

Add this line to your application's Gemfile:

    gem 'consistent'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install consistent

## Usage

### Create Ring

```ruby
ring = Consistent::Ring.new
```

### Adding nodes to Ring

```ruby
ring.add node: 'server1.mydomain.cc', weight: 100, status: :alive
ring.add node: 'server2.mydomain.cc' # weight: 100 and status: :alive are default values
ring.add node: 'server3.mydomain.cc', weight: 50
# Commit your changes
ring.refresh!
```

Or you can add nodes with one kick

```ruby
ring.add! [{ node: 'server1.mydomain.cc' },
           { node: 'server2.mydomain.cc', status: :dead },
           { node: 'server3.mydomain.cc', weight: 50 }]
```

Or you can pass nodes to constructor and ring will be refreshed explicitly

```ruby
ring = Consistent::Ring.new [{ node: 'server1.mydomain.cc' },
                             { node: 'server2.mydomain.cc', status: :dead },
                             { node: 'server3.mydomain.cc', weight: 50 }]
```

### Updating node statuses

```ruby
ring.update node: 'server1.mydomain.cc', status: :dead
ring.update node: 'server2.mydomain.cc', status: :dead
ring.refresh!

# Same as
ring.update [{ node: 'server1.mydomain.cc', status: :dead }, 
             { node: 'server2.mydomain.cc', status: :dead }]
ring.refresh!

# Or bang function that will refresh ring explicitly
ring.update! [{ node: 'server1.mydomain.cc', status: :dead }, 
              { node: 'server2.mydomain.cc', status: :dead }]
```

#### Note, 
you can't change weight while update. You can change only status. To change weight you should replace old nodes with new ones.

### Replacing all nodes with new ones

```ruby
ring.replace! [{ node: 'new_serverA.mydomain.cc' },
               { node: 'new_serverB.mydomain.cc' }]
```

### Getting nodes

```ruby
ring.get "some value"
#=> 'server2.mydomain.cc'

# You can define how many results to be returned
ring.get "some value", 2
#=> ['server2.mudomain.cc', 'server3.mydomain.cc']

# Or even return all values
ring.get "some value", :all
#=> ['server2.mudomain.cc', 'server3.mydomain.cc']
```