# Redis, in great number
A simple wrapper around the traditional redis module for nodejs to use multiple redis servers

# Example
```
var options = [
  {
    host: "127.0.0.1",
    port: 6379,
    password: "secret"
  },
  {
    host: "127.0.0.1",
    port: 6380,
    password: "another-secret"
  }
]

var globalOptions = {};

var RedisSpawn = require('redis-spawn');
var cluster = new RedisSpawn(options, globalOptions);

// callback
cluster.get('key').set('key', 'value', function(error, result){});
// or promise
cluster.get('key', true).set('key').then(function(result){}).catch(function(error){});
```

# Features
- Support multiple redis servers using consistent hashing via [hashring module](https://github.com/3rd-Eden/node-hashring)
- Automatically remove failed redis servers from the ring and add them back when they are online again
- Support both callback and promise client

# TODOs
- Test
- Fire events when the redis servers go down or up
- Encapsulate the logic of consistent hashing to allow custom implementation
