var redis = require('redis')
  , HashRing = require('hashring')
  , debug =require('debug')('redis-spawn')
  , _ = require('underscore')
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
  , when = require('when')

var redisBindFunction = function(client, op) {
  return function() {
    var deferred = when.defer();
    var args = _.values(arguments);
    args.push(function(){
      if (arguments[0]) {
        deferred.reject(arguments[0]);
      } else {
        deferred.resolve(arguments[1]);
      }
    });
    client[op].apply(client, args);
    return deferred.promise;
  }
}

// From https://gist.github.com/tobiash/2884566 and modify to use when 
var liftOps = function(client) {
  var functions, lc, op, ops, p, _i, _len;
  functions = _.functions(client);
  ops = functions.filter(function(f) {
    return f.toUpperCase() === f;
  });
  lc = (function() {
    var _i, _len, _results;
    _results = [];
    for (_i = 0, _len = ops.length; _i < _len; _i++) {
      op = ops[_i];
      _results.push(op.toLowerCase());
    }
    return _results;
  })();
  ops = ops.concat(lc);
  p = {};
  for (_i = 0, _len = ops.length; _i < _len; _i++) {
    op = ops[_i];
    p[op] = redisBindFunction(client, op);
  }
  p["multi"] = p["MULTI"] = function() {
    var m;
    m = client.multi.call(client, arguments);
    m.exec = redisBindFunction(m, 'exec');
    return m;
  };
  return p;
};

var Manager = function(cluster) {
  this.cluster = cluster;
  this._instances = {};
  this._promiseInstances = {};

  var ringKeys = {};

   _.each(cluster, function(config, options){
    config = _.extend({}, options, config);

    var key = [config.host, config.port].join(":")
      , weight = config.weight || 1

    if (config.password) {
      config.auth_pass = config.password;
    }

    if (!this._instances[key]) {
      var client = redis.createClient(config.port, config.host, config);

      client.on('connect', this._onClientConnect.bind(this, config));
      client.on('error', this._onClientError.bind(this, config));

      if (!_.isUndefined(config.database)) {
        client.select(config.database);
      }

      this._instances[key] = client;
      this._promiseInstances[key] = liftOps(client);
      this._promiseInstances[key]._original = client;
    }

    ringKeys[key] = weight;
  }, this);

  this._hashRing = new HashRing(ringKeys);

  EventEmitter.call(this);
}

util.inherits(Manager, EventEmitter);

_.extend(Manager.prototype, {
  _onClientConnect: function(config) {
    return function() {
      var key = [config.host, config.port].join(':');

      // is it in the ring? re-add the server to the ring
      if (!this._hashRing.has(key)) {
        var _params = {};
        _param[key] = config.weight || 1;
        this._hashRing.add(_param);
      }
    }.bind(this);
  },

  _onClientError: function(config) {
    return function(error) {
      var key = [config.host, config.port].join(':');

      // is it in the ring? removes it so that it wont be called anymore
      if (this._hashRing.has(key)) {
        this._hashRing.remove(key);
      } 
    }.bind(this);
  },

  get: function(key, promise) {
    var instance = this._hashRing.get(key);
    if (promise) {
      return this._promiseInstances[instance];
    }
    return this._instances[instance];
  },

  each: function(cb, promise) {
    var instances = this._instances;
    if (promise) {
      instances = this._promiseInstances;
    }

    _.each(instances, function(instance){
      cb(instance)
    });
  }
});

module.exports = Manager;