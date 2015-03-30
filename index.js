var redis = require('redis')
  , HashRing = require('hashring')
  , debug =require('debug')('redis-spawn')
  , _ = require('underscore')
  , EventEmitter = require('events').EventEmitter
  , util = require('util');

var Manager = function(cluster) {
  this.cluster = cluster;
  this._redisInstances = {};

  var ringKeys = {};

   _.each(cluster, function(config, options){
    config = _.extend({}, options, config);

    var key = [config.host, config.port].join(":")
      , weight = config.weight || 1

    if (config.password) {
      config.auth_pass = config.password;
    }

    if (!this._redisInstances[key]) {
      var client = redis.createClient(config.port, config.host, config);

      client.on('connect', this._onClientConnect.bind(this, config));
      client.on('error', this._onClientError.bind(this, config));

      if (!_.isUndefined(config.database)) {
        client.select(config.database);
      }

      this._redisInstances[key] = client;
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
    }
  },

  _onClientError: function(config) {
    return function(error) {
      var key = [config.host, config.port].join(':');

      // is it in the ring? removes it so that it wont be called anymore
      if (this._hashRing.has(key)) {
        this._hashRing.remove(key);
      } 
    }
  },

  getInstance: function(key) {
    var instance = this._hashRing.get(key)

    return this._redisInstances[instance];
  },

  eachInstance: function(cb) {
    _.each(this._redisInstances, function(instance){
      cb(instance)
    });
  }
});

module.exports = Manager;