var ObjectId = require('mongodb').ObjectID;

module.exports = exports = function(collection, cache, options) {
  var key = options.key || '_id';
  var key_type = options.key_type || ObjectId;
  var projection = options.projection || {};

  return {
    find: function (id, done) {
      id = id.toString();
      cache.get(id, function(err, value) {
        if (value) return done(null, value);

        var query = {};
        query[key] = ensure(id, key_type);
        collection.findOne(query, projection, function (err, value) {
          if (err) return done(err);
          if (!value) return done();

          cache.set(id, value);
          done(null, value);
        });
      })
    },

    update: function (id, update, done) {
      var query = {};
      query[key] = ensure(id, key_type);
      collection.update(query, update, function(err) {
        if (err) return done(err);
        cache.del(id.toString(), done);
      })
    },

    insert: function (value, done) {
      var id = value._id || new key_type();
      value._id = ensure(id, key_type);

      collection.insert(value, function (err) {
        if (err) return done(err);

        cache.set(id.toString(), value);
        done();
      })
    },

    remove: function (id, done) {
      var query = {};
      query[key] = ensure(id, key_type);

      collection.remove(query, function (err) {
        if (err) return done(err);

        cache.del(id.toString(), done);
      })
    }
  };
};

function default_logger(msg, err) {
  if(err) console.error('[' + new Date().toISOString() + '] (' +pisum.name+ '): ' + msg, err);
}

exports.redis = {
  //a wrapper for a redis <String,String> type
  string: function(redis, options) {
    if(!options) options = {};
    var logerror = options.logerror || default_logger;
    //the property that is being cached
    var property = options.property;

    var wrapper = {
      get: function(id, done) {
        redis.get(id, function(err, value) {
          if (err) logerror("error reading cache:", err);
          value = wrapper.fromValue(value);
          done(null, value);
        })
      },
      //all sets are fire & forget
      set: function(id, value) {
        value = wrapper.toValue(value);
        redis.set(id, value, logerror.bind(this, 'error writing cache:'));
      },
      del: function(id, done) {
        redis.del(id, done);
      },
      toValue: function(value) { return (property? value[property] : value).toString(); },
      fromValue: function(value) { return property? obj(property, value) : value; }
    };
    return wrapper;
  },

  //a wrapper for a redis <String,NumberString> type
  number: function(redis, options) {
    var property = options && options.property;
    if(!property) throw new Error("property option required");

    var wrapper = exports.redis.string(redis, options);
    wrapper.fromValue = function(value) { return obj(property, value); };
    return wrapper;
  },

  //a wrapper for a redis <String,JsonString> type
  json: function(redis, options) {
    var wrapper = exports.redis.string(redis, options);
    wrapper.toValue = JSON.stringify;
    wrapper.fromValue = JSON.parse;
    return wrapper;
  },

  //a wrapper for a redis <String,Hash> type
  hash: function(redis, logerror) {
    throw new Error("Not Implemented");
  },

  //a wrapper for a redis <String,List> type
  list: function(redis, logerror) {
    throw new Error("Not Implemented");
  },

  //a wrapper for a redis <String,Set> type
  set: function(redis, logerror) {
    throw new Error("Not Implemented");
  },

  //a wrapper for a redis <String,SortedSet> type
  soretedset: function(redis, logerror) {
    throw new Error("Not Implemented");
  }
};

function obj(prop, value) {
  var o = {};
  o[prop] = value;
  return o;
}

function ensure(value, type) {
  return new type(value.toString()).valueOf();
}
