var ObjectId = require('bson').ObjectID;

module.exports = exports = function(collection, cache, options) {
  if(!options) options = {};
  var key = options.key || '_id';
  var key_type = options.key_type || ObjectId;
  var projection = options.projection || {};
  var default_update_options = options.update_defaults || {w:1};
  var default_insert_options = options.insert_defaults || {w:1};

  return {
    cache: cache,
    collection: collection,

    find: function (id, done) {
      id = id.toString();
      cache.get(id, function(err, value) {
        if (value) {
          value[key] = ensure(id, key_type);
          return done(null, value);
        }

        var query = {};
        query[key] = ensure(id, key_type);
        collection.findOne(query, projection, function (err, data) {
          if (err) return done(err);
          if (!data) return done();

          cache.set(id, data);
          done(null, data);
        });
      })
    },

    update: function (id, data, options, done) {
      done = arguments[arguments.length-1];
      if(done === options || !done) options = default_update_options;
      if(options.multi) throw new Error("multi is not supported via pisum");
      if(!options.w) options.w = 1;

      var query = {};
      query[key] = ensure(id, key_type);
      collection.update(query, data, options, function(err) {
        if (err) return done(err);
        cache.del(id.toString(), done);
      })
    },

    insert: function (data, options, done) {
      done = arguments[arguments.length-1];
      if(done === options || !options) options = default_insert_options;
      if(!options.w) options.w = 1;

      var id = data._id || new key_type();
      data._id = ensure(id, key_type);

      collection.insert(data, options, function (err, data) {
        if (err) return done(err);

        cache.set(id.toString(), data);
        done(null, data);
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
          if(typeof value!=="undefined")
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
    wrapper.fromValue = function(value) { return obj(property, Number(value)); };
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
