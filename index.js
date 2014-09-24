var ObjectId = require('mongodb').ObjectID;

module.exports = exports = function pisum(options) {
	return new Pod(options);
};

function Pod(options) {
	this._name = options.name || 'PISUM';
	this._redis = options.redis;
	this._mongo = options.mongo;
	this._key = options.key || '_id';
	this._key_type = options.key_type || ObjectId;
	this._projection = options.projection || {};
}

Pod.prototype.find = function(id, done) {
	var self = this;
	var id_s = id.toString();

	self._redis.get(id_s, function(err, doc_s) {
		if (err) console.error('[' + new Date().toISOString() + '] (' + self._name + '): error reading document from cache:', err);
		if (doc_s) return done(null, JSON.parse(doc_s));
        var query = {};
        query[self._key] = ensure(id_s, self._key_type);
		self._mongo.findOne(query, self._projection, function(err, doc) {
			if (err) return done(err);
			if (!doc) return done();
			var doc_s = JSON.stringify(doc);
			self._redis.set(id_s, doc_s, function(err) {
				if (err) console.error('[' + new Date().toISOString() + '] (' + self._name + '): error caching document:', err);
			});
			done(null, doc);
		});
	})

};

Pod.prototype.update = function(id, update, done) {
	var self = this;
	var id_s = id.toString();
    var query = {};
    query[self._key] = ensure(id_s, self._key_type);
	self._mongo.update(query, update, function(err) {
		if (err) return done(err);
		self._redis.del(id_s, function(err) {
			if (err) return done(err);
			done();
		})
	})
};

Pod.prototype.insert = function(doc, done) {
	var self = this;
	var id = doc._id || new ObjectId();
	var id_s = id.toString();
	doc._id = ensure(id_s, self._key_type);

	self._mongo.insert(doc, function(err) {
		if (err) return done(err);
		var doc_s = JSON.stringify(doc);
		self._redis.set(id_s, doc_s, function(err) {
			if (err) console.error('[' + new Date().toISOString() + '] (' + self._name + '): error caching document:', err);
		});
		done();
	})
};

Pod.prototype.remove = function(id, done) {
	var self = this;
	var id_s = id.toString();
    var query = {};
    query[self._key] = ensure(id_s, self._key_type);
	self._mongo.remove(query, function(err) {
		if (err) return done(err);
		self._redis.del(id_s, function(err) {
			if (err) return done(err);
			done();
		})
	})
};

function ensure(value, type) {
	return new type(value.toString()).valueOf();
}
