var should = require("should");
var pisum = require('../');
var ObjectId = require('bson').ObjectID;
var collection = require("./mock/mongo.js");
var cache = require("./mock/redis.js");

var test_pisum = pisum(collection, pisum.redis.number(cache, {property:"test"}));
var test_pisum_json = pisum(collection, pisum.redis.json(cache));

describe("CRUD Tests:", function(){
  var id = ObjectId();

  it("should put it in the collection and the cache", function(done){
    test_pisum.insert({ _id:id, test: 3}, function(err, data) {
      data.should.have.property("_id");
      data._id.should.eql(id);
      data.test.should.be.type('number');
      collection.data.should.have.property(id);
      cache.data.should.have.property(id);
      done();
    });
  });

  it("should retrieve it from cache when it exists", function() {
    test_pisum.find(id, function(err, data) {
      should.exist(data);
      data.should.have.property("_id");
      data._id.should.eql(id);
      data.test.should.be.type('number');
    });
  });

  it("should update the collection and delete from the cache", function() {
    test_pisum.update(id, { test:4 }, function(err) {
      collection.data[id].test.should.eql(4);
      cache.data.should.not.have.property(id);
    });
  });

  it("should retrieve it from the collection if it isn't in the cache and then put it in the cache", function() {
    test_pisum.find(id, function(err, data) {
      should.exist(data);
      data.should.have.property("_id");
      data._id.should.eql(id);
      cache.data.should.have.property(id);
    });
  });

  it("should delete from the collection and the cache", function() {
    test_pisum.remove(id, function(err) {
      cache.data.should.not.have.property(id);
      collection.data.should.not.have.property(id);
    });
  });

  it("should not find deleted documents", function() {
    test_pisum.find(id, function(err, data) {
      should.not.exist(data);
    });
  });

  it("should get/set JSON", function(done) {
    var data = { _id:id, foo:{ bar:"baz" }};
    test_pisum_json.insert(data, function(err, data) {
      data.should.have.property("_id");
      data._id.should.eql(id);
      collection.data.should.have.property(id);
      cache.data[id].should.eql(JSON.stringify(data));

      test_pisum_json.find(id, function(err, found) {
        should.exist(found);
        found.should.eql(data);
        done();
      });
    });
  });

})
