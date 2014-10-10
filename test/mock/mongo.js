var ObjectId = require('bson').ObjectID;
var collection = {};
var debug = require("debug")("test:mongo");

var m = module.exports = {
  data: collection,
  id_key: "_id",
  findOne: function(query, projection, callback) {
    debug("findOne");
    var doc = collection[query[module.exports.id_key]];
    callback(null, doc);
  },
  update: function(query, data, options, callback) {
    debug("update");
    var doc = collection[query[module.exports.id_key]];
    if(doc) {
      for(var i in data) doc[i] = data[i];
    }
    if(callback) callback();
  },
  insert: function(data, options, callback) {
    debug("insert");
    var id = data[module.exports.id_key];
    if(!id) id = data[module.exports.id_key] = ObjectId();

    collection[data[module.exports.id_key]] = data;
    if(callback) callback(null, data);
  },
  remove: function(query, callback) {
    debug("remove");
    delete collection[query[module.exports.id_key]];
    if(callback) callback();
  }
};

//m.insert({ _id:"54375a7687dfe3ef71eb0eb5", test: 1 });
//m.insert({ _id:"54375a778432d9fa713b6575", test: 2 });
//console.log(collection);
//
//m.findOne({ _id:"54375a7687dfe3ef71eb0eb5" }, {}, function(err, data) {
//  console.log(data);
//});
//
//m.update({ _id:"54375a7687dfe3ef71eb0eb5" }, { test: 3 });
//console.log(collection["54375a7687dfe3ef71eb0eb5"]);
//
//m.remove({ _id:"54375a7687dfe3ef71eb0eb5" });
//console.log(collection["54375a7687dfe3ef71eb0eb5"]);


