
var redis = {};
var debug = require("debug")("test:redis");


var r = module.exports = {
  data: redis,
  get: function(id, done) {
    debug("get");
    done(null, redis[id]);
  },
  //all sets are fire & forget
  set: function(id, value) {
    debug("set");
    redis[id] = value;
  },
  del: function(id, done) {
    debug("del");
    delete redis[id];
    done();
  }
};


//r.set("54375a7687dfe3ef71eb0eb5", 1);
//r.set("54375a778432d9fa713b6575", 2);
