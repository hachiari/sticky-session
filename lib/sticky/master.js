'use strict';

var cluster = require('cluster');
var util = require('util');
var net = require('net');
var ip = require('ip');
var common = require('_http_common');
var parsers = common.parsers;
var HTTPParser = process.binding('http_parser').HTTPParser;

var debug = require('debug')('sticky:master');

function Master(workerCount, env) {

  net.Server.call(this, {
    pauseOnConnect: true
  }, this.balanceProxyAddress);

  this.env = env || {};

  this.seed = (Math.random() * 0xffffffff) | 0;
  this.workers = [];

  debug('master seed=%d', this.seed);

  this.once('listening', function() {
    debug('master listening on %j', this.address());

    for (var i = 0; i < workerCount; i++)
      this.spawnWorker();
  });
}
util.inherits(Master, net.Server);
module.exports = Master;

Master.prototype.hash = function hash(ip) {
  var hash = this.seed;
  for (var i = 0; i < ip.length; i++) {
    var num = ip[i];

    hash += num;
    hash %= 2147483648;
    hash += (hash << 10);
    hash %= 2147483648;
    hash ^= hash >> 6;
  }

  hash += hash << 3;
  hash %= 2147483648;
  hash ^= hash >> 11;
  hash += hash << 15;
  hash %= 2147483648;

  return hash >>> 0;
};

Master.prototype.spawnWorker = function spawnWorker() {
  var worker = cluster.fork(this.env);

  var self = this;
  worker.on('exit', function(code) {
    debug('worker=%d died with code=%d', worker.process.pid, code);
    self.respawn(worker);
  });

  worker.on('message', function(msg) {
    // Graceful exit
    if (msg.type === 'close')
      self.respawn(worker);
  });

  debug('worker=%d spawn', worker.process.pid);
  this.workers.push(worker);
};

Master.prototype.respawn = function respawn(worker) {
  var index = this.workers.indexOf(worker);
  if (index !== -1)
    this.workers.splice(index, 1);
  this.spawnWorker();
};

function getClientIp(req) {
    // The X-Forwarded-For request header helps you identify the IP address of a client when you use HTTP/HTTPS load balancer.
    // http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/TerminologyandKeyConcepts.html#x-forwarded-for
    // If the value were "client, proxy1, proxy2" you would receive the array ["client", "proxy1", "proxy2"]
    // http://expressjs.com/4x/api.html#req.ips
    var ip = req.headers['x-forwarded-for'] ? req.headers['x-forwarded-for'].split(',')[0] : (req.connection ? req.connection.remoteAddress : "127.0.0.1");
    return ip;
}

Master.prototype.balanceProxyAddress = function balance(socket) {
  var self = this;
  debug('incoming proxy');
  socket.resume();
  socket.once('data', function (buffer) {
      var parser = parsers.alloc();
      parser.reinitialize(HTTPParser.REQUEST);
      parser.onIncoming = function (req) {

          //default to remoteAddress, but check for
          //existence of proxyHeader
          var address = getClientIp(req);
          debug('Proxy Address %j', address);
          var hash = self.hash(ip.toBuffer(address));

          // Pass connection to worker
          // Pack the request with the message
          self.workers[hash % self.workers.length].send(['sticky:balance', buffer.toString('base64')], socket);
      };
      parser.execute(buffer, 0, buffer.length);
      parser.finish();
  });
};
