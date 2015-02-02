var util = require('util'),
  amqp = require('amqp'),
  assert = require('assert'),
  events = require('events'),
  uuid = require('node-uuid'),
  hostname = require('os').hostname(),

  _ref = require('./protocol'),
  createMessage = _ref.createMessage,
  createEvent = _ref.createEvent;

debug_fn = process.env.NODE_CELERY_DEBUG === '1' ? util.debug : function() {};

function Configuration(options) {
  var self = this;

  for (var o in options) {
    if (options.hasOwnProperty(o)) {
      self[o.replace(/^CELERY_/, '')] = options[o];
    }
  }

  self.BROKER_URL = self.BROKER_URL || 'amqp://';
  self.DEFAULT_QUEUE = self.DEFAULT_QUEUE || 'celery';
  self.DEFAULT_EXCHANGE = self.DEFAULT_EXCHANGE || 'celery';
  self.DEFAULT_EXCHANGE_TYPE = self.DEFAULT_EXCHANGE_TYPE || 'direct';
  self.DEFAULT_ROUTING_KEY = self.DEFAULT_ROUTING_KEY || 'celery';
  self.RESULT_EXCHANGE = self.RESULT_EXCHANGE || 'celeryresults';
  self.EVENT_EXCHANGE = self.EVENT_EXCHANGE || 'celeryev';
  self.SEND_TASK_SENT_EVENT = self.SEND_TASK_SENT_EVENT || false;
  self.TASK_RESULT_EXPIRES = self.TASK_RESULT_EXPIRES * 1000 || 86400000; // Default 1 day
  self.TASK_RESULT_DURABLE = self.TASK_RESULT_DURABLE || true; // Set Durable true by default (Celery 3.1.7)
  self.ROUTES = self.ROUTES || {};

  if (self.RESULT_BACKEND && self.RESULT_BACKEND.toLowerCase() === 'amqp') {
    self.backend_type = 'amqp';
  }
}

function Client(conf, callback) {
  var self = this;

  self.conf = new Configuration(conf);

  self.ready = false;
  self.broker_connected = false;
  self.backend_connected = false;

  debug_fn('Connecting to broker...');
  self.broker = amqp.createConnection({
    url: self.conf.BROKER_URL,
    heartbeat: 580
  }, {}, callback);

  if (self.conf.backend_type === 'amqp') {
    self.backend = self.broker;
    self.backend_connected = true;
  } else {
    self.backend_connected = true;
  }

  self.broker.on('ready', function() {
    debug_fn('Broker connected...');
    debug_fn('Creating Default Exchange');
    self.exchange = self.broker.exchange(self.conf.DEFAULT_EXCHANGE, {type: self.conf.DEFAULT_EXCHANGE_TYPE, durable: true, internal: false, autoDelete: false});
    if(self.conf.SEND_TASK_SENT_EVENT){
      debug_fn('Creating Event exchange');
      self.events_exchange = self.broker.exchange(self.conf.EVENT_EXCHANGE, {type: 'topic', durable: true, internal: false, autoDelete: false});
    }
    self.broker_connected = true;
    if (self.backend_connected) {
      self.ready = true;
      self.emit('connect');
    }
  });

  self.broker.on('error', function(err) {
    self.emit('error', err);
  });

  self.broker.on('close', function() {
    self.emit('end');
  });
  self.broker.on('end', function() {
    self.emit('end');
  });
}

util.inherits(Client, events.EventEmitter);

Client.prototype.createTask = function(name, options, additionalOptions) {
  return new Task(this, name, options, additionalOptions);
};

Client.prototype.end = function() {
  this.broker.disconnect();
};

Client.prototype.call = function(name /*[args], [kwargs], [options], [callback]*/ ) {
  var args, kwargs, options, callback;
  for (var i = arguments.length - 1; i > 0; i--) {
    if (typeof arguments[i] === 'function') {
      callback = arguments[i];
    } else if (Object.prototype.toString.call(arguments[i]) === '[object Array]') {
      args = arguments[i];
    } else if (typeof arguments[i] === 'object') {
      if (options) {
        kwargs = arguments[i];
      } else {
        options = arguments[i];
      }
    }
  }

  var task = this.createTask(name),
    result = task.call(args, kwargs, options);

  if (callback) {
    debug_fn('Subscribing to result...');
    result.on('ready', callback);
  }
  return result;
};

function Task(client, name, options, additionalOptions) {
  var self = this, route;

  self.client = client;
  self.name = name;
  self.options = options || {};
  self.additionalOptions = additionalOptions || {};

  self.queue = (route = self.client.conf.ROUTES[name]) && route.queue;
}

Task.prototype.publish = function(args, kwargs, options, callback) {
  var self = this;

  var id = uuid.v4(), event,
    message = createMessage(self.name, args, kwargs, options, id);

  self.client.exchange.publish(
    self.options.queue || self.queue || self.client.conf.DEFAULT_QUEUE,
    message,
    {
      'contentType': 'application/json',
      'contentEncoding': 'utf-8'
    },
    callback
  );

  if (self.additionalOptions.ignore_result){
    return null;
  }

  if (self.client.conf.SEND_TASK_SENT_EVENT){
    event = createEvent(message, {
      exchange: self.client.conf.DEFAULT_EXCHANGE,
      routing_key: self.client.conf.DEFAULT_ROUTING_KEY,
      queue: self.client.conf.DEFAULT_QUEUE,
      hostname: hostname
    });
    self.client.events_exchange.publish(
      'task.sent',
      event,
      {
        'contentType': 'application/json',
        'contentEncoding': 'utf-8',
        'headers': {
          'hostname': hostname
        },
        'deliveryMode': 2
      }

    )
  }
  return new Result(id, self.client);
};

Task.prototype.call = function(args, kwargs, options, callback) {
  var self = this;

  args = args || [];
  kwargs = kwargs || {};
  options = options || self.options || {};

  assert(self.client.ready);
  return self.publish(args, kwargs, options, callback);
};

function Result(taskid, client) {
  var self = this;

  events.EventEmitter.call(self);
  self.taskid = taskid;
  self.client = client;
  self.result = null;
  debug_fn('Subscribing to result queue...');
    self.client.backend.queue(
      self.taskid.replace(/-/g, ''),
      {
          durable: self.client.conf.TASK_RESULT_DURABLE,
          closeChannelOnUnsubscribe: true,
          "arguments": {
            'x-expires': self.client.conf.TASK_RESULT_EXPIRES
          }
      },
      onOpen
    );

  function onOpen(queue) {
      var ctag;
      queue.bind(self.client.conf.RESULT_EXCHANGE, '#');
      queue.subscribe(function(message) {
        var status = message.status.toLowerCase();
        debug_fn('Message on result queue [' + self.taskid + '] status:' + status);

        self.emit('ready', message);
        self.emit(status, message);

        if (status == 'success' || status == 'failure' || status == 'revoked' || status == 'ignored'){
          queue.unsubscribe(ctag);
          self.removeAllListeners()
        }
      })
      .addCallback(function(ok){
        ctag = ok.consumerTag;
      })

    }

}

util.inherits(Result, events.EventEmitter);


exports.createClient = function(config, callback) {
  return new Client(config, callback);
};
