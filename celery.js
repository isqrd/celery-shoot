var util = require('util'),
  amqp = require('amqp'),
  assert = require('assert'),
  events = require('events'),
  hostname = require('os').hostname(),

  _ref = require('./protocol'),
  createMessage = _ref.createMessage,
  createEvent = _ref.createEvent;

debug_fn = process.env.NODE_CELERY_DEBUG === '1' ? util.debug : function() {};

function CeleryConfiguration(options) {
  var self = this;

  for (var o in options) {
    if (options.hasOwnProperty(o)) {
      self[o.replace(/^CELERY_/, '')] = options[o];
    }
  }

  self.BROKER_URL = self.BROKER_URL || 'amqp://';
  self.QUEUES = self.QUEUES || ['celery'];
  self.DEFAULT_QUEUE = self.DEFAULT_QUEUE || self.QUEUES[0];
  self.DEFAULT_EXCHANGE_TYPE = self.DEFAULT_EXCHANGE_TYPE || 'direct';
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

  self.conf = new CeleryConfiguration(conf);

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
    self.exchanges = {};
    self.conf.QUEUES.forEach(function(queueName){
      self.exchanges[queueName] = self.broker.exchange(queueName, {type: self.conf.DEFAULT_EXCHANGE_TYPE, durable: true, internal: false, autoDelete: false});
    });
    if(self.conf.SEND_TASK_SENT_EVENT){
      debug_fn('Creating Event exchange');
      self.eventsExchange = self.broker.exchange(self.conf.EVENT_EXCHANGE, {type: 'topic', durable: true, internal: false, autoDelete: false});
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

/**
 *
 * @param name
 * @param [defaultExecOptions] Passed as properties to the message
 *         Supports -  retries, expires, taskset, chord, utc, callbacks, errbacks, timelimit
 *         {@link http://docs.celeryproject.org/en/latest/internals/protocol.html#message-format}
 * @param [taskOptions] Supports `ignoreResult`
 * @constructor
 */
Client.prototype.createTask = function CeleryClient_createTask(name, defaultExecOptions, taskOptions) {
  return new Task(this, name, defaultExecOptions, taskOptions);
};

Client.prototype.end = function CeleryClient_end() {
  this.broker.disconnect();
};

/**
 * Shortcut method to quickly call a task.
 *
 * Effectively calls `createTask` and then `task.call`
 *
 * @param {String} name The name of the task to invoke.
 * @param {Array} [args]
 * @param {Object} [kwargs]
 * @param {Object} [options] Passed as properties to the message
 *         Supports -  retries, expires, taskset, chord, utc, callbacks, errbacks, timelimit
 *         {@link http://docs.celeryproject.org/en/latest/internals/protocol.html#message-format}
 */
Client.prototype.call = function CeleryClient_call(name /*[args], [kwargs], [options], [callback]*/ ) {
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

  var task = this.createTask(name, options),
    result = task.call(args, kwargs);

  if (callback) {
    debug_fn('Subscribing to result...');
    result.on('ready', callback);
  }
  return result;
};

/**
 * Create a task.
 *
 * Will choose queue name with the following priority:
 *   1. taskOptions.queueName
 *   2. ROUTES{TaskName}
 *   3. DEFAULT_QUEUE
 *
 * @param client
 * @param name
 * @param [defaultExecOptions] Passed as properties to the message
 *         Supports -  retries, expires, taskset, chord, utc, callbacks, errbacks, timelimit
 *         {@link http://docs.celeryproject.org/en/latest/internals/protocol.html#message-format}
 * @param [taskOptions] Supports `ignoreResult` and `queueName`.
 *
 * @constructor
 */
function Task(client, name, defaultExecOptions, taskOptions) {
  var self = this, route;

  self.client = client;
  self.name = name;
  self.defaultExecOptions = defaultExecOptions || {};
  self.taskOptions = taskOptions || {};

  if (taskOptions.queueName){
    self.queueName = taskOptions.queueName;
  } else if ((route = self.client.conf.ROUTES[name]) && route.queue){
    self.queueName = route.queue;
  } else {
    self.queueName = self.client.conf.DEFAULT_QUEUE
  }
}


/**
 * Proxy for `publish` that provides default arguments
 * @param {Array} [args]
 * @param {Object} [kwargs]
 * @param {Object} [execOptions] Overwrite `defaultExecOptions` at runtime.
 * @param {Function} [callback]
 */
Task.prototype.call = function CeleryTask_call(args, kwargs, execOptions, callback) {
  var self = this;

  assert(self.client.ready);
  assert(self.client.exchanges[self.queueName] != null);

  args = args || [];
  kwargs = kwargs || {};

  var options, field;
  if (execOptions == null){ // or  empty(execOptions)
    options = self.defaultExecOptions;
  } else {
    options = {};
    for (field in self.defaultExecOptions) options[field] = self.defaultExecOptions[field];
    for (field in execOptions) options[field] = execOptions[field];
  }

  var event,
    message = createMessage(self.name, args, kwargs, options);

  self.client.exchanges[self.queueName].publish(
    self.queueName,
    message,
    {
      'contentType': 'application/json',
      'contentEncoding': 'utf-8'
    },
    callback
  );

  if (self.taskOptions.ignoreResult){
    return null;
  }

  if (self.client.conf.SEND_TASK_SENT_EVENT){
    event = createEvent(message, {
      exchange: self.queueName,
      routing_key: self.queueName,
      queue: self.queueName,
      hostname: hostname
    });
    self.client.eventsExchange.publish(
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
  return new Result(message.id, self.client);
};

/**
 * This isn't really a result "class"... it's just a glorified event emitter, that will eventually
 *  emit "started" (if track_started=True), then one of "success", "failure", "revoked" or "ignored".
 *
 * @param taskid
 * @param client
 * @constructor
 */
function Result(taskid, client) {
  var self = this;

  events.EventEmitter.call(self);

  var conf = client.conf,
    TASK_RESULT_DURABLE = conf.TASK_RESULT_DURABLE,
    TASK_RESULT_EXPIRES = conf.TASK_RESULT_EXPIRES,
    RESULT_EXCHANGE = conf.RESULT_EXCHANGE;

  debug_fn('Subscribing to result queue...');
  client.backend.queue(taskid.replace(/-/g, ''), {
    durable: TASK_RESULT_DURABLE,
    closeChannelOnUnsubscribe: true,
    "arguments": {
      'x-expires': TASK_RESULT_EXPIRES
    }
  }, function Result_onQueueOpen(queue) {
    var ctag = null;
    queue.bind(RESULT_EXCHANGE, '#');
    queue.subscribe(function Result_onQueueMessage(message) {
      var status = message.status.toLowerCase();
      debug_fn('Message on result queue [' + taskid + '] status:' + status);
      self.emit(status, message);
      if (status == 'success' || status == 'failure' || status == 'revoked' || status == 'ignored'){
        self.emit('ready', message);
        queue.unsubscribe(ctag);
        self.removeAllListeners()
      }
    })
    .addCallback(function Result_onQueueReady(ok){
      ctag = ok.consumerTag;
    });
  });
}

util.inherits(Result, events.EventEmitter);


exports.createClient = function(config, callback) {
  return new Client(config, callback);
};
