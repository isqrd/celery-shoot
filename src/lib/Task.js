var
  debug = require('./debug')('celery:Task'),
  uuid = require('node-uuid'),
  amqp = require('amqp-coffee'),
  async = require('async'),
  assert = require('assert'),
  hostname = require('os').hostname(),
  _ = require('underscore'),
  pid = process.pid;


module.exports = (function () {
  /**
   * Create a task.
   *
   * Will choose exchange name with the following priority:
   *   1. taskOptions.exchangeName
   *   2. ROUTES{TaskName}.exchangeName
   *   3. Client.options.defaultExchange
   * Will choose the routing key with the following priority
   *   1. taskOptions.routingKey
   *   2. ROUTES{TaskName}.routingKey
   *   3. Client.options.defaultRoutingKey
   *
   * @param client
   * @param name
   * @param [defaultExecOptions] Passed as properties to the message
   *         Supports -  retries, expires, taskset, chord, utc, callbacks, errbacks, timelimit
   *         {@link http://docs.celeryproject.org/en/latest/internals/protocol.html#message-format}
   * @param [taskOptions] Supports `ignoreResult` and `exchangeName` & `routingKey`
   *
   * @constructor
   */
  function Task(client, name, defaultExecOptions, taskOptions) {
    var self = this;

    self.client = client;
    self.name = name;
    self.defaultExecOptions = _.defaults(defaultExecOptions, {
      utc: true
    });
    self.taskOptions = _.defaults(taskOptions, {
      ignoreResult: false,
      exchangeName: null,
      routingKey: null
    });
    if (self.taskOptions.queueName != null){
      console.error('taskOptions.queueName is no longer supported. ' +
        'Please use taskOptions.queueName & taskOptions.routingKey instead.');
    }

    var route = self.client.options.routes[name];
    if (self.taskOptions.exchangeName != null) {
      self.exchangeName = self.taskOptions.exchangeName;
    } else if (route && route.exchange != null) {
      self.exchangeName = route.exchange;
    } else {
      self.exchangeName = self.client.options.defaultExchange
    }
    if (self.taskOptions.routingKey != null) {
      self.routingKey = self.taskOptions.routingKey;
    } else if (route && route.routingKey != null) {
      self.routingKey = route.routingKey;
    } else {
      self.routingKey = self.client.options.defaultRoutingKey;
    }
  }

  /**
   *
   * Params Signature - ([args, [,kwargs[, execOptions]]][[,onStarted],onCompleted])
   * @param {Array} [args]
   * @param {Object} [kwargs]
   * @param {Object} [execOptions] Overwrite `defaultExecOptions` at runtime.
   * @param {Function} [onStarted]
   * @param {Function} [onCompleted]
   */
  Task.prototype.invoke = function CeleryTask_invoke(/* [args, [,kwargs [, execOptions]]][[,onStarted],onCompleted] */) {
    var self = this;
    var onCompleted, onStarted;

    var params = Array.prototype.slice.call(arguments, 0);

    assert(params.length <= 5);

    var lastParam = params.length > 0 && params[params.length - 1];
    if (lastParam && _.isFunction(lastParam)) {
      onCompleted = lastParam;
      params.pop();

      // or... the last param now..
      var secondLastParam = params.length > 0 && params[params.length - 1];
      if (secondLastParam && _.isFunction(secondLastParam)) {
        onStarted = secondLastParam;
        params.pop();
      }
    }

    var remainingParams = params.length;
    var args = remainingParams > 0 && params[0] || [];
    var kwargs = remainingParams > 1 && params[1] || {};
    var execOptions = remainingParams > 2 && params[2] || {};
    var options = _.defaults(execOptions, self.defaultExecOptions);
    var message = self.createMessage(args, kwargs, options);
    this.sendMessage(message, onStarted, onCompleted);
  };

  function formatDateOrRelativeMs(date) {
    if (!(date instanceof Date)) {
      date = new Date(Date.now() + date);
    }
    return date.toISOString().slice(0, -1);
  }

  var ADDITIONAL_OPTIONS = ['retries', 'eta', 'expires', 'taskset', 'chord', 'utc', 'callbacks', 'errbacks', 'timelimit'];
  Task.prototype.createMessage = function CeleryTask_createMessage(args, kwargs, options, id) {
    var self = this;
    var message = {
      id: id || uuid.v4(),
      task: self.name,
      args: args,
      kwargs: kwargs,
      utc: true // dates should be considered UTC by default
    };
    // Are callers going to behave? & not send unsupported opts
    // var notAllowed = _.difference(ADDITIONAL_OPTIONS,_.keys(options));
    // if (notAllowed.length > 0) throw new Error("Unsupported options " + ",".join(notAllowed));
    _.extend(message, options);

    if (message.eta) {
      message.eta = formatDateOrRelativeMs(message.eta);
    }

    if (message.expires) {
      message.expires = formatDateOrRelativeMs(message.expires);
    }
    return message;
  };

  Task.prototype.createEvent = function CeleryTask_createEvent(message) {
    var self = this;
    // Need to update for Celery 4.0 task_sent => before_task_publish
    //  http://docs.celeryproject.org/en/latest/internals/deprecation.html#removals-for-version-4-0
    //var event ={
    //  type: 'before_task_publish',
    //  body: message,
    //  exchange: options.exchange || 'celery',
    //  routing_key: options.routing_key || 'celery',
    //  headers: {},
    //  properties: {},
    //  declare: {},
    //  retry_policy: {}
    //};
    return _.extend(
      // Task/Message related data
      _.pick(message, 'args', 'kwargs', 'retries', 'expires', 'eta'), {
        uuid: message.id, // remap  id=>uuid
        name: message.task, // remap task=>name
        type: 'task-sent',

        // where did we send it?
        exchange: self.exchangeName,
        routing_key: self.routingKey,

        // identify the current host & time
        timestamp: (new Date()).getTime() / 1000,
        utcoffset: (new Date()).getTimezoneOffset() / -60,
        pid: pid,
        hostname: hostname
      })
  };

  /**
   *
   * @param {Array} message
   * @param {Function} [onStarted]
   * @param {Function} [onCompleted]
   */
  Task.prototype.sendMessage = function CeleryTask_sendMessage(message, onStarted, onCompleted) {
    var self = this;

    assert(self.client.connection.state === 'open');

    var invokeAndGetResults = [
      function CeleryTask_publishTask(done) {
        self.client.connection.publish(
          self.exchangeName, // actual exchange
          self.routingKey, // routing key
          message, //data
          {
            confirm: true,
            mandatory: false,
            immediate: false,
            contentEncoding: 'utf-8'
          },
          done
        );
      }
    ];

    if (self.client.options.sendTaskSentEvent) {
      invokeAndGetResults.push(function CeleryTask_sendTaskEvent(done) {
        debug("sendTaskSent");
        var event = self.createEvent(message);
        self.client.connection.publish(
          self.client.options.eventsExchange, // exchange
          'task.sent', // routing key
          event, // data
          {
            confirm: true,
            mandatory: false,
            immediate: false,
            contentEncoding: 'utf-8',
            headers: {
              hostname: hostname
            },
            deliveryMode: 2
          },
          done
        );
      });
    }

    if (!self.taskOptions.ignoreResult) {
      var resultsQueue = message.id.replace(/-/g, '');
      invokeAndGetResults.push(function CeleryTask_connectToResultsQueue(done) {
          debug("connectToResultsQueue");
          self.client.connection.queue(_.extend({
            queue: resultsQueue
          }, self.client.options.taskResultQueueOptions), done)
        },
        function CeleryTask_declareResultsQueue(queue, done) {
          debug("declareResultsQueue");
          queue.declare(function (err) {
            done(err, queue);
          });
        },
        function CeleryTask_getResults(queue, done) {
          debug("getResults");
          queue.bind(self.client.options.resultsExchange, '#');
          var consumer = self.client.connection.consume(resultsQueue, {},
            function CeleryTask_onMessage(envelope) {

              var message = envelope.data,
                status = message.status.toLowerCase();
              debug("onmessage::" + status);

              if (status === 'failure' || status === 'revoked' || status === 'ignored') {
                consumer.close(function () {
                  done(message); // error
                });

              } else if (status === 'success') {
                consumer.close(function () {
                  debug('consumer closed!');
                  done(null, message);
                });
              } else if (status === 'started' && onStarted) {
                onStarted(message);
              }
            }
          );
        });
    }
    async.waterfall(invokeAndGetResults, onCompleted);
  };

  return Task;
})();
