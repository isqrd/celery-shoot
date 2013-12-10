var uuid = require('node-uuid');

var pid = process.pid;

var fields = ['task', 'id', 'args', 'kwargs', 'retires', 'eta', 'expires',
              'taskset', 'chord', 'utc', 'callbacks', 'errbacks', 'timeouts'];


function formatDate(date) {
  return new Date(date).toISOString().slice(0, - 1);
}

function createMessage(task, args, kwargs, options, id) {
  args = args || [];
  kwargs = kwargs || {};

  var message = {
    task: task,
    args: args,
    kwargs: kwargs
  };

  message.id = id || uuid.v4();
  for (var o in options) {
    if (options.hasOwnProperty(o)) {
      if (fields.indexOf(o) === -1) {
        throw "invalid option: " + o;
      }
      message[o] = options[o];
    }
  }

  if (message.eta) {
    message.eta = formatDate(message.eta);
  }

  if (message.expires) {
    message.expires = formatDate(message.expires);
  }

  return message;
}

function createEvent(message, options){

  var event = {
    args: message.args,
    uuid: message.id,
    exchange: options.exchange || 'celery',
    timestamp: options.timestamp || (new Date()).getTime()/1000,
    pid: options.pid || pid,
    routing_key: options.routing_key || 'celery',
    queue: options.queue || 'celery',
    utcoffset: options.utcoffset || (new Date()).getTimezoneOffset()/-60,
    type: 'task-sent',
    //clock: 0 -- probably not necessary
    kwargs: message.kwargs,
    hostname: options.hostname || '',
    name: message.task
  };

  if ('retries' in message){
    event.retries = message.retries;
  }
  if ('expires' in message){
    event.expires = message.expires;
  }
  if ('eta' in message){
    event.eta = message.eta;
  }
  return event;
}

exports.createMessage = createMessage;
exports.createEvent = createEvent;
