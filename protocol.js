var uuid = require('node-uuid');

var pid = process.pid;

// http://docs.celeryproject.org/en/latest/internals/protocol.html#message-format
//var mandatoryFields = ['task', 'id', 'args', 'kwargs'];
var fields = ['retries', 'eta', 'expires',
              'taskset', 'chord', 'utc', 'callbacks', 'errbacks', 'timelimit'];


function formatDate(date) {
  if (!(date instanceof Date)){
    date = new Date(Date.now() + date);
  }
  return date.toISOString().slice(0, - 1);
}

function createMessage(task, args, kwargs, options, id) {
  args = args || [];
  kwargs = kwargs || {};
  options = options || {};

  var message = {
    id: id || uuid.v4(),
    task: task,
    args: args,
    kwargs: kwargs
  };

  var field;
  for(var i=0; i < fields.length; i++){
    field = fields[i];
    if (options.hasOwnProperty(field)){
      message[field] = options[field];
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
