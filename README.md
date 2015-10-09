# Celery client for Node.js

Celery is an asynchronous task/job queue based on distributed
message passing. `celery-shoot` allows to queue tasks from Node.js.
If you are new to Celery check out http://celeryproject.org/

## Differences with `node-celery`

 1. This library is now based on [amqp-coffee](https://github.com/dropbox/amqp-coffee),
    instead of [node-amqp](https://github.com/postwait/node-amqp).

 2. `EventEmitter` based code has been removed; only pure callbacks are available.

 3. Support for the Redis Backend has been removed.

    * I will accept pull-requests if you would like to re-add support.

 4. Primary Queue / Exchange declaration has been removed. This means if you start up
    `celery-shoot` on a fresh RabbitMQ vhost, you'll get an error.

    To get around this, just start a `celery worker` on that vhost first.

     * Why? If you declared your Queues/Exchanges from node, you need to mirror
       the celery settings _exactly_. If you don't, you need to stop both node &
       celery, delete the queues, restart both services (with the correct settings,
       or with the `celery worker` first). This was a big trap, that often came up
       deploying to production so we're better off without it!

## Usage

Simple example, included as [examples/hello-world.js](https://github.com/3stack-software/celery-shoot/blob/master/examples/hello-world.js):

```javascript
var celery = require('celery-shoot'),
client = celery.connectWithUri('amqp://guest:guest@localhost:5672//', function(err){
  assert(err == null);

  var task = client.createTask('tasks.echo');
  task.invoke(["Hello Wolrd"], function(err, result){
      console.log(err, result);
  })
});
```


### ETA

The ETA (estimated time of arrival) lets you set a specific date and time that is the earliest time at which your task will be executed:

```javascript
var celery = require('celery-shoot'),
client = celery.connectWithUri('amqp://guest:guest@localhost:5672//', function(err){
  assert(err == null);

  var task = client.createTask('tasks.send_email', {
      eta: 60 * 60 * 1000 // execute in an hour from invocation
  }, {
      ignoreResult: true // ignore results
  });
  task.invoke([], {
    to: 'to@example.com',
    title: 'sample email'
  })
});
```

### Expiration

The expires argument defines an optional expiry time, a specific date and time using Date:

```javascript
var celery = require('celery-shoot'),
client = celery.connectWithUri('amqp://guest:guest@localhost:5672//', function(err){
  assert(err == null);

  var task = client.createTask('tasks.sleep', {
      eta: 60 * 60 * 1000 // expire in an hour
  });
  task.invoke([2 * 60 * 60], function(err, res){
      console.log(err, res);
  })
});
```

### Routing

The simplest way to route tasks to different queues is using `options.routes`:

```javascript
var celery = require('celery-shoot'),
client = celery.connectWithUri('amqp://guest:guest@localhost:5672//', {
  routes: {
      'tasks.send_mail': {
          'queue': 'mail'
      }
  }
}, function(err){
  assert(err == null);

  var task = client.createTask('tasks.send_email');
  task.invoke([], {
    to: 'to@example.com',
    title: 'sample email'
  });
  var task2 = client.createTask('tasks.calculate_rating');
  task2.invoke([], {
      item: 1345
  });
});
```

You can also configure custom routers, similar to http://celery.readthedocs.org/en/latest/userguide/routing.html#routers


```js
var myRouter = function(task, args, kwargs){
  if(task === 'myapp.tasks.compress_video'){
    return {
      'exchange': 'video',
      'routingKey': 'video.compress'
    }
  }
  return null;
}
Client({
  routes: [myRouter]
});
```
