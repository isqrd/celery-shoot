var celery = require('../celery'),
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
