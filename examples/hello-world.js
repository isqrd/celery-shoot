var celery = require('../celery'),
client = celery.connectWithUri('amqp://guest:guest@localhost:5672//', function(err){
  assert(err == null);

  var task = client.createTask('tasks.echo');
  task.invoke(["Hello Wolrd"], function(err, result){
      console.log(err, result);
  })
});
