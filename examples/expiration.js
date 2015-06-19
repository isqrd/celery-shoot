var celery = require('../src/celery'),
client = celery.connectWithUri('amqp://guest:guest@localhost:5672//', function(err){
  assert(err == null);

  var task = client.createTask('tasks.sleep', {
      eta: 60 * 60 * 1000 // expire in an hour
  });
  task.invoke([2 * 60 * 60], function(err, res){
      console.log(err, res);
  })
});
