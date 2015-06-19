var celery = require('../celery'),
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
