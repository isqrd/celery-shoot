const Promise = require('bluebird');
const { withClient } = require('../dist/celery-shoot.cjs');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

const n = parseInt(process.argv.length > 2 ? process.argv[2] : 100, 10);
const L = `tasksCompleted (n=${n})`;

withClient(
  AMQP_HOST,
  {
    client: { sendTaskSentEvent: false },
  },
  client => {
    console.time(L);
    const promises = [];
    for (let i = 0; i < n; i++) {
      const promise = client.invokeTask({
        name: 'tasks.add',
        args: [i, i],
      });
      promises.push(promise);
    }
    return Promise.all(promises).then(
      () => {
        console.timeEnd(L);
      },
      err => {
        console.timeEnd(L);
        console.error(err);
      },
    );
  },
);
