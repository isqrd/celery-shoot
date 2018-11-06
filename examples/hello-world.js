const { withClient } = require('../dist/celery-shoot.cjs');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

withClient(AMQP_HOST, {}, async client => {
  const result = await client.invokeTask({
    name: 'tasks.error',
    args: ['Hello World'],
  });
  console.log('tasks.echo response:', result);
});
