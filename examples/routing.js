const { withClient } = require('../dist/celery-shoot.cjs');

const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';

const routes = {
  'tasks.send_mail': {
    queue: 'mail',
  },
};

withClient(AMQP_HOST, { client: { routes } }, async client => {
  await client.invokeTask({
    name: 'tasks.send_email',
    kwargs: {
      to: 'to@example.com',
      title: 'sample email',
    },
    ignoreResult: true,
  });
  await client.invokeTask({
    name: 'tasks.calculate_rating',
    kwargs: {
      item: 1345,
    },
  });
});
