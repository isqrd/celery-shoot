import Promise from 'bluebird';

export class CeleryResultError extends Error {
  constructor(status, result, traceback, ...params) {
    super(...params);
    this.status = status;
    this.result = result;
    this.traceback = traceback;
  }
}

export default class Backend {
  constructor(channel, options) {
    this.channel = channel;
    const {
      exchange = 'celeryresults',
      exchangeOptions,
      queueOptions,
    } = options;
    this.exchange = exchange;
    this.exchangeOptions = {
      ...exchangeOptions,
    };
    this.queueOptions = {
      autoDelete: true,
      noWait: false,
      exclusive: false,
      durable: true,
      passive: false,
      arguments: {
        'x-expires': 86400000, // 1 day
      },
      ...queueOptions,
    };
    this._exchangeAsserted = false;
  }

  assertExchange() {
    // celery will assert the exchange, before sending a result
    // however, we cannot bind our queue until it exists
    if (this._exchangeAsserted) {
      return;
    }
    this._exchangeAsserted = true;
    // TODO
  }

  getResult(taskId) {
    // eslint-disable-next-line one-var,one-var-declaration-per-line
    let resolve, reject;
    const onResult = new Promise((innerResolve, innerReject) => {
      resolve = innerResolve;
      reject = innerReject;
    });

    const handleMessage = msg => {
      if (msg == null) {
        reject(new Error('Consumer cancelled by RabbitMQ'));
      }
      const { content } = msg;
      const data = JSON.parse(content.toString());
      const status = data.status.toLowerCase();

      if (
        status === 'failure' ||
        status === 'revoked' ||
        status === 'ignored'
      ) {
        reject(
          new CeleryResultError(
            data.status,
            data.result,
            data.traceback,
            `Task ${taskId} ${status}`,
          ),
        );
      } else if (status === 'success') {
        resolve(data.result);
      }
    };

    return Promise.using(
      this.consumeResults(taskId, handleMessage),
      () => onResult,
    );
  }

  consumeResults(taskId, onMessage) {
    const { channel, queueOptions, exchange } = this;
    const resultsQueue = taskId.replace(/-/g, '');
    let close = null;

    // ensure the results exchange is configured
    this.assertExchange();
    // to be used with `Promise.using`, cleans up consumerTag when done
    return Promise.all([
      channel.assertQueue(resultsQueue, queueOptions),
      channel.bindQueue(resultsQueue, exchange, '#'),
      channel.consume(resultsQueue, onMessage),
    ])
      .spread((assertOk, bindOk, { consumerTag }) => {
        close = consumerTag;
        // no return
      })
      .disposer(() => {
        if (close != null) {
          return channel.cancel(close);
        }
        return null;
      });
  }
}
