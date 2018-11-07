import Promise from 'bluebird';

function defer() {
  let resolve, reject;
  const promise = new Promise((a, b) => {
    resolve = a;
    reject = b;
  });
  return {
    resolve,
    reject,
    promise,
  };
}

class AsyncChannel {
  constructor() {
    this.done = false; // when we're done, there's no future reads available.. just the buffer
    this.buffer = []; // only when we have more data than waiters.
    this.waiters = []; // queued/eager calls to `next()` that we don't have data for
  }

  push(value) {
    if (this.done) {
      return;
    }
    // give the next waiter the message
    if (this.waiters.length) {
      // assert this.buffer.length === 0
      const waiter = this.waiters.shift();
      waiter.resolve({ done: false, value });
      return;
    }
    // no waiters, lets buffer the message for later
    this.buffer.push(value);
  }

  close() {
    if (this.done) {
      return;
    }
    this.done = true;
    while (this.waiters.length) {
      // assert this.buffer.length === 0
      const waiter = this.waiters.shift();
      waiter.resolve({ done: true });
    }
  }

  async next() {
    if (this.buffer.length) {
      // assert this.waiter.length === 0
      const value = this.buffer.shift(); // pop(0)
      return { done: false, value };
    }
    if (this.done) {
      return { done: true };
    }
    const waiter = defer();
    this.waiters.push(waiter);
    return waiter.promise;
  }
}

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

  async assertExchange() {
    // celery will assert the exchange, before sending a result
    // however, we cannot bind our queue until it exists
    if (this._exchangeAsserted) {
      return null;
    }
    this._exchangeAsserted = true;
    // TODO
    return null;
  }

  async getResult(taskId) {
    const { results, close } = await this.consumeResults(taskId);

    try {
      // TODO support track-started
      // TODO use await Promise.race(Promise.delay(ms).then(()=> throw new reason), ...)
      let { done, value = null } = await results.next();
      while (!done) {
        const data = JSON.parse(value.content.toString());
        const status = data.status.toLowerCase();
        if (
          status === 'failure' ||
          status === 'revoked' ||
          status === 'ignored'
        ) {
          throw new CeleryResultError(
            data.status,
            data.result,
            data.traceback,
            `Task ${taskId} ${status}`,
          );
        } else if (status === 'success') {
          return data.result;
        }

        ({ done, value = null } = await results.next());
      }
    } finally {
      await close();
    }
  }

  async consumeResults(taskId) {
    const { channel, queueOptions, exchange } = this;
    const resultsQueue = taskId.replace(/-/g, '');
    const results = new AsyncChannel();
    const [, , , { consumerTag }] = await Promise.all([
      // as these are rpc calls over the one channel, they will complete serially
      this.assertExchange(),
      channel.assertQueue(resultsQueue, queueOptions),
      channel.bindQueue(resultsQueue, exchange, '#'),
      channel.consume(resultsQueue, msg => {
        if (msg == null) {
          results.close();
        } else {
          results.push(msg);
        }
      }),
    ]);

    return {
      results,
      async close() {
        results.close();
        return channel.cancel(consumerTag);
      },
    };
  }
}
