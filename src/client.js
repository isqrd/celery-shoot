import amqp from 'amqplib';
import uuidv4 from 'uuid/v4';
import Promise from 'bluebird';
import { asTaskV1, asTaskV2, serializeTask, serializeEvent } from './protocol';
import Backend from './backend';

export class CeleryClient {
  constructor(connection, publisherChannel, backend, options = {}) {
    this.connection = connection;
    this.publisherChannel = publisherChannel;
    this.backend = backend;
    this.options = {
      taskProtocol: 1,
      defaultExchange: 'celery',
      defaultRoutingKey: 'celery',
      eventsExchange: 'celeryev',
      sendTaskSentEvent: true,
      routes: {},
      ...options,
    };
    this.closed = false;
    this.blocked = false;
    this.connection.on('close', () => {
      this.closed = true;
      this.connection = null;
      this.publisherChannel = null;
      this.backend = null;
    });
  }

  close() {
    const connection = this.connection;
    this.connection = null;
    this.publisherChannel = null;
    this.backend = null;
    return connection.close();
  }

  lookupRoute(taskName, args, kwargs) {
    const {
      routes,
      defaultExchange: exchange,
      defaultRoutingKey: routingKey,
    } = this.options;

    const routeArr = Array.isArray(routes) ? routes : [routes];
    for (const routerOrMap of routeArr) {
      let route;
      if (typeof routerOrMap === 'function') {
        // it's a router
        route = routerOrMap(taskName, args, kwargs);
      } else {
        // it's a map{taskName => route}
        route = routerOrMap[taskName];
      }
      if (route != null) {
        return {
          exchange,
          routingKey,
          ...route,
        };
      }
    }
    return { exchange, routingKey };
  }

  async invokeTask({
    name,
    args = [],
    kwargs = {},
    exchange: overrideExchange = null,
    routingKey: overrideRoutingKey = null,
    ignoreResult = false,
    ...options
  }) {
    if (this.blocked) {
      throw new Error('Blocked');
    }
    const taskId = uuidv4();
    let { exchange, routingKey } = this.lookupRoute(name, args, kwargs);
    // per call options will always overwrite lookups.
    if (overrideExchange != null) {
      exchange = overrideExchange;
    }
    if (overrideRoutingKey != null) {
      routingKey = overrideRoutingKey;
    }

    const { eventsExchange, taskProtocol, sendTaskSentEvent } = this.options;

    let task;
    if (taskProtocol === 1) {
      task = asTaskV1(taskId, name, args, kwargs, options);
    } else {
      task = asTaskV2(taskId, name, args, kwargs, options);
    }
    const { headers, properties, body, sentEvent } = task;

    if (sendTaskSentEvent) {
      await this.publish(
        serializeTask(exchange, routingKey, headers, properties, body),
        serializeEvent(
          eventsExchange,
          'task-sent',
          sentEvent,
          exchange,
          routingKey,
        ),
      );
    } else {
      await this.publish(
        serializeTask(exchange, routingKey, headers, properties, body),
      );
    }

    if (ignoreResult) {
      return null;
    }

    return this.backend.getResult(taskId);
  }

  waitForDrain() {
    this.blocked = true;
    Promise.fromCallback(drainedCallback => {
      this.publisherChannel.once('drain', evt => {
        this.blocked = false;
        drainedCallback();
      });
    });
  }

  async publish(...messages) {
    // TODO find better indication we're using a confirm channel
    if (this.publisherChannel.waitForConfirms == null) {
      let needDrain;
      // fire and forget
      messages.forEach(({ exchange, routingKey, content, options }) => {
        needDrain = !this.publisherChannel.publish(
          exchange,
          routingKey,
          content,
          options,
        );
      });
      if (!needDrain) {
        return null;
      }
      return this.waitForDrain();
    }

    let needDrain;
    const confirms = messages.map(
      ({ exchange, routingKey, content, options }) =>
        Promise.fromCallback(confirmCallback => {
          needDrain = !this.publisherChannel.publish(
            exchange,
            routingKey,
            content,
            options,
            confirmCallback,
          );
        }),
    );

    if (needDrain) {
      confirms.push(this.waitForDrain());
    }

    return Promise.all(confirms);
  }
}

export function connect(
  connectionUri,
  {
    socket: socketOptions = {},
    client: clientOptions = {},
    backend: backendOptions = {},
  } = {},
) {
  return amqp
    .connect(
      connectionUri,
      socketOptions,
    )
    .then(connection =>
      Promise.all([
        connection.createChannel(), // TODO support createConfirmChannel
        connection.createChannel(),
      ]).spread(
        (publisherChannel, backendChannel) =>
          new CeleryClient(
            connection,
            publisherChannel,
            new Backend(backendChannel, backendOptions), // TODO support rpc backend
            clientOptions,
          ),
      ),
    );
}

export function withClient(connectionUri, options, fn) {
  return Promise.using(
    connect(
      connectionUri,
      options,
    ).disposer(client => client.close()),
    fn,
  );
}
