/* eslint-env mocha */
const CeleryClient = require('../src/celery');
const assert = require('assert');
const Promise = require('bluebird');


const AMQP_HOST = process.env.AMQP_HOST || 'amqp://guest:guest@localhost//';


function getClient() {
  return CeleryClient.connectWithUri(AMQP_HOST).disposer(client => client.close());
}


describe('celery functional tests', () => {
  describe('initialization', () => {
    it('should create a client without error', () => Promise.using(getClient(), (client) => {
      assert.ok(true);
    }));
  });

  describe('basic task calls', () => {
    it('should call a task without error', () => Promise.using(getClient(), (client) => {
      const add = client.createTask('tasks.add');
      return add.invoke([1, 2]).then((resultMsg) => {
        assert.ok(true);
        assert.equal(resultMsg.result, 3);
      });
    }));
  });

  describe('eta', () => {
    it('should call a task with a delay', () => {
      Promise.using(getClient(), (client) => {
        const time = client.createTask('tasks.time');
        const calledAt = (new Date()).getTime();
        const delay = 2 * 1000;
        const acceptableDelay = 1000;

        return time.invoke([], {}, { eta: delay }).then((resultMsg) => {
          // should execute within 100ms of delay
          assert.ok(resultMsg.status, 'SUCCESS');
          // console.log(resultMsg.result, calledAt + delay, resultMsg.result - (calledAt + delay));

          // make sure it's called at least after the delay
          assert.ok(resultMsg.result > calledAt + acceptableDelay, `!(${resultMsg.result} > ${calledAt + acceptableDelay} )`);
        });
      });
    });
  });

  describe('expires', () => {
    it('should call a task which expires', () => {
      Promise.using(getClient(), (client) => {
        const time = client.createTask('tasks.time');
        const pastTime = -10 * 1000;

        return time.invoke([], {}, { expires: pastTime }).then(() => {
          assert.ok(false);
        }, (err) => {
          assert.equal(err.status, 'REVOKED');
        });
      });
    });
  });
});
