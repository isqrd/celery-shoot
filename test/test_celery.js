var CeleryClient = require('../src/celery'),
  assert = require('assert');

describe('celery functional tests', function () {
  describe('initialization', function () {
    it('should create a client without error', function (done) {
      var client1 = CeleryClient.connectWithUri({}, {}, function (err) {
        assert.ok(err == null);
        assert.equal(client1.connection.state, 'open');
        client1.close(function (err) {
          assert.ok(err == null);
          done();
        });
      });
    });
  });

  describe('basic task calls', function () {
    it('should call a task without error', function (done) {
      var client = CeleryClient.connectWithUri({}, {}, function (err) {
        assert.ok(err == null);
        assert.equal(client.connection.state, 'open');
        var add = client.createTask('tasks.add');
        add.invoke([1, 2], function (err, resultMsg) {
          assert.ok(err == null);
          assert.equal(resultMsg.result, 3);

          client.close(function (err) {
            assert.ok(err == null);
            done();
          })
        });
      });
    });
  });

  describe('eta', function () {
    it('should call a task with a delay', function (done) {
      var client = CeleryClient.connectWithUri({}, {}, function (err) {
        assert.ok(err == null);
        assert.equal(client.connection.state, 'open');
        var time = client.createTask('tasks.time'),
          calledAt = (new Date()).getTime(),
          delay = 2 * 1000,
          acceptableDelay = 1000;

        time.invoke([], {}, {eta: delay}, function (err, resultMsg) {
          assert.ok(err == null);
          // should execute within 100ms of delay
          assert.ok(resultMsg.status, 'SUCCESS');
          //console.log(resultMsg.result, calledAt + delay, resultMsg.result - (calledAt + delay));

          // make sure it's called at least after the delay
          assert.ok(resultMsg.result > calledAt + acceptableDelay, "!(" + resultMsg.result + " > " + (calledAt + acceptableDelay) + " )");

          client.close(function (err) {
            assert.ok(err == null);
            done();
          })
        });
      });
    });
  });

  describe('expires', function () {
    it('should call a task which expires', function (done) {
      var client = CeleryClient.connectWithUri({}, {}, function (err) {
        assert.ok(err == null);
        assert.equal(client.connection.state, 'open');
        var time = client.createTask('tasks.time'),
          pastTime = - 10 * 1000;

        time.invoke([], {}, {expires: pastTime}, function (err, resultMsg) {
          assert.ok(err != null);
          assert.ok(resultMsg == null);
          assert.equal(err.status, 'REVOKED');
          client.close(function (err) {
            assert.ok(err == null);
            done();
          })
        });
      });
    });
  });
});
