echo 'Running protocol tests...'
NODE_CELERY_DEBUG=1 mocha test_protocol

echo 'Running functional tests...'
NODE_CELERY_DEBUG=1 mocha test_celery
