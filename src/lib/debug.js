var DEBUG     = require('debug'),
    DEBUG_LEVEL = 5 || process.env.NODE_CELERY_DEBUG,
    debuggers = {};

module.exports = function(name){
  if (!DEBUG_LEVEL) {
    return function(){};
  }

  return function(level, message) {
    if ((message == null) && (level != null)) {
      message = level;
      level = 1;
    }
    if (level > DEBUG_LEVEL) {
      return function() {};
    }

    if (!debuggers[name]) {
      debuggers[name] = DEBUG(name);
    }
    if (typeof message === 'function') {
      message = message();
    }
    return debuggers[name](message);
  };
};

