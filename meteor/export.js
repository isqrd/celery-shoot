CeleryClient = Npm.require("celery-shoot");
var Future = Npm.require("fibers/future");
CeleryClient.injectFuturesMixin(Future);
