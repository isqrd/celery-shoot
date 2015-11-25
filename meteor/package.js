'use strict';

// Meteor package.js file
var packageName = '3stack:celery-shoot';
var repository = 'https://github.com/3stack-software/celery-shoot';

Package.describe({
  name: packageName,
  version: '4.2.0',
  summary: 'Connect Meteor to a python task server',
  git: repository,
  documentation: '../README.md'
});

Npm.depends({
  "celery-shoot": "4.2.0"
});

Package.onUse(function (api) {
  api.versionsFrom('METEOR@1.2');

  api.export('CeleryClient', 'server');
  api.addFiles(['export.js'],'server');
});
