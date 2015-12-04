'use strict';

// Meteor package.js file
var packageName = '3stack:celery-shoot';
var repository = 'https://github.com/3stack-software/celery-shoot';

Package.describe({
  name: packageName,
  version: '4.3.0-rc.0',
  summary: 'Connect Meteor to a python task server',
  git: repository,
  documentation: '../README.md'
});

Npm.depends({
  "celery-shoot": repository + "/tarball/7945d170b387a53d305aca0f53383f5b00f307ab"
});

Package.onUse(function (api) {
  api.versionsFrom('METEOR@1.2');

  api.export('CeleryClient', 'server');
  api.addFiles(['export.js'],'server');
});
