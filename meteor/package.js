'use strict';

// Meteor package.js file
var packageName = '3stack:celery-shoot';
var repository = 'https://github.com/3stack-software/celery-shoot';

Package.describe({
  name: packageName,
  version: '4.1.0-rc.0',
  summary: 'Connect Meteor to a python task server',
  git: repository,
  documentation: '../README.md'
});

Npm.depends({
  "celery-shoot": repository + "/archive/9ac96b8428feb28704e4c2681f66c1f8081ba95c.tar.gz"
});

Package.onUse(function (api) {
  api.versionsFrom('METEOR@1.1.0.2');

  api.export('CeleryClient', 'server');
  api.addFiles(['export.js'],'server');
});
