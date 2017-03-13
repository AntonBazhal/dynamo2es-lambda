'use strict';

const joi = require('joi');

const FIELD = joi.string().min(1);

const ELASTICSEARCH_SCHEMA = joi.object({
  bulk: joi.object({
    body: joi.any().forbidden()
  }).optional().unknown()
}).unknown();

const HANDLER_OPTIONS = joi.object({
  elasticsearch: ELASTICSEARCH_SCHEMA,
  es: ELASTICSEARCH_SCHEMA,
  beforeHook: joi.func(),
  afterHook: joi.func(),
  recordErrorHook: joi.func(),
  errorHook: joi.func(),
  separator: joi.string().allow(''),
  idField: [FIELD, joi.array().min(1).items(FIELD)],
  index: joi.string().min(1),
  indexField: [FIELD, joi.array().min(1).items(FIELD)],
  type: joi.string().min(1),
  typeField: [FIELD, joi.array().min(1).items(FIELD)],
  pickFields: [FIELD, joi.array().min(1).items(FIELD)],
  versionField: FIELD
})
.without('elasticsearch', 'es')
.xor('index', 'indexField')
.xor('type', 'typeField');

const EVENT = joi.object({
  Records: joi.array().items(joi.object({
    eventName: joi.string().required(),
    dynamodb: joi.object({
      Keys: joi.object().required(),
      NewImage: joi.object(),
      OldImage: joi.object()
    }).required()
  })).required()
});

const VERSION = joi.number().min(0);

module.exports = {
  HANDLER_OPTIONS,
  EVENT,
  VERSION
};
