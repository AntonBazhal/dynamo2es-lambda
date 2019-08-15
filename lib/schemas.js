const joi = require('@hapi/joi');

const FIELD = joi.string().min(1);

const ELASTICSEARCH_SCHEMA = joi.object({
  // One would expect to use .type(elasticsearch.Client) here, but it doesn't work.
  // Maybe it's because Client is a function than a class? So, we will settle for
  // validating the interface.
  client: joi.object({
    bulk: joi.func().required()
  }).required().unknown(),
  bulk: joi.object({
    body: joi.any().forbidden()
  }).optional().unknown()
}).unknown();

const HANDLER_OPTIONS = joi
  .object({
    elasticsearch: ELASTICSEARCH_SCHEMA,
    es: ELASTICSEARCH_SCHEMA,
    beforeHook: joi.func(),
    afterHook: joi.func(),
    recordErrorHook: joi.func(),
    errorHook: joi.func(),
    transformRecordHook: joi.func(),
    separator: joi.string().allow(''),
    idField: [FIELD, joi.array().min(1).items(FIELD)],
    idResolver: joi.func(),
    index: joi.string().min(1),
    indexField: [FIELD, joi.array().min(1).items(FIELD)],
    indexPrefix: joi.string().allow(''),
    type: joi.string().min(1),
    typeField: [FIELD, joi.array().min(1).items(FIELD)],
    parentField: FIELD,
    pickFields: [FIELD, joi.array().min(1).items(FIELD)],
    versionField: FIELD,
    versionResolver: joi.func(),
    retryOptions: joi.object()
  })
  .without('elasticsearch', 'es')
  .oxor('idField', 'idResolver')
  .oxor('versionField', 'versionResolver')
  .xor('index', 'indexField')
  .oxor('type', 'typeField')
  .without('index', 'indexPrefix')
  .with('indexPrefix', 'indexField')
  .label('options');

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
