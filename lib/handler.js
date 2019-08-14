const _ = require('lodash');
const aeclient = require('aws-elasticsearch-client');
const AWS = require('aws-sdk');
const bunyan = require('alpha-lambda-bunyan');
const lambdaHandler = require('alpha-lambda');
const promiseRetry = require('promise-retry');

const errors = require('./errors');
const schemas = require('./schemas');
const utils = require('./utils');

const DEFAULT_RETRY_COUNT = 0;

module.exports = function(options) {
  options = options || {}; // eslint-disable-line no-param-reassign

  utils.validate(options, schemas.HANDLER_OPTIONS);

  const elasticsearchOpts = options.elasticsearch || options.es || {};
  const bulkOpts = elasticsearchOpts.bulk || {};

  const esclient = aeclient.create(_.omit(elasticsearchOpts, 'bulk'));

  const separator = _.has(options, 'separator') ? options.separator : '.';
  const retryOptions = _.assign({ retries: DEFAULT_RETRY_COUNT }, options.retryOptions);
  const indexPrefix = options.indexPrefix || '';

  const handler = lambdaHandler()
    .use(bunyan())
    .use((event, context) => {
      return Promise.resolve()
        .then(() => {
          if (options.beforeHook) {
            options.beforeHook(event, context);
          }

          utils.validate(event, schemas.EVENT, { allowUnknown: true });

          const parsedEvent = event.Records.reduce((acc, record) => {
            try {
              const parsedRecord = AWS.DynamoDB.Converter.unmarshall({
                NewImage: { M: record.dynamodb.NewImage || {} },
                OldImage: { M: record.dynamodb.OldImage || {} },
                Keys: { M: record.dynamodb.Keys }
              });

              let doc = options.pickFields
                ? _.pick(parsedRecord.NewImage, options.pickFields)
                : parsedRecord.NewImage;

              const idResolver = options.idResolver || (() => {
                return options.idField
                  ? utils.assembleField(parsedRecord, options.idField, separator)
                  : utils.assembleField(parsedRecord, _.keys(parsedRecord.Keys), separator);
              });

              const id = idResolver(doc, parsedRecord.OldImage);

              const actionDescriptionObj = {
                _index: options.index
                  || `${indexPrefix}${utils.assembleField(parsedRecord, options.indexField, separator)}`,
                _type: options.type
                  || (
                    options.typeField
                    && utils.assembleField(parsedRecord, options.typeField, separator)
                  ),
                _id: id
              };

              // Omit blank _type
              if (!actionDescriptionObj._type) {
                delete actionDescriptionObj._type;
              }

              if (options.parentField) {
                actionDescriptionObj.parent = utils.getField(parsedRecord, options.parentField);
              }

              if (options.versionResolver || options.versionField) {
                const version = options.versionResolver
                  ? options.versionResolver(doc, parsedRecord.OldImage)
                  : utils.getField(parsedRecord, options.versionField);
                utils.validate(version, schemas.VERSION.label(options.versionField || 'resolved version'));
                actionDescriptionObj.version = version;
                actionDescriptionObj.versionType = 'external';
              }

              if (options.transformRecordHook) {
                doc = options.transformRecordHook(doc, parsedRecord.OldImage);
              }

              if (doc) {
                let action;
                switch (record.eventName) {
                  case 'INSERT':
                  case 'MODIFY':
                    action = { index: actionDescriptionObj };
                    acc.actions.push(action);
                    acc.actions.push(doc);
                    break;

                  case 'REMOVE':
                    if (_.has(actionDescriptionObj, 'version')) {
                      actionDescriptionObj.version++;
                    }
                    action = { delete: actionDescriptionObj };
                    acc.actions.push(action);
                    break;

                  default:
                    throw new errors.UnknownEventNameError(record);
                }

                acc.meta.push({
                  event: _.assign(
                    {},
                    record,
                    {
                      dynamodb: _.assign({}, record.dynamodb, parsedRecord)
                    }
                  ),
                  action,
                  document: doc
                });
              }
            } catch (err) {
              if (options.recordErrorHook) {
                options.recordErrorHook(event, context, err);
              } else {
                throw err;
              }
            }

            return acc;
          }, { actions: [], meta: [] });

          if (parsedEvent.actions.length === 0) {
            return {
              result: {
                took: 0,
                errors: false,
                items: []
              },
              meta: parsedEvent.meta
            };
          }

          return promiseRetry(retry => {
            return esclient
              .bulk(_.defaults({ body: parsedEvent.actions }, bulkOpts))
              .then(result => {
                return {
                  result,
                  meta: parsedEvent.meta
                };
              })
              .catch(retry);
          }, retryOptions);
        })
        .then(result => {
          if (options.afterHook) {
            return Promise.resolve()
              .then(() => options.afterHook(event, context, result.result, result.meta))
              .then(hookResult => {
                return hookResult !== undefined ? hookResult : result.result;
              });
          }
          return result.result;
        })
        .catch(err => {
          if (options.errorHook) {
            return options.errorHook(event, context, err);
          }
          throw err;
        });
    });

  handler.CLIENT = esclient;
  return handler;
};
