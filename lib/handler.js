'use strict';

const _ = require('lodash');
const aeclient = require('aws-elasticsearch-client');
const lambdaHandler = require('lambda-handler-as-promised');
const marshaler = require('dynamodb-marshaler');
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

  const handler = lambdaHandler((event, context) => {
    return Promise.resolve().then(() => {
      if (options.beforeHook) {
        options.beforeHook(event, context);
      }

      utils.validate(event, schemas.EVENT, { allowUnknown: true });

      const parsedEvent = event.Records.reduce((acc, record) => {
        try {
          const parsedRecord = marshaler.toJS({
            NewImage: { M: record.dynamodb.NewImage || {} },
            OldImage: { M: record.dynamodb.OldImage || {} },
            Keys: { M: record.dynamodb.Keys }
          });

          const id = options.idField
            ? utils.assembleField(parsedRecord, options.idField, separator)
            : utils.assembleField(parsedRecord, _.keys(parsedRecord.Keys), separator);

          const actionDescriptionObj = {
            _index: options.index
              || `${indexPrefix}${utils.assembleField(parsedRecord, options.indexField, separator)}`,
            _type: options.type
              || utils.assembleField(parsedRecord, options.typeField, separator),
            _id: id
          };

          if (options.parentField) {
            actionDescriptionObj.parent = utils.getField(parsedRecord, options.parentField);
          }

          if (options.versionField) {
            const version = utils.getField(parsedRecord, options.versionField);
            utils.validate(version, schemas.VERSION.label(options.versionField));
            actionDescriptionObj.version = version;
            actionDescriptionObj.versionType = 'external';
          }

          let doc = options.pickFields
            ? _.pick(parsedRecord.NewImage, options.pickFields)
            : parsedRecord.NewImage;

          if (options.transformRecordHook) {
            doc = options.transformRecordHook(doc);

            if (!doc) {
              throw new Error('transformRecordHook must return an object');
            }
          }

          let action;
          switch (record.eventName) {
            case 'INSERT':
            case 'MODIFY':
              action = { [options.upsert ? 'update' : 'index']: actionDescriptionObj };
              acc.actions.push(action);
              if (options.upsert) {
                acc.actions.push({ doc, doc_as_upsert: true });
              } else acc.actions.push(doc);
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
