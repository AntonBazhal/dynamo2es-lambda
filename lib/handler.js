'use strict';

const _ = require('lodash');
const aeclient = require('aws-elasticsearch-client');
const lambdaHandler = require('lambda-handler-as-promised');
const marshaler = require('dynamodb-marshaler');

const errors = require('./errors');
const schemas = require('./schemas');
const utils = require('./utils');

module.exports = function(options) {
  options = options || {}; // eslint-disable-line no-param-reassign

  utils.validate(options, schemas.HANDLER_OPTIONS);

  const esclient = aeclient.create(options.elasticsearch || options.es);
  const separator = _.has(options, 'separator') ? options.separator : '.';

  return lambdaHandler((event, context) => {
    return Promise.resolve().then(() => {
      if (options.beforeHook) {
        options.beforeHook(event, context);
      }

      utils.validate(event, schemas.EVENT, { allowUnknown: true });

      const actions = event.Records.reduce((acc, record) => {
        try {
          const parsedRecord = marshaler.toJS({
            newDoc: { M: record.dynamodb.NewImage || {} },
            oldDoc: { M: record.dynamodb.OldImage || {} },
            keys: { M: record.dynamodb.Keys }
          });

          const id = options.idField
            ? utils.assembleField(parsedRecord, options.idField, separator)
            : utils.assembleField(parsedRecord, _.keys(parsedRecord.keys), separator);

          const actionDescriptionObj = {
            _index: options.index
              || utils.assembleField(parsedRecord, options.indexField, separator),
            _type: options.type
              || utils.assembleField(parsedRecord, options.typeField, separator),
            _id: id
          };

          if (options.versionField) {
            const version = utils.getField(parsedRecord, options.versionField);
            utils.validate(version, schemas.VERSION.label(options.versionField));
            actionDescriptionObj.version = version;
            actionDescriptionObj.versionType = 'external';
          }

          const doc = options.pickFields
            ? _.pick(parsedRecord.newDoc, options.pickFields)
            : parsedRecord.newDoc;

          switch (record.eventName) {
            case 'INSERT':
            case 'MODIFY':
              acc.push({ index: actionDescriptionObj });
              acc.push(doc);
              break;

            case 'REMOVE':
              if (_.has(actionDescriptionObj, 'version')) {
                actionDescriptionObj.version++;
              }
              acc.push({ delete: actionDescriptionObj });
              break;

            default:
              throw new errors.UnknownEventNameError(record);
          }
        } catch (err) {
          if (options.recordErrorHook) {
            options.recordErrorHook(event, context, err);
          } else {
            throw err;
          }
        }

        return acc;
      }, []);

      if (actions.length === 0) {
        return {
          took: 0,
          errors: false,
          items: []
        };
      }

      return esclient.bulk({ body: actions });
    })
    .then(result => {
      if (options.afterHook) {
        options.afterHook(event, context, result);
      }
      return result;
    })
    .catch(err => {
      if (options.errorHook) {
        return options.errorHook(event, context, err);
      }
      throw err;
    });
  });
};
