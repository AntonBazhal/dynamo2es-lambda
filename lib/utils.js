'use strict';

const _ = require('lodash');
const joi = require('joi');

const errors = require('./errors');

module.exports = {
  validate(value, schema, options) {
    const validationResult = joi.validate(
      value,
      schema,
      _.assign(
        {
          abortEarly: false,
          allowUnknown: false,
          convert: false
        },
        options
      )
    );

    if (validationResult.error) {
      throw new errors.ValidationError(validationResult.error);
    }

    return validationResult.value;
  },

  getField(parsedRecord, path) {
    const value = [parsedRecord.Keys, parsedRecord.NewImage, parsedRecord.OldImage]
      .reduce((acc, entry) => {
        return acc === undefined
          ? _.get(entry, path)
          : acc;
      }, undefined);

    if (value === undefined) {
      throw new errors.FieldNotFoundError(parsedRecord, path);
    }

    return value;
  },

  assembleField(parsedRecord, paths, separator) {
    if (Array.isArray(paths)) {
      return paths.map(path => this.getField(parsedRecord, path)).join(separator);
    }
    return this.getField(parsedRecord, paths);
  }
};
