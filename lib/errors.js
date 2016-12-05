'use strict';

class FieldNotFoundError extends Error {
  constructor(parsedRecord, path) {
    super(`"${path}" field not found in record`);
    this.name = 'FieldNotFoundError';
    this.details = parsedRecord;
  }
}

class UnknownEventNameError extends Error {
  constructor(record) {
    super(`"${record.eventName}" is an unknown event name`);
    this.name = 'UnknownEventNameError';
    this.details = record;
  }
}

class ValidationError extends Error {
  constructor(joiError, message) {
    super(message || joiError.message);
    this.name = 'ValidationError';
    this.details = joiError.details;
  }
}

module.exports = {
  FieldNotFoundError,
  UnknownEventNameError,
  ValidationError
};
