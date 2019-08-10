class FieldNotFoundError extends Error {
  constructor(parsedRecord, path) {
    super(`"${path}" field not found in record`);
    this.name = 'FieldNotFoundError';
    this.details = parsedRecord;
  }
}

module.exports = FieldNotFoundError;
