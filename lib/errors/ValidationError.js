class ValidationError extends Error {
  constructor(joiError, message) {
    super(message || joiError.message);
    this.name = 'ValidationError';
    this.details = joiError.details;
  }
}

module.exports = ValidationError;
