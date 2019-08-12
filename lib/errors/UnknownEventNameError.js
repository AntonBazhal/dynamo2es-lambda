class UnknownEventNameError extends Error {
  constructor(record) {
    super(`"${record.eventName}" is an unknown event name`);
    this.name = 'UnknownEventNameError';
    this.details = record;
  }
}

module.exports = UnknownEventNameError;
