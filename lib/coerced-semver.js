const joi = require('@hapi/joi').extend(require('joi-extension-semver'));
const semver = require('semver');

module.exports = {
  base: joi.semver(),
  name: 'coercedSemver',
  coerce (value, state, options) { // eslint-disable-line no-unused-vars
    return (semver.coerce(value) || { version: null }).version;
  }
};
