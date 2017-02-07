'use strict';

const aeclient = require('aws-elasticsearch-client');
const chai = require('chai');
const chaiSubset = require('chai-subset');
const lambdaTester = require('lambda-tester');
const sinon = require('sinon');
const uuid = require('uuid');

const formatEvent = require('./utils/ddb-stream-event-formatter');
const lambdaHandler = require('../');

chai.use(chaiSubset);
const expect = chai.expect;

const errors = lambdaHandler.errors;
let sandbox;

function stubESCalls(handler) {
  return sandbox.stub(aeclient, 'create', () => {
    return {
      bulk: handler || (() => Promise.resolve({
        took: 0,
        errors: false,
        items: []
      }))
    };
  });
}

function formatErrorMessage(messages) {
  return messages.join('. ');
}

describe('handler', function() {

  before(function() {
    sandbox = sinon.sandbox.create();
  });

  afterEach(function() {
    sandbox.restore();
  });

  describe('options validation', function() {
    it('should handle case when options object is not passed', function() {
      expect(() => lambdaHandler()).to.throw(errors.ValidationError);
    });

    it('should throw when incompatible options are present', function() {
      const testOptions = {
        es: {},
        elasticsearch: {},
        index: 'foo',
        indexField: 'bar',
        type: 'foo',
        typeField: 'bar'
      };

      expect(() => lambdaHandler(testOptions))
        .to.throw(errors.ValidationError)
        .with.property('message', formatErrorMessage([
          '"elasticsearch" conflict with forbidden peer "es"',
          '"value" contains a conflict between exclusive peers [index, indexField]',
          '"value" contains a conflict between exclusive peers [type, typeField]'
        ]));
    });

    it('should throw when options are invalid (first set)', function() {
      const testOptions = {
        es: 'foo',
        beforeHook: {},
        afterHook: {},
        recordErrorHook: {},
        errorHook: {},
        separator: 5,
        idField: {},
        indexField: {},
        typeField: {},
        pickFields: {},
        versionField: {}
      };

      expect(() => lambdaHandler(testOptions))
        .to.throw(errors.ValidationError)
        .with.property('message', formatErrorMessage([
          'child "es" fails because ["es" must be an object]',
          'child "beforeHook" fails because ["beforeHook" must be a Function]',
          'child "afterHook" fails because ["afterHook" must be a Function]',
          'child "recordErrorHook" fails because ["recordErrorHook" must be a Function]',
          'child "errorHook" fails because ["errorHook" must be a Function]',
          'child "separator" fails because ["separator" must be a string]',
          'child "idField" fails because ["idField" must be a string, "idField" must be an array]',
          'child "indexField" fails because ["indexField" must be a string, "indexField" must be an array]',
          'child "typeField" fails because ["typeField" must be a string, "typeField" must be an array]',
          'child "pickFields" fails because ["pickFields" must be a string, "pickFields" must be an array]',
          'child "versionField" fails because ["versionField" must be a string]',
          '"value" must contain at least one of [index, indexField]',
          '"value" must contain at least one of [type, typeField]'
        ]));
    });

    it('should throw when options are invalid (second set)', function() {
      const testOptions = {
        elasticsearch: 'foo',
        index: 1,
        type: 2
      };

      expect(() => lambdaHandler(testOptions))
        .to.throw(errors.ValidationError)
        .with.property('message', formatErrorMessage([
          'child "elasticsearch" fails because ["elasticsearch" must be an object]',
          'child "index" fails because ["index" must be a string]',
          'child "type" fails because ["type" must be a string]'
        ]));
    });

    it('should throw when unknown options passed', function() {
      const testOptions = {
        junk: 'junk',
        index: 'index',
        type: 'type'
      };

      expect(() => lambdaHandler(testOptions))
        .to.throw(errors.ValidationError)
        .with.property('message', formatErrorMessage([
          '"junk" is not allowed'
        ]));
    });
  });

  describe('hooks', function() {
    it('should call "beforeHook" when provided', function() {
      stubESCalls();

      let hookCalled = false;
      const testEvent = formatEvent();

      const handler = lambdaHandler({
        beforeHook: (event, context) => {
          hookCalled = true;
          expect(event).to.deep.equal(testEvent);
          expect(context)
            .to.exist
            .and.to.have.property('awsRequestId');
        },
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => {
          expect(hookCalled).to.be.true;
        });
    });

    it('should call "afterHook" when provided', function() {
      const testResult = {
        meaningOfLife: 42
      };
      stubESCalls(() => Promise.resolve(testResult));

      let hookCalled = false;
      const testEvent = formatEvent();

      const handler = lambdaHandler({
        afterHook: (event, context, result) => {
          hookCalled = true;
          expect(event).to.deep.equal(testEvent);
          expect(context)
            .to.exist
            .and.to.have.property('awsRequestId');
          expect(result)
            .to.exist
            .and.to.deep.equal(testResult);
        },
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => {
          expect(hookCalled).to.be.true;
        });
    });

    it('should call "recordErrorHook" when provided and should not throw', function() {
      stubESCalls();

      let hookCalled = false;
      const testEvent = formatEvent();

      const handler = lambdaHandler({
        recordErrorHook: (event, context, err) => {
          hookCalled = true;
          expect(event).to.deep.equal(testEvent);
          expect(context)
            .to.exist
            .and.to.have.property('awsRequestId');
          expect(err).to.exist
            .and.to.be.an.instanceOf(errors.FieldNotFoundError)
            .with.property('message', '"foo" field not found in record');
        },
        indexField: 'foo',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => {
          expect(hookCalled).to.be.true;
        });
    });

    it('should call "errorHook" when provided, return result and should not throw', function() {
      const testError = new Error('Winter is coming!');
      stubESCalls(() => Promise.reject(testError));

      let hookCalled = false;
      const testResult = uuid.v4();
      const testEvent = formatEvent();

      const handler = lambdaHandler({
        errorHook: (event, context, err) => {
          hookCalled = true;
          expect(event).to.deep.equal(testEvent);
          expect(context).to.exist.and.to.have.property('awsRequestId');
          expect(err).to.exist.and.to.deep.equal(testError);
          return testResult;
        },
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(hookCalled).to.be.true;
          expect(result).to.equal(testResult);
        });
    });
  });

  describe('separator', function() {
    it('should use separator when provided', function() {
      const testSeparator = '~';
      const testResult = uuid.v4();
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2
        }
      });

      stubESCalls(params => {
        const expectedConcat = `${testField1}${testSeparator}${testField2}`;
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.containSubset({
            _id: expectedConcat,
            _index: expectedConcat,
            _type: expectedConcat
          });

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        separator: testSeparator,
        indexField: ['field1', 'field2'],
        typeField: ['field1', 'field2']
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should support empty separator', function() {
      const testResult = uuid.v4();
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2
        }
      });

      stubESCalls(params => {
        const expectedConcat = `${testField1}${testField2}`;
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.containSubset({
            _id: expectedConcat,
            _index: expectedConcat,
            _type: expectedConcat
          });

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        separator: '',
        indexField: ['field1', 'field2'],
        typeField: ['field1', 'field2']
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });
  });

  describe('id', function() {
    it('should use "idField" when provided (single field)', function() {
      const testResult = uuid.v4();
      const testField = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field: testField,
          otherField: uuid.v4()
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.to.have.property('_id', testField);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        idField: 'field',
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should use "idField" when provided (multiple fields)', function() {
      const testResult = uuid.v4();
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2,
          field3: uuid.v4()
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.to.have.property('_id', `${testField1}.${testField2}`);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        idField: ['field1', 'field2'],
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should throw when "idField" not found in record', function() {
      stubESCalls();

      const testEvent = formatEvent();
      const handler = lambdaHandler({
        idField: 'notFoundField',
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectError(err => {
          expect(err)
            .to.be.an.instanceOf(errors.FieldNotFoundError)
            .with.property('message', '"notFoundField" field not found in record');
        });
    });

    it('should concatenate record keys when "idField" not provided', function() {
      const testResult = uuid.v4();
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.to.have.property('_id', `${testField1}.${testField2}`);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });
  });

  describe('index', function() {
    it('should use "index" value when provided', function() {
      const testIndex = uuid.v4();
      const testResult = uuid.v4();
      const testEvent = formatEvent({ name: 'INSERT' });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.to.have.property('_index', testIndex);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: testIndex,
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should use "indexField" when provided (single field)', function() {
      const testResult = uuid.v4();
      const testField = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: { field: testField }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.to.have.property('_index', testField);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        indexField: 'field',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should use "indexField" when provided (multiple fields)', function() {
      const testResult = uuid.v4();
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.to.have.property('_index', `${testField1}.${testField2}`);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        indexField: ['field1', 'field2'],
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should throw when "indexField" not found in record', function() {
      stubESCalls();

      const testEvent = formatEvent();
      const handler = lambdaHandler({
        indexField: 'notFoundField',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectError(err => {
          expect(err)
            .to.be.an.instanceOf(errors.FieldNotFoundError)
            .with.property('message', '"notFoundField" field not found in record');
        });
    });
  });

  describe('type', function() {
    it('should use "type" value when provided', function() {
      const testType = uuid.v4();
      const testResult = uuid.v4();
      const testEvent = formatEvent({ name: 'INSERT' });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.to.have.property('_type', testType);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: testType
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should use "typeField" when provided (single field)', function() {
      const testResult = uuid.v4();
      const testField = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: { field: testField }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.to.have.property('_type', testField);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        typeField: 'field'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should use "typeField" when provided (multiple fields)', function() {
      const testResult = uuid.v4();
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[0])
          .to.exist
          .and.to.have.property('index')
          .that.is.an('object')
          .and.to.have.property('_type', `${testField1}.${testField2}`);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        typeField: ['field1', 'field2']
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should throw when "typeField" not found in record', function() {
      stubESCalls();

      const testEvent = formatEvent();
      const handler = lambdaHandler({
        index: 'index',
        typeField: 'notFoundField'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectError(err => {
          expect(err)
          .to.be.an.instanceOf(errors.FieldNotFoundError)
          .with.property('message', '"notFoundField" field not found in record');
        });
    });
  });

  describe('pickFields', function() {
    it('should use "pickFields" when provided (single field)', function() {
      const testResult = uuid.v4();
      const testDoc = {
        field1: uuid.v4(),
        field2: uuid.v4()
      };
      const testEvent = formatEvent({
        name: 'INSERT',
        new: testDoc
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[1])
          .to.exist
          .and.to.be.an('object')
          .and.to.deep.equal({ field1: testDoc.field1 });

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        pickFields: 'field1'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should use "pickFields" when provided (multiple fields)', function() {
      const testResult = uuid.v4();
      const testDoc = {
        field1: uuid.v4(),
        field2: uuid.v4(),
        field3: uuid.v4()
      };
      const testEvent = formatEvent({
        name: 'INSERT',
        new: testDoc
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[1])
          .to.exist
          .and.to.be.an('object')
          .and.to.deep.equal({
            field1: testDoc.field1,
            field2: testDoc.field2
          });

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        pickFields: ['field1', 'field2']
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should pick all the fields when "pickFields" not provided', function() {
      const testResult = uuid.v4();
      const testDoc = {
        field1: uuid.v4(),
        field2: uuid.v4()
      };
      const testEvent = formatEvent({
        name: 'INSERT',
        new: testDoc,
        keys: {
          field1: testDoc.field1
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body');
        expect(params.body[1])
          .to.exist
          .and.to.be.an('object')
          .and.to.deep.equal(testDoc);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });
  });

  describe('versionField', function() {
    it('should use field`s value when "versionField" is provided', function() {
      const testResult = uuid.v4();
      const testDoc = {
        field1: uuid.v4(),
        field2: 1
      };
      const testEvent = formatEvent({
        name: 'INSERT',
        new: testDoc,
        keys: {
          field1: testDoc.field1
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body')
          .that.is.an('array')
          .with.lengthOf(2);
        expect(params.body[0])
          .to.be.an('object')
          .and.to.have.property('index')
          .that.containSubset({
            version: testDoc.field2,
            versionType: 'external'
          });

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        versionField: 'field2'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should support 0 version', function() {
      const testResult = uuid.v4();
      const testDoc = {
        field1: uuid.v4(),
        field2: 0
      };
      const testEvent = formatEvent({
        name: 'INSERT',
        new: testDoc,
        keys: {
          field1: testDoc.field1
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body')
          .that.is.an('array')
          .with.lengthOf(2);
        expect(params.body[0])
          .to.be.an('object')
          .and.to.have.property('index')
          .that.containSubset({
            version: testDoc.field2,
            versionType: 'external'
          });

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        versionField: 'field2'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should not set "version" and "versionType" fields when "versionField" is not provided', function() {
      const testResult = uuid.v4();
      const testDoc = {
        field1: uuid.v4()
      };
      const testEvent = formatEvent({
        name: 'INSERT',
        new: testDoc,
        keys: {
          field1: testDoc.field1
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body')
          .that.is.an('array')
          .with.lengthOf(2);
        expect(params.body[0])
          .to.be.an('object')
          .and.to.have.property('index');

        const actionDescription = params.body[0].index;
        expect(actionDescription).not.to.have.property('version');
        expect(actionDescription).not.to.have.property('versionType');

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should throw when "versionField" not found in record', function() {
      stubESCalls();

      const testEvent = formatEvent();
      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        versionField: 'notFoundField'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectError(err => {
          expect(err)
            .to.be.an.instanceOf(errors.FieldNotFoundError)
            .with.property('message', '"notFoundField" field not found in record');
        });
    });

    it('should throw when version is invalid', function() {
      stubESCalls();

      const testEvent = formatEvent({
        name: 'INSERT',
        new: {
          _version: '1'
        },
        keys: {
          key: uuid.v4()
        }
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        versionField: '_version'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectError(err => {
          expect(err)
            .to.be.an.instanceOf(errors.ValidationError)
            .with.property('message', '"_version" must be a number');
        });
    });

    it('should increment version for "REMOVE" event', function() {
      const testResult = uuid.v4();
      const testDoc = {
        field1: uuid.v4(),
        field2: 1
      };
      const testEvent = formatEvent({
        name: 'REMOVE',
        old: testDoc,
        keys: {
          field1: testDoc.field1
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body')
          .that.is.an('array')
          .with.lengthOf(1);
        expect(params.body[0])
          .to.be.an('object')
          .and.to.have.property('delete')
          .that.containSubset({
            version: testDoc.field2 + 1,
            versionType: 'external'
          });

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        versionField: 'field2'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });
  });

  describe('event validation', function() {
    it('should throw when invalid event received', function() {
      stubESCalls();

      const testEvent = formatEvent();
      delete testEvent.Records[0].eventName;
      delete testEvent.Records[0].dynamodb;

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectError(err => {
          expect(err).to.be.an.instanceOf(errors.ValidationError)
          .with.property('message', 'child "Records" fails because ["Records" at ' +
            'position 0 fails because [child "eventName" fails because ["eventName" ' +
            'is required], child "dynamodb" fails because ["dynamodb" is required]]]');
        });
    });

    it('should call errorHook and not throw when invalid event received and errorHook passed', function() {
      stubESCalls();

      let hookCalled = false;

      const testEvent = formatEvent();
      delete testEvent.Records[0].eventName;

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        errorHook: (event, context, err) => {
          hookCalled = true;
          expect(err).to.be.an.instanceOf(errors.ValidationError);
        }
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => {
          expect(hookCalled).to.be.true;
        });
    });

    it('should not throw when event has unknown fields', function() {
      stubESCalls();

      const testEvent = formatEvent();
      testEvent.junk = 'junk';
      testEvent.Records[0].junk = 'junk';
      testEvent.Records[0].dynamodb.junk = 'junk';

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult();
    });
  });

  describe('event names (types)', function() {
    it('should support "INSERT" event', function() {
      const testResult = uuid.v4();
      const testDoc = {
        field1: uuid.v4(),
        field2: uuid.v4()
      };
      const testEvent = formatEvent({
        name: 'INSERT',
        new: testDoc,
        keys: {
          field1: testDoc.field1
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body')
          .that.is.an('array')
          .with.lengthOf(2);
        expect(params.body[0])
          .to.be.an('object')
          .and.to.have.property('index')
          .that.containSubset({
            _index: 'index',
            _type: 'type',
            _id: testDoc.field1
          });
        expect(params.body[1])
          .to.be.an('object')
          .and.to.deep.equal(testDoc);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should support "MODIFY" event', function() {
      const testResult = uuid.v4();
      const testDoc = {
        field1: uuid.v4(),
        field2: uuid.v4()
      };
      const testEvent = formatEvent({
        name: 'MODIFY',
        new: testDoc,
        keys: {
          field1: testDoc.field1
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body')
          .that.is.an('array')
          .with.lengthOf(2);
        expect(params.body[0])
          .to.be.an('object')
          .and.to.have.property('index')
          .that.containSubset({
            _index: 'index',
            _type: 'type',
            _id: testDoc.field1
          });
        expect(params.body[1])
          .to.be.an('object')
          .that.deep.equals(testDoc);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should support "REMOVE" event', function() {
      const testResult = uuid.v4();
      const testDoc = {
        field1: uuid.v4(),
        field2: uuid.v4()
      };
      const testEvent = formatEvent({
        name: 'REMOVE',
        new: testDoc,
        keys: {
          field1: testDoc.field1
        }
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body')
          .that.is.an('array')
          .with.lengthOf(1);
        expect(params.body[0])
          .to.be.an('object')
          .and.to.have.property('delete')
          .that.containSubset({
            _index: 'index',
            _type: 'type',
            _id: testDoc.field1
          });

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should throw when unknown event name found', function() {
      stubESCalls();

      const testEvent = formatEvent({
        name: 'UNKNOWN'
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectError(err => {
          expect(err).to.be.an.instanceOf(errors.UnknownEventNameError);
          expect(err).to.have.property('message', '"UNKNOWN" is an unknown event name');
          expect(err).to.have.property('details').that.deep.equals(testEvent.Records[0]);
        });
    });

    it('should call recordErrorHook and not throw when unknown event name found and recordErrorHook passed', function() {
      stubESCalls();

      let handlerCalled = false;
      const testEvent = formatEvent({
        name: 'UNKNOWN'
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        recordErrorHook: (event, context, err) => {
          handlerCalled = true;
          expect(err).to.be.an.instanceOf(errors.UnknownEventNameError);
          expect(err).to.have.property('message', '"UNKNOWN" is an unknown event name');
          expect(err).to.have.property('details').that.deep.equals(testEvent.Records[0]);
        }
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(handlerCalled).to.be.true;
          expect(result).to.deep.equal({
            took: 0,
            errors: false,
            items: []
          });
        });
    });
  });

  describe('events count', function() {
    it('should support single event', function() {
      const testResult = uuid.v4();
      const testDoc = { field: uuid.v4() };
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: { field: testDoc.field },
        newImage: testDoc
      });

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body')
          .that.is.an('array')
          .with.lengthOf(2);
        expect(params.body[0])
          .to.be.an('object')
          .and.to.have.property('index')
          .that.has.property('_id', testDoc.field);
        expect(params.body[1])
          .to.be.an('object')
          .and.to.deep.equal(testDoc);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });

    it('should support multiple events', function() {
      const testResult = uuid.v4();
      const testDoc1 = { field: uuid.v4() };
      const testDoc2 = { field: uuid.v4() };
      const testEvent = formatEvent([
        {
          name: 'INSERT',
          keys: { field: testDoc1.field },
          newImage: testDoc1
        },
        {
          name: 'MODIFY',
          keys: { field: testDoc2.field },
          newImage: testDoc2
        }
      ]);

      stubESCalls(params => {
        expect(params)
          .to.exist
          .and.to.have.property('body')
          .that.is.an('array')
          .with.lengthOf(4);
        expect(params.body[0])
          .to.be.an('object')
          .and.to.have.property('index')
          .that.has.property('_id', testDoc1.field);
        expect(params.body[1])
          .to.be.an('object')
          .and.to.deep.equal(testDoc1);
        expect(params.body[2])
          .to.be.an('object')
          .and.to.have.property('index')
          .that.has.property('_id', testDoc2.field);
        expect(params.body[3])
          .to.be.an('object')
          .that.deep.equals(testDoc2);

        return Promise.resolve(testResult);
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.exist.and.to.be.equal(testResult);
        });
    });
  });
});
