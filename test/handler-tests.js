'use strict';

const _ = require('lodash');
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

function formatErrorMessage(messages) {
  return messages.join('. ');
}

describe('handler', function() {

  before(function() {
    lambdaTester.checkForResourceLeak(true);
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
        indexPrefix: 'baz',
        type: 'foo',
        typeField: 'bar'
      };

      expect(() => lambdaHandler(testOptions))
        .to.throw(errors.ValidationError)
        .with.property('message', formatErrorMessage([
          '"elasticsearch" conflict with forbidden peer "es"',
          '"options" contains a conflict between exclusive peers [index, indexField]',
          '"options" contains a conflict between exclusive peers [type, typeField]',
          '"index" conflict with forbidden peer "indexPrefix"'
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
        indexPrefix: 5,
        typeField: {},
        pickFields: {},
        versionField: {},
        retryOptions: 2
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
          'child "indexPrefix" fails because ["indexPrefix" must be a string]',
          'child "typeField" fails because ["typeField" must be a string, "typeField" must be an array]',
          'child "pickFields" fails because ["pickFields" must be a string, "pickFields" must be an array]',
          'child "versionField" fails because ["versionField" must be a string]',
          'child "retryOptions" fails because ["retryOptions" must be an object]'
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

    it('should throw when required options are missing', function() {
      const testOptions = {
        indexPrefix: 'foo'
      };

      expect(() => lambdaHandler(testOptions))
        .to.throw(errors.ValidationError)
        .with.property('message', formatErrorMessage([
          '"options" must contain at least one of [index, indexField]',
          '"options" must contain at least one of [type, typeField]',
          '"indexPrefix" missing required peer "indexField"'
        ]));
    });

    it('should throw when elasticsearch options are invalid', function() {
      const testOptions = {
        elasticsearch: {
          bulk: ''
        },
        index: 'index',
        type: 'type'
      };

      expect(() => lambdaHandler(testOptions))
        .to.throw(errors.ValidationError)
        .with.property('message', formatErrorMessage([
          'child "elasticsearch" fails because [child "bulk" fails because ["bulk" must be an object]]'
        ]));
    });

    it('should throw when elasticsearch.bulk options are invalid', function() {
      const testOptions = {
        es: {
          bulk: {
            body: {}
          }
        },
        index: 'index',
        type: 'type'
      };

      expect(() => lambdaHandler(testOptions))
        .to.throw(errors.ValidationError)
        .with.property('message', formatErrorMessage([
          'child "es" fails because [child "bulk" fails because [child "body" fails because ["body" is not allowed]]]'
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

      sinon.stub(handler.CLIENT, 'bulk').resolves();

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

      let hookCalled = false;
      const testItemKeys = { id: uuid.v4() };
      const testItemData = { data: 'some data', nestedData: { data: 'nested data' } };
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: testItemKeys,
        new: testItemData
      });

      const handler = lambdaHandler({
        afterHook: (event, context, result, parsedRecords) => {
          hookCalled = true;
          expect(event).to.deep.equal(testEvent);
          expect(context)
            .to.exist
            .and.to.have.property('awsRequestId');
          expect(result)
            .to.exist
            .and.to.deep.equal(testResult);
          expect(parsedRecords)
            .to.exist
            .and.to.deep.equal([
              {
                event: {
                  eventName: 'INSERT',
                  eventSource: testEvent.Records[0].eventSource,
                  dynamodb: {
                    Keys: testItemKeys,
                    NewImage: _.assign({}, testItemKeys, testItemData),
                    OldImage: {},
                    StreamViewType: testEvent.Records[0].dynamodb.StreamViewType
                  }
                },
                action: { index: { _index: 'index', _type: 'type', _id: testItemKeys.id } },
                document: _.assign({}, testItemKeys, testItemData)
              }
            ]);
        },
        index: 'index',
        type: 'type'
      });

      const stub = sinon.stub(handler.CLIENT, 'bulk').resolves(testResult);

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => {
          expect(stub.called).to.be.true;
          expect(hookCalled).to.be.true;
        });
    });

    it('should use return value from "afterHook" when provided', function() {
      const testEvent = formatEvent();
      const testHookResult = uuid.v4();

      const handler = lambdaHandler({
        afterHook: () => {
          return Promise.resolve(testHookResult);
        },
        index: 'index',
        type: 'type'
      });

      sinon.stub(handler.CLIENT, 'bulk').resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(result).to.be.deep.equal(testHookResult);
        });
    });

    it('should call "recordErrorHook" when provided and should not throw', function() {
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
      let hookCalled = false;
      const testResult = uuid.v4();
      const testEvent = formatEvent();
      const testError = new Error('Winter is coming!');

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

      const stub = sinon.stub(handler.CLIENT, 'bulk').rejects(testError);

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(result => {
          expect(stub.called).to.be.true;
          expect(hookCalled).to.be.true;
          expect(result).to.equal(testResult);
        });
    });
  });

  describe('separator', function() {
    it('should use separator when provided', function() {
      const testSeparator = '~';
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2
        }
      });

      const handler = lambdaHandler({
        separator: testSeparator,
        indexField: ['field1', 'field2'],
        typeField: ['field1', 'field2']
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          const expectedConcat = `${testField1}${testSeparator}${testField2}`;
          return expect(value).to.have.deep.property('body[0].index')
            .that.containSubset({
              _id: expectedConcat,
              _index: expectedConcat,
              _type: expectedConcat
            });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should support empty separator', function() {
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2
        }
      });

      const handler = lambdaHandler({
        separator: '',
        indexField: ['field1', 'field2'],
        typeField: ['field1', 'field2']
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          const expectedConcat = `${testField1}${testField2}`;
          return expect(value).to.have.deep.property('body[0].index')
            .that.containSubset({
              _id: expectedConcat,
              _index: expectedConcat,
              _type: expectedConcat
            });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });
  });

  describe('id', function() {
    it('should use "idField" when provided (single field)', function() {
      const testField = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field: testField,
          otherField: uuid.v4()
        }
      });

      const handler = lambdaHandler({
        idField: 'field',
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[0].index._id', testField);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should use "idField" when provided (multiple fields)', function() {
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

      const handler = lambdaHandler({
        idField: ['field1', 'field2'],
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[0].index._id', `${testField1}.${testField2}`);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should throw when "idField" not found in record', function() {
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
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2
        }
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[0].index._id', `${testField1}.${testField2}`);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });
  });

  describe('index', function() {
    it('should use "index" value when provided', function() {
      const testIndex = uuid.v4();
      const testEvent = formatEvent({ name: 'INSERT' });

      const handler = lambdaHandler({
        index: testIndex,
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[0].index._index', testIndex);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should use "indexField" when provided (single field)', function() {
      const testField = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: { field: testField }
      });

      const handler = lambdaHandler({
        indexField: 'field',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[0].index._index', testField);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should use "indexField" when provided (multiple fields)', function() {
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2
        }
      });

      const handler = lambdaHandler({
        indexField: ['field1', 'field2'],
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[0].index._index', `${testField1}.${testField2}`);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should use "indexPrefix" when provided', function() {
      const testField = uuid.v4();
      const testIndexPrefix = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: { field: testField }
      });

      const handler = lambdaHandler({
        indexField: 'field',
        indexPrefix: testIndexPrefix,
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value)
            .to.have.deep.property('body[0].index._index', `${testIndexPrefix}${testField}`);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should throw when "indexField" not found in record', function() {
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
      const testEvent = formatEvent({ name: 'INSERT' });

      const handler = lambdaHandler({
        index: 'index',
        type: testType
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[0].index._type', testType);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should use "typeField" when provided (single field)', function() {
      const testField = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: { field: testField }
      });

      const handler = lambdaHandler({
        index: 'index',
        typeField: 'field'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[0].index._type', testField);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should use "typeField" when provided (multiple fields)', function() {
      const testField1 = uuid.v4();
      const testField2 = uuid.v4();
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: {
          field1: testField1,
          field2: testField2
        }
      });

      const handler = lambdaHandler({
        index: 'index',
        typeField: ['field1', 'field2']
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[0].index._type', `${testField1}.${testField2}`);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should throw when "typeField" not found in record', function() {
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
      const testDoc = {
        field1: uuid.v4(),
        field2: uuid.v4()
      };
      const testEvent = formatEvent({
        name: 'INSERT',
        new: testDoc
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        pickFields: 'field1'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[1]')
            .that.deep.equals({ field1: testDoc.field1 });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should use "pickFields" when provided (multiple fields)', function() {
      const testDoc = {
        field1: uuid.v4(),
        field2: uuid.v4(),
        field3: uuid.v4()
      };
      const testEvent = formatEvent({
        name: 'INSERT',
        new: testDoc
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        pickFields: ['field1', 'field2']
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[1]')
            .that.deep.equals({ field1: testDoc.field1, field2: testDoc.field2 });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should pick all the fields when "pickFields" not provided', function() {
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

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[1]')
            .that.deep.equals(testDoc);
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });
  });

  describe('versionField', function() {
    it('should use field`s value when "versionField" is provided', function() {
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

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        versionField: 'field2'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.containSubset({
            body: [
              { index: { version: testDoc.field2, versionType: 'external' } }
            ]
          });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should support 0 version', function() {
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

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        versionField: 'field2'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.containSubset({
            body: [
              { index: { version: testDoc.field2, versionType: 'external' } }
            ]
          });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should not set "version" and "versionType" fields when "versionField" is not provided', function() {
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

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.have.deep.property('body[0].index')
            .that.not.have.any.keys('version', 'versionType');
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should throw when "versionField" not found in record', function() {
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

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        versionField: 'field2'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.containSubset({
            body: [
              { delete: { version: testDoc.field2 + 1, versionType: 'external' } }
            ]
          });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });
  });

  describe('event validation', function() {
    it('should throw when invalid event received', function() {
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
      const testEvent = formatEvent();
      testEvent.junk = 'junk';
      testEvent.Records[0].junk = 'junk';
      testEvent.Records[0].dynamodb.junk = 'junk';

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      sinon.stub(handler.CLIENT, 'bulk').resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult();
    });
  });

  describe('event names (types)', function() {
    it('should support "INSERT" event', function() {
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

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.containSubset({
            body: [
              { index: { _index: 'index', _type: 'type', _id: testDoc.field1 } },
              testDoc
            ]
          });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should support "MODIFY" event', function() {
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

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.containSubset({
            body: [
              { index: { _index: 'index', _type: 'type', _id: testDoc.field1 } },
              testDoc
            ]
          });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should support "REMOVE" event', function() {
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

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.containSubset({
            body: [
              { delete: { _index: 'index', _type: 'type', _id: testDoc.field1 } }
            ]
          });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should throw when unknown event name found', function() {
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
      const testDoc = { field: uuid.v4() };
      const testEvent = formatEvent({
        name: 'INSERT',
        keys: { field: testDoc.field },
        newImage: testDoc
      });

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.containSubset({
            body: [
              { index: { _id: testDoc.field } },
              testDoc
            ]
          });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });

    it('should support multiple events', function() {
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

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs(sinon.match(value => {
          return expect(value).to.containSubset({
            body: [
              { index: { _id: testDoc1.field } },
              testDoc1,
              { index: { _id: testDoc2.field } },
              testDoc2
            ]
          });
        }))
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });
  });

  describe('bulk options', function() {
    it('should pass bulk options to the request when provided', function() {
      const testKeys = { id: uuid.v4() };
      const testEvent = formatEvent({ name: 'INSERT', keys: testKeys });
      const testBulkOptions = {
        refresh: 'true'
      };

      const handler = lambdaHandler({
        elasticsearch: {
          bulk: testBulkOptions
        },
        index: 'index',
        type: 'type'
      });

      const mock = sinon.mock(handler.CLIENT).expects('bulk')
        .once()
        .withExactArgs({
          body: [
            {
              index: {
                _index: 'index',
                _type: 'type',
                _id: testKeys.id
              }
            },
            testKeys
          ],
          refresh: testBulkOptions.refresh
        })
        .resolves();

      return lambdaTester(handler)
        .event(testEvent)
        .expectResult(() => mock.verify());
    });
  });

  describe('retryOptions', function() {
    it('should not retry on error when retryOptions is not passed', function() {
      const testEvent = formatEvent();
      const testError = new Error('indexing error');

      const handler = lambdaHandler({
        index: 'index',
        type: 'type'
      });

      const stub = sinon.stub(handler.CLIENT, 'bulk').rejects(testError);

      return lambdaTester(handler)
        .event(testEvent)
        .expectError(err => {
          expect(stub.calledOnce).to.be.true;
          expect(err).to.deep.equal(testError);
        });
    });

    it('should retry on error specified number of times when retryOptions is passed', function() {
      const testEvent = formatEvent();
      const testError = new Error('indexing error');
      const retryCount = 2;

      const handler = lambdaHandler({
        index: 'index',
        type: 'type',
        retryOptions: {
          retries: retryCount
        }
      });

      const stub = sinon.stub(handler.CLIENT, 'bulk').rejects(testError);

      return lambdaTester(handler)
        .event(testEvent)
        .expectError(err => {
          expect(stub.callCount).to.be.equal(retryCount + 1);
          expect(err).to.deep.equal(testError);
        });
    });
  });
});
