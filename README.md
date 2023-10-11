# dynamo2es-lambda

[![Build Status][ci-image]][ci-url]
[![Coverage Status][coverage-image]][coverage-url]
[![NPM version][npm-image]][npm-url]
[![Dependencies Status][dependencies-image]][dependencies-url]
[![DevDependencies Status][devdependencies-image]][devdependencies-url]

Configurable [AWS Lambda][aws-lambda-url] handler to index documents from [DynamoDB Streams][dynamodb-streams-url] in [Amazon Elasticsearch Service][aws-elasticsearch-url].

## Installation

```bash
$ npm install --save dynamo2es-lambda @elastic/elasticsearch aws-sdk
```

## Usage

`dynamo2es-lambda` takes `options` object and returns [AWS Lambda][aws-lambda-url] handler (using [alpha-lambda][alpha-lambda-url]) that is ready to be connected to any [DynamoDB Stream][dynamodb-streams-url]. `options` object supports the following configuration options:

- **index** - { String } - Elasticsearch index to be used for all the documents; optional if `indexField` is provided
- **type** - { String } - Elasticsearch type to be used for all the documents; optional if `typeField` is provided
- **[elasticsearch]** - { Object }
  - **[client]** - { Object } - an [elasticsearch][elasticsearch-client-url] client instance (also works with the [legacy 16.x client][elasticsearch-legacy-client-url])
  - **[bulk]** - { Object } - aside from general Elasticsearch configuration, you can use this field to pass additional parameters to [bulk API][bulk-api-url]
- **[indexField]** - { String | String[] } - field(s) to be used as an Elasticsearch index; if multiple fields are provided, values are concatenated using `separator`; required if `indexPrefix` field is present; can't be used together with `index`
- **[indexPrefix]** - { String } - static string to be used as a prefix to form index together with `indexField` value
- **[typeField]** - { String | String[] } - field(s) to be used as an Elasticsearch type; if multiple fields are provided, values are concatenated using `separator`; can't be used together with `type`
- **[idField]** - { String | String[] } - field(s) to be used as an Elasticsearch id; if multiple fields are provided, values are concatenated using `separator`; can't be used together with `idResolver` [defaults to document's key field(s)]
- **[idResolver]** - { Function(record, old) } - optional function to format an Elasticsearch id; can't be used together with `idField`
- **[versionField]** - { String } - field to be used as an [external version for Elasticsearch document][elasticsearch-versioning-url] [by default no version check is performed]
- **[versionResolver]** - { Function(record, old) } - optional function to resolve a value to be used as an [external version for Elasticsearch document][elasticsearch-versioning-url]
- **[parentField]** - { String } - field to be used as a [parent id][elasticsearch-parent-child-url] [no parent by default]
- **[pickFields]** - { String | String[] } - by default, the whole document is sent to Elasticsearch for indexing; if this option is provided, only field(s) specified would be sent
- **[separator]** - { String } - separator that is used to concatenate fields [defaults to `'.'`]
- **[beforeHook]** - { Function(event, context) } - function to be called before any processing is done
- **[afterHook]** - { Function(event, context, result, meta) } - function to be called after all the processing is done; `meta` object contains parsed event data, action description and document that was indexed
- **[recordErrorHook]** - { Function(event, context, error) } - function to be called when error occurs while processing specific record; if hook is not provided, error is thrown and processing stops
- **[errorHook]** - { Function(event, context, error) } - function to be called when error occurs; if hook is not provided, error is thrown
- **[retryOptions]** - { Object } - retry configuration in case Elasticsearch indexing fails ([options description can be found here][promise-retry-url]) [is not retried by default]
- **[transformRecordHook]** - { Function(record, old) } - optional function to perform custom data processing; accepts single record and old image; record is omitted if function does not return result; useful for reshaping/excluding document before sending it to Elasticsearch

> Note: `context` object, available in hooks, includes [`bunyan` context extension provided by `alpha-lambda-bunyan`][alpha-lambda-bunyan-url]

> Note: All hooks can be asynchronous

## Example

```js
const { Client } = require('@elastic/elasticsearch');
const d2es = require('dynamo2es-lambda');

module.exports.handler = d2es({
  elasticsearch: {
    client: new Client({
      node: 'your-aws-es-host.amazonaws.com',
      // Optional, if you need to sign the request with IAM credentials
      // Connection: require('aws-elasticsearch-connector')
    }),
    bulk: {
      refresh: 'wait_for'
    }
  },
  indexField: ['storeId', 'customerId'],
  type: 'type',
  idField: 'orderId',
  versionField: '_version',
  separator: '-',
  beforeHook: (event, context) => context.log.info({ event }),
  afterHook: (event, context, result) => {
    context.log.info({ result });
    if (result.errors) {
      /* error handling logic */
    }
  },
  errorHook: (event, context, err) => context.log.error({ err }),
  recordErrorHook: (event, context, err) => context.log.error({ err }),
  transformRecordHook: (record, old, context) => {
    return {
      ...record,
      {
        fullName: `${record.firstName} ${record.lastName}`
      }
    };
  }
});
```

## Result Object

`dynamo2es-lambda` returns raw result provided by the [bulk API][bulk-api-url]:

```json
"took": 123,
"errors": false,
"items": [
  {
    "index": {
      "_index": "08c312d0-9bd0-4a43-9748-9469f78e3ea0",
      "_type": "type",
      "_id": "f2f8cef2-031d-401f-a0c5-d6ce50a0bef3",
      "_version": 0,
      "result": "created",
      "_shards": {
          "total": 2,
          "successful": 2,
          "failed": 0
      },
      "created": true,
      "status": 201
    }
  }
]
```

> Note: `errors` property is set to `true` only in case of critical errors (e.g. version conflict), but not for non-critical ones (e.g. not found).

## License

The MIT License (MIT)

Copyright (c) 2016-2019 Anton Bazhal

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

[alpha-lambda-bunyan-url-url]: https://www.npmjs.com/package/alpha-lambda-bunyan
[alpha-lambda-url]: https://www.npmjs.com/package/alpha-lambda
[elasticsearch-client-url]: https://www.npmjs.com/package/@elastic/elasticsearch
[elasticsearch-legacy-client-url]: https://www.npmjs.com/package/elasticsearch
[aws-elasticsearch-url]: https://aws.amazon.com/elasticsearch-service/
[aws-lambda-url]: https://aws.amazon.com/lambda/details/
[bulk-api-url]: https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-bulk
[ci-image]: https://circleci.com/gh/AntonBazhal/dynamo2es-lambda.svg?style=shield&circle-token=10551f1137392ea7edd52832eccf5b239f5d7535
[ci-url]: https://circleci.com/gh/AntonBazhal/dynamo2es-lambda
[coverage-image]: https://coveralls.io/repos/github/AntonBazhal/dynamo2es-lambda/badge.svg?branch=master
[coverage-url]: https://coveralls.io/github/AntonBazhal/dynamo2es-lambda?branch=master
[dependencies-url]: https://david-dm.org/antonbazhal/dynamo2es-lambda
[dependencies-image]: https://david-dm.org/antonbazhal/dynamo2es-lambda/status.svg
[devdependencies-url]: https://david-dm.org/antonbazhal/dynamo2es-lambda?type=dev
[devdependencies-image]: https://david-dm.org/antonbazhal/dynamo2es-lambda/dev-status.svg
[dynamodb-streams-url]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
[elasticsearch-versioning-url]: https://www.elastic.co/blog/elasticsearch-versioning-support
[elasticsearch-parent-child-url]: https://www.elastic.co/guide/en/elasticsearch/guide/current/parent-child.html
[npm-url]: https://www.npmjs.org/package/dynamo2es-lambda
[npm-image]: https://img.shields.io/npm/v/dynamo2es-lambda.svg
[promise-retry-url]: https://www.npmjs.com/package/promise-retry#promiseretryfn-options
