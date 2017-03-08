const path = require('path');
const expect = require('chai').expect;

// const opentracing = require('opentracing');
// const {MockTracer} = require('opentracing/lib/mock_tracer');

const grpc = require('grpc');
const grpcErrors = require('grpc-errors');

const grpcUtils = require('../');

// const mockTracer = new MockTracer();
// opentracing.initGlobalTracer(mockTracer);

describe('grpc-utils', function () {
  var server;
  var client;
  before(function () {
    ({server, client} = initTest());
  });

  after(function () {
    // console.log('\nSpans:');
    // const report = mockTracer.report();
    // for (let i = 0; i < report.spans.length; i++) {
    //   const span = report.spans[i];
    //   const tags = span.tags();
    //   const tagKeys = Object.keys(tags);
    //
    //   console.log(`    ${span.operationName()} - ${span.durationMs()}ms`);
    //   for (let j = 0; j < tagKeys.length; j++) {
    //     let key = tagKeys[j];
    //     let value = tags[key];
    //     console.log(`        tag '${key}':'${value}'`);
    //   }
    // }
    server.forceShutdown();
  });

  it('should call hello', function () {
    return client.hello({name: 'james'}).then(function ({message}) {
      expect(message).to.equal('hello james');
    });
  });

  it('should retry on unavailable', function() {
    return client.unavailable({name: 'james'}).then(function ({message}) {
      expect(message).to.equal('hello james');
    });
  });
});

function initTest() {
  const server = new grpc.Server();

  var serverTransforms = {
    request: grpcUtils.createObjectDeserializer,
    response: function (TObj) {
      return grpcUtils.createObjectSerializer(TObj);
    }
  };

  var clientTransforms = {
    request: function (TObj) {
      return grpcUtils.createObjectSerializer(TObj);
    },
    response: grpcUtils.createObjectDeserializer
  };

  var compiled = grpc.load(path.join(__dirname, 'proto/test.proto'));

  var service = compiled.test.TestService;

  var finalImpl = grpcUtils.createImpl(service, testImpl, [serverTransforms]);
  finalImpl.on('callError', function (...args) {
    // server.emit('callError', ...args);
  });

  server.addProtoService(service.service, finalImpl);

  var cred = grpc.ServerCredentials.createInsecure();
  server.bind('0.0.0.0:50001', cred);
  server.start();

  var clientCreds = grpc.credentials.createInsecure();
  var Client = grpcUtils.createClient(service, [clientTransforms]);

  var client = new Client('localhost:50001', clientCreds);

  return {server, client};
}

let unavailableCount = 0;
const testImpl = {
  hello: function({name}) {
    return {message: 'hello ' + name};
  },

  unavailable: function({name}) {
    unavailableCount++;
    if (unavailableCount % 2) {
      throw new grpcErrors.UnavailableError('unavailable');
    } else {
      return {message: 'hello ' + name};
    }

  }
};
