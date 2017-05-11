const path = require('path');
const expect = require('chai').expect;

// const opentracing = require('opentracing');
// const {MockTracer} = require('opentracing/lib/mock_tracer');

const grpc = require('grpc');
const grpcErrors = require('grpc-errors');
const ProtoBuf = require('protobufjs');
const ObjectId = require('bson-objectid');

const grpcUtils = require('../');

const now = new Date();

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

  describe('basic functionality', function () {
    it('should call hello', function () {
      return client.hello({name: 'james'}).then(function (res) {
        var {
          message,
          time,
          testwrap,
          testwrap2,
          objid,
          stringmap,
          undef,
          json,
          testenum,
          unsetarr,
          unsetint,
          unsettime
        } = res;
        expect(message).to.equal('hello james');
        expect(time).to.be.a.Date;
        expect(time.getTime()).to.equal(now.getTime());
        expect(testwrap).to.have.all.members([1, 2]);
        expect(testwrap2.getTime()).to.equal(now.getTime());
        expect(objid).to.be.an.instanceof(ObjectId);
        expect(stringmap.nowIs.getTime()).to.equal(now.getTime());
        expect(undef).to.be.undefined;
        expect(json).to.deep.equal({some: {field: 'val'}});
        expect(testenum).to.equal('two');
        expect(unsetint).to.equal(0);
        expect(unsettime).to.be.null;
        expect(unsetarr).to.have.lengthOf(0);
      });
    });
  });

  describe('retries', function () {
    it('should retry on unavailable', function() {
      return client.unavailable({name: 'james'}).then(function ({message}) {
        expect(message).to.equal('hello james');
      });
    });
  });
});

function initTest() {
  const server = new grpc.Server();

  var root = new ProtoBuf.Root();
  var ns = root.loadSync(grpcUtils.applyProtoRoot(path.join(__dirname, 'proto/test.proto')));
  const testService = ns.lookupService('test.TestService');

  grpcUtils.applyCustomWrappers(ns);

  var finalImpl = grpcUtils.createImpl(testService, testImpl);
  finalImpl.on('callError', function (err) {
    // server.emit('callError', ...args);
  });

  var service = grpc.loadObject(testService);
  server.addService(service.service, finalImpl);

  var cred = grpc.ServerCredentials.createInsecure();
  server.bind('0.0.0.0:50001', cred);
  server.start();

  var clientCreds = grpc.credentials.createInsecure();
  var Client = grpcUtils.createClient(testService);

  var client = new Client('localhost:50001', clientCreds);

  return {server, client};
}
let unavailableCount = 0;
const testImpl = {
  hello: function({name}) {
    return {
      message: 'hello ' + name,
      time: now,
      testwrap: [1, 2],
      testwrap2: now,
      objid: new ObjectId(),
      stringmap: {
        nowIs: now
      },
      undef: 'field',
      json: {some: {field: 'val'}},
      testenum: 'two'
    };
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
