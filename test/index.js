const path = require('path');
const chai = require('chai');
const sinon = require('sinon');

const grpc = require('@grpc/grpc-js');
const grpcErrors = require('grpc-errors');
const ProtoBuf = require('@ortoo/protobufjs');
const ObjectId = require('bson').ObjectId;

const grpcUtils = require('../');

chai.use(require('sinon-chai'));

const { expect } = chai;

const now = new Date();

describe('grpc-utils', function() {
  var server;
  var client;
  before(function() {
    ({ server, client } = initTest());
  });

  after(function() {
    server.forceShutdown();
  });

  describe('basic functionality', function() {
    it('should call hello', function() {
      return client.hello({ name: 'james' }).then(function(res) {
        var {
          message,
          time,
          testwrap,
          testwrap2,
          objid,
          stringmap,
          undef,
          bson,
          json,
          hybrid,
          testenum,
          unsetarr,
          unsetint,
          unsettime,
          stringobjid,
          nullwrap,
          undefinedwrap,
          enumArray,
          wrappedMap,
          oneofMap,
          oneofTimestamp,
          setGoogleStringValue,
          unsetGoogleStringValue,
          defaultGoogleStringValue,
          emptyMap,
          underscoreField,
          stringArr,
          messageArr
        } = res;
        expect(message).to.equal('hello james');
        expect(time).to.be.an.instanceOf(Date);
        expect(time.getTime()).to.equal(now.getTime());
        expect(testwrap).to.have.all.members([1, 2]);
        expect(testwrap2.getTime()).to.equal(now.getTime());
        expect(objid).to.be.an.instanceof(ObjectId);
        expect(stringmap.nowIs.getTime()).to.equal(now.getTime());
        expect(undef).to.be.undefined;
        expect(json).to.deep.equal({
          some: { field: 'val3' },
          date: '2017-01-01T00:00:00.000Z',
          objId: '510928d5014ce75842000009'
        });
        expect(bson).to.have.keys(['some', 'date', 'objId']);
        expect(bson.some).to.deep.equal({ field: 'val' });
        expect(bson.date).to.be.a('Date');
        expect(bson.date.toISOString()).to.equal('2017-01-01T00:00:00.000Z');
        expect(bson.objId).to.be.an.instanceof(ObjectId);
        expect(hybrid).to.have.keys(['some', 'date', 'objId']);
        expect(hybrid.some).to.deep.equal({ field: 'val2' });
        expect(hybrid.date).to.be.a('Date');
        expect(hybrid.date.toISOString()).to.equal('2017-01-01T00:00:00.000Z');
        expect(hybrid.objId).to.be.an.instanceof(ObjectId);
        expect(testenum).to.equal('two');
        expect(unsetint).to.equal(0);
        expect(unsettime).to.be.null;
        expect(unsetarr).to.have.lengthOf(0);
        expect(String(stringobjid)).to.equal('510928d5014ce75842000008');
        expect(nullwrap).to.be.null;
        expect(undefinedwrap).to.be.undefined;
        expect(enumArray).to.have.members(['one', 'zero', 'zero', 'one']);
        expect(wrappedMap).to.deep.equal({
          some: 'value',
          inA: 'map',
          otherwise: null
        });
        expect(oneofTimestamp).to.be.undefined;
        expect(oneofMap).to.deep.equal({
          mapping: 'is great!'
        });
        expect(setGoogleStringValue).to.equal('some string');
        expect(defaultGoogleStringValue).to.equal('');
        expect(unsetGoogleStringValue).to.be.null;
        expect(emptyMap).to.be.an('object').that.is.empty;
        expect(underscoreField).to.equal('hello');
        expect(stringArr).to.deep.equal(['some']);
        expect(messageArr).to.deep.equal([new Date('2017-01-01')]);
      });
    });
  });

  describe('context', function() {
    beforeEach(function() {
      testImpl.hello.resetHistory();
      testImpl.helloCommonContext.resetHistory();
    });

    it('should set headers for request data (default applicationId)', function() {
      return client
        .hello({ name: 'james', context: { testProperty: 'silly', numberValue: 37 } })
        .then(function() {
          expect(testImpl.hello).to.have.been.calledWith(
            sinon.match.any,
            sinon.match(({ metadata }) => {
              return (
                metadata.get('x-or2-context-test-property')[0] === 'silly' &&
                metadata.get('x-or2-context-number-value')[0] === '37' &&
                metadata.get('x-or2-context-application-id')[0] === 'governorhub'
              );
            })
          );
        });
    });

    it('should set headers for request data (provided applicationId)', function() {
      return client
        .hello({
          name: 'james',
          context: { testProperty: 'silly', numberValue: 37, applicationId: 'testapp' }
        })
        .then(function() {
          expect(testImpl.hello).to.have.been.calledWith(
            sinon.match.any,
            sinon.match(({ metadata }) => {
              return (
                metadata.get('x-or2-context-test-property')[0] === 'silly' &&
                metadata.get('x-or2-context-number-value')[0] === '37' &&
                metadata.get('x-or2-context-application-id')[0] === 'testapp'
              );
            })
          );
        });
    });

    it('should set headers for request data (commonContext)', function() {
      return client
        .helloCommonContext({
          name: 'james',
          context: {
            userId: 'test1',
            requestId: 'test2',
            ortoo: { testProperty: 'silly', numberValue: 37, applicationId: 'testapp' }
          }
        })
        .then(function() {
          expect(testImpl.helloCommonContext).to.have.been.calledWith(
            sinon.match.any,
            sinon.match(({ metadata }) => {
              return (
                metadata.get('x-or2-context-test-property')[0] === 'silly' &&
                metadata.get('x-or2-context-number-value')[0] === '37' &&
                metadata.get('x-or2-context-application-id')[0] === 'testapp' &&
                metadata.get('x-or2-context-request-id')[0] === 'test2' &&
                metadata.get('x-or2-context-user-id')[0] === 'test1'
              );
            })
          );
        });
    });

    it('should not set headers for thekey request data (commonContext)', function() {
      return client
        .helloCommonContext({
          name: 'james',
          context: {
            userId: 'test1',
            requestId: 'test2',
            thekey: {}
          }
        })
        .then(function() {
          expect(testImpl.helloCommonContext).to.have.been.calledWith(
            sinon.match.any,
            sinon.match(({ metadata }) => {
              return (
                metadata.get('x-or2-context-request-id')[0] === undefined &&
                metadata.get('x-or2-context-user-id')[0] === undefined
              );
            })
          );
        });
    });

    it('should set defaults', function() {
      return client
        .hello({ name: 'james', context: { testProperty: 'silly', numberValue: 37 } })
        .then(function() {
          expect(testImpl.hello).to.have.been.calledWith({
            context: {
              requestId: sinon.match.string,
              applicationId: 'governorhub',
              testProperty: 'silly',
              numberValue: 37
            },
            name: 'james'
          });
        });
    });

    it('should not overwrite values for legacy Context', function() {
      return client
        .hello({
          name: 'james',
          context: {
            testProperty: 'silly',
            numberValue: 37,
            applicationId: 'testapp',
            requestId: '1234'
          }
        })
        .then(function() {
          expect(testImpl.hello).to.have.been.calledWith({
            context: {
              requestId: '1234',
              applicationId: 'testapp',
              testProperty: 'silly',
              numberValue: 37
            },
            name: 'james'
          });
        });
    });

    it('should not overwrite values for CommonContext', function() {
      return client
        .helloCommonContext({
          name: 'connorchris',
          context: {
            ortoo: {
              godMode: false,
              applicationId: 'testapp',
              clientId: 'clientId123'
            },
            requestId: 'requestId1234',
            userId: 'userId37'
          }
        })
        .then(function() {
          expect(testImpl.helloCommonContext).to.have.been.calledWith({
            context: {
              requestId: 'requestId1234',
              userId: 'userId37',
              contextType: 'ortoo',
              ortoo: {
                godMode: false,
                applicationId: 'testapp',
                clientId: 'clientId123'
              },
              thekey: null
            },
            name: 'connorchris'
          });
        });
    });

    it('should check The Key with CommonContext', function() {
      return client
        .helloCommonContext({
          name: 'james',
          context: {
            thekey: {
              clientId: 'clientId123'
            },
            requestId: 'requestId1234',
            userId: 'userId37'
          }
        })
        .then(function() {
          expect(testImpl.helloCommonContext).to.have.been.calledWith({
            context: {
              requestId: 'requestId1234',
              userId: 'userId37',
              contextType: 'thekey',
              thekey: {
                clientId: 'clientId123'
              },
              ortoo: null
            },
            name: 'james'
          });
        });
    });
  });

  describe('retries', function() {
    it('should retry on unavailable', function() {
      return client.unavailable({ name: 'james' }).then(function({ message }) {
        expect(message).to.equal('hello james');
      });
    });
  });

  describe('error', function() {
    it('should throw an error with the correct stack trace', function() {
      return client.error({}).then(
        () => {
          throw new Error('should not get here');
        },
        err => {
          expect(err.stack).to.include('caused when calling gRPC method .ortoo.TestService.Error');
          expect(err.stack).to.include('test/index.js');
        }
      );
    });
  });

  describe('cancelled on context', function() {
    it('the context should have a working cancelled property', function(done) {
      const cancelStub = testImpl.cancel;
      let call;
      const rawClient = client._grpcClient;

      cancelStub.callsFake(function({ context }) {
        return new Promise((_, reject) => {
          if (context.cancelled) {
            reject(new Error());
            done(new Error('context should not be cancelled yet'));
          }

          call.cancel();

          const intervalId = setInterval(() => {
            if (context.cancelled) {
              clearInterval(intervalId);
              reject(new Error());
              done();
            }
          }, 10);
        });
      });

      call = rawClient.cancel({ name: 'james', context: {} }, () => {});
    });
  });
});

function initTest() {
  const server = new grpc.Server();

  var root = new ProtoBuf.Root();
  var ns = root.loadSync(grpcUtils.applyProtoRoot(path.join(__dirname, 'proto/test.proto'), root));
  const testService = ns.lookupService('ortoo.TestService');

  grpcUtils.applyCustomWrappers(ns);

  var finalImpl = grpcUtils.createImpl(testService, testImpl);
  finalImpl.on('callError', function() {
    // server.emit('callError', ...args);
  });

  var service = grpcUtils.loadObject(testService);
  server.addService(service.service, finalImpl);

  var cred = grpc.ServerCredentials.createInsecure();
  server.bindAsync('0.0.0.0:50001', cred, err => {
    if (err) {
      console.error(err);
      return;
    }

    server.start();
  });

  var clientCreds = grpc.credentials.createInsecure();
  var Client = grpcUtils.createClient(testService, [], { retryOnCodes: [14] });

  var client = new Client('localhost:50001', clientCreds);

  return { server, client };
}
let unavailableCount = 0;
const testImpl = {
  hello: sinon.spy(function({ name }) {
    return {
      message: 'hello ' + name,
      time: now,
      testwrap: [1, null, 2],
      testwrap2: now,
      objid: new ObjectId(),
      stringobjid: '510928d5014ce75842000008',
      stringmap: {
        nowIs: now
      },
      undef: 'field',
      bson: { some: { field: 'val' }, date: new Date('2017-01-01'), objId: new ObjectId() },
      hybrid: { some: { field: 'val2' }, date: new Date('2017-01-01'), objId: new ObjectId() },
      json: {
        some: { field: 'val3' },
        date: new Date('2017-01-01'),
        objId: new ObjectId('510928d5014ce75842000009')
      },
      testenum: 'two',
      nullwrap: null,
      enumArray: ['one', 'two', 'zero', 1],
      wrappedMap: {
        some: 'value',
        inA: 'map',
        otherwise: null
      },

      oneofMap: {
        mapping: 'is great!'
      },

      secondOneOfString: 'hi there',

      setGoogleStringValue: 'some string',
      defaultGoogleStringValue: '',
      emptyMap: {},
      underscoreField: 'hello',

      stringArr: ['some', null, null, undefined],
      messageArr: [null, undefined, new Date('2017-01-01')]
    };
  }),

  // eslint-disable-next-line no-empty-pattern
  helloCommonContext: sinon.spy(function({}) {
    return {};
  }),

  unavailable: function({ name }) {
    unavailableCount++;
    if (unavailableCount % 2) {
      throw new grpcErrors.UnavailableError('unavailable');
    } else {
      return { message: 'hello ' + name };
    }
  },

  error: function() {
    throw new Error('This is a terrible error!');
  },

  cancel: sinon.stub()
};
