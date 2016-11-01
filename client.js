const Promise = require('bluebird');
const lowerFirst = require('lodash.lowerfirst');
const Backoff = require('backo');
const through2 = require('through2');
const duplexer2 = require('duplexer2');

module.exports = RPCRetryServiceClientFactory;

function RPCRetryServiceClientFactory(TService, transforms) {
  const RPCBaseServiceClient = RPCBaseServiceClientFactory(TService, transforms);

  function RPCRetryServiceClient(addr, creds, opts) {
    RPCBaseServiceClient.call(this, addr, creds);

    opts = opts || {};
    this._maxAttempts = opts.maxAttempts || 10;
    this._minMs = opts.min || 100;
    this._maxMs = opts.max || 10000;
  }

  RPCRetryServiceClient.prototype = Object.create(RPCBaseServiceClient.prototype);
  RPCRetryServiceClient.prototype.constructor = RPCRetryServiceClient;

  /* eslint-disable no-loop-func */
  for (let child of TService.service.children) {
    if (child.className !== 'Service.RPCMethod') {
      continue;
    }

    let methodName = lowerFirst(child.name);

    // Don't retry on streams - they should just send errors down the stream
    if (!(child.responseStream || child.requestStream)) {
      RPCRetryServiceClient.prototype[methodName] = function (...args) {
        var backoff = new Backoff({min: this._minMs, max: this._maxMs});

        var attempt = () => {
          var _super = Object.getPrototypeOf(RPCRetryServiceClient.prototype);
          return _super[methodName].apply(this, args).catch(err => {
            if (backoff.attempts < this._maxAttempts && err.grpc_status === 14) {
              return Promise.delay(backoff.duration()).then(attempt);
            } else {
              throw err;
            }
          });
        };

        return attempt();
      };
    }
  }

  return RPCRetryServiceClient;
}

function RPCBaseServiceClientFactory(TService, transforms) {

  const requestTransforms = {};
  const responseTransforms = {};

  function RPCBaseServiceClient(addr, creds) {
    this._grpcClient = new TService(addr, creds);
  }

  RPCBaseServiceClient.prototype = {};
  RPCBaseServiceClient.prototype.constructor = RPCBaseServiceClient;

  /* eslint-disable no-loop-func */
  for (let child of TService.service.children) {
    if (child.className !== 'Service.RPCMethod') {
      continue;
    }

    let methodName = lowerFirst(child.name);


    requestTransforms[methodName] = requestTransforms[methodName] || [];
    responseTransforms[methodName] = responseTransforms[methodName] || [];

    // requestTransform1 -> requestTransform2 -> RPC client -> responseTransform2 -> responseTransform1
    for (let {request, response} of transforms) {
      requestTransforms[methodName].push(request(child.resolvedRequestType));
      responseTransforms[methodName].unshift(response(child.resolvedResponseType));
    }

    if (child.requestStream && child.responseStream) {
      RPCBaseServiceClient.prototype[methodName] = function (...args) {
        var call = this._grpcClient[methodName](...args);
        var inStream = through2.obj(); // Pass through stream
        var outStream = through2.obj();
        var origInStream = inStream;
        var origOutStream = outStream;

        getRequestTransforms(methodName).forEach(function (transform) {
          if (transform) {
            var transformStream = createTransformStream(transform);
            // Pass the errors through 2
            inStream.on('error', function(err) {
              transformStream.emit('error', err);
            });

            inStream = inStream.pipe(transformStream);
          }
        });

        getResponseTransforms(methodName).forEach(function (transform) {
          if (transform) {
            var transformStream = createTransformStream(transform);
            outStream.on('error', function(err) {
              transformStream.emit('error', err);
            });
            outStream = outStream.pipe(transformStream);
          }
        });

        // Map inStream errors to outStream
        inStream.on('error', function(err) {
          origOutStream.emit('error', err);
        });

        inStream.pipe(call);
        call.pipe(origOutStream);

        call.on('error', function(err) {
          origOutStream.emit('error', err);
        });

        return duplexer2(origInStream, outStream);
      };
    } else if (child.requestStream) {
      RPCBaseServiceClient.prototype[methodName] = function (...args) {
        var inStream = through2.obj();
        var origInStream = inStream;

        getRequestTransforms(methodName).forEach(function (transform) {
          if (transform) {
            var transformStream = createTransformStream(transform);
            // Pass the errors through 2
            inStream.on('error', function(err) {
              transformStream.emit('error', err);
            });

            inStream = inStream.pipe(transformStream);
          }
        });

        var resultProm = Promise.fromCallback(function (callback) {
          var call = this._grpcClient[methodName](...args, callback);
          inStream.pipe(call);

          inStream.on('error', function (err) {
            call.emit('error', err);
          });
        }).then(function(result) {
          getResponseTransforms(methodName).forEach(function(transform) {
            if (transform) {
              result = transform(result);
            }
          });

          return result;
        });

        // Make instream thenable
        ['then', 'catch', 'finally'].forEach(function (field) {
          origInStream[field] = resultProm[field].bind(resultProm);
        });

        return origInStream;
      };
    } else if (child.responseStream) {
      RPCBaseServiceClient.prototype[methodName] = function (data, ...args) {
        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        var call = this._grpcClient[methodName](data, ...args);
        var outStream = through2.obj();

        call.pipe(outStream);

        call.on('error', function(err) {
          outStream.emit('error', err);
        });

        getResponseTransforms(methodName).forEach(function (transform) {
          if (transform) {
            var transformStream = createTransformStream(transform);
            outStream.on('error', function(err) {
              transformStream.emit('error', err);
            });
            outStream = outStream.pipe(transformStream);
          }
        });

        return outStream;
      };
    } else {
      // No streams
      let promisifiedClient = Promise.promisify(TService.prototype[methodName]);
      RPCBaseServiceClient.prototype[methodName] = function (data, ...args) {
        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        return promisifiedClient.call(this._grpcClient, data, ...args).then(function (result) {
          getResponseTransforms(methodName).forEach(function(transform) {
            if (transform) {
              result = transform(result);
            }
          });

          return result;
        });
      };
    }
  }

  return RPCBaseServiceClient;

  function getRequestTransforms(method) {
    return requestTransforms[method] || [];
  }

  function getResponseTransforms(method) {
    return responseTransforms[method] || [];
  }
}

function createTransformStream(transformer) {
  return through2.obj(function(obj, enc, callback) {
    callback(null, transformer(obj));
  });
}
