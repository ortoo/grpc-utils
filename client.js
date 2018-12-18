const util = require('util');

const lowerFirst = require('lodash.lowerfirst');
const through2 = require('through2');
const duplexer2 = require('duplexer2');
const grpc = require('grpc');
const backoff = require('backoff');

module.exports = RPCBaseServiceClientFactory;

function RPCBaseServiceClientFactory(TService, transforms=[], opts={}) {
  const defaultOpts = {
    retryFailAfter: 10,
    retryOnCodes: [],
    retryInitialDelay: 100,
    retryMaxDelay: 10000
  };

  opts = Object.assign({}, defaultOpts, opts);

  const requestTransforms = {};
  const responseTransforms = {};

  const Client = grpc.loadObject(TService);

  function RPCBaseServiceClient(addr, creds, clientOpts) {
    this._grpcClient = new Client(addr, creds, clientOpts);
  }

  RPCBaseServiceClient.prototype = {};
  RPCBaseServiceClient.prototype.constructor = RPCBaseServiceClient;

  /* eslint-disable no-loop-func */
  for (let child of TService.methodsArray) {
    let methodName = lowerFirst(child.name);


    requestTransforms[methodName] = requestTransforms[methodName] || [];
    responseTransforms[methodName] = responseTransforms[methodName] || [];

    child.resolve();

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

        const runAsPromise = util.promisify((callback) => {
          var call = this._grpcClient[methodName](...args, callback);
          inStream.pipe(call);

          inStream.on('error', function (err) {
            call.emit('error', err);
          });
        });

        const resultProm = runAsPromise().then(function(result) {
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
      RPCBaseServiceClient.prototype[methodName] = function (data, metadata, ...args) {
        metadata = metadata ? metadata.clone() : new grpc.Metadata();

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
      let promisifiedClient = util.promisify(Client.prototype[methodName]);
      RPCBaseServiceClient.prototype[methodName] = function (data, metadata, ...args) {
        metadata = metadata ? metadata.clone() : new grpc.Metadata();

        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        return new Promise((resolve, reject) => {
          const fibonacciBackoff = backoff.fibonacci({
            randomisationFactor: 0.1,
            initialDelay: opts.retryInitialDelay,
            maxDelay: opts.retryMaxDelay
          });

          const run = () => {
            promisifiedClient.call(this._grpcClient, data, metadata, ...args).then(function (result) {
              getResponseTransforms(methodName).forEach(function(transform) {
                if (transform) {
                  result = transform(result);
                }
              });

              resolve(result);
            }).catch(err => {
              if (err.code && opts.retryOnCodes.includes(err.code)) {
                fibonacciBackoff.backoff(err);
              } else {
                reject(err);
              }
            });
          };

          fibonacciBackoff.failAfter(opts.retryFailAfter);

          fibonacciBackoff.on('ready', function() {
            run();
          });

          fibonacciBackoff.on('fail', err => {
            reject(err);
          });

          run();
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
