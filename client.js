const util = require('util');

const kebabCase = require('lodash.kebabcase');
const camelCase = require('lodash.camelcase');
const through2 = require('through2');
const duplexer2 = require('duplexer2');
const grpc = require('@grpc/grpc-js');
const backoff = require('backoff');

const loadObject = require('./loadObject');

module.exports = RPCBaseServiceClientFactory;

function RPCBaseServiceClientFactory(TService, transforms = [], opts = {}) {
  const defaultOpts = {
    retryFailAfter: 10,
    retryOnCodes: [],
    retryInitialDelay: 100,
    retryMaxDelay: 10000
  };

  opts = Object.assign({}, defaultOpts, opts);

  const requestTransforms = {};
  const responseTransforms = {};

  const Client = loadObject(TService);

  function RPCBaseServiceClient(addr, creds, clientOpts) {
    this._grpcClient = new Client(addr, creds, clientOpts);
  }

  RPCBaseServiceClient.prototype = {};
  RPCBaseServiceClient.prototype.constructor = RPCBaseServiceClient;

  /* eslint-disable no-loop-func */
  for (let child of TService.methodsArray) {
    let methodName = camelCase(child.name);

    requestTransforms[methodName] = requestTransforms[methodName] || [];
    responseTransforms[methodName] = responseTransforms[methodName] || [];

    child.resolve();

    // requestTransform1 -> requestTransform2 -> RPC client -> responseTransform2 -> responseTransform1
    for (let { request, response } of transforms) {
      requestTransforms[methodName].push(request(child.resolvedRequestType));
      responseTransforms[methodName].unshift(response(child.resolvedResponseType));
    }

    const attachContextToError = (err, callingContext) => {
      const stack = `${err.stack}\ncaused when calling gRPC method ${
        child.fullName
      }\n${callingContext.stack.replace(/^Error\n/, '')}`;

      return Object.assign(err, { stack });
    };

    const getCallingContext = () => {
      const callingContext = {};
      Error.captureStackTrace(callingContext, RPCBaseServiceClient.prototype[methodName]);
      return callingContext;
    };

    if (child.requestStream && child.responseStream) {
      RPCBaseServiceClient.prototype[methodName] = function(...args) {
        var call = this._grpcClient[methodName](...args);
        var inStream = through2.obj(); // Pass through stream
        var outStream = through2.obj();
        var origInStream = inStream;
        var origOutStream = outStream;

        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            var transformStream = createTransformStream(transform);
            // Pass the errors through 2
            inStream.on('error', function(err) {
              transformStream.emit('error', err);
            });

            inStream = inStream.pipe(transformStream);
          }
        });

        getResponseTransforms(methodName).forEach(function(transform) {
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
      RPCBaseServiceClient.prototype[methodName] = function(...args) {
        var inStream = through2.obj();
        var origInStream = inStream;

        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            var transformStream = createTransformStream(transform);
            // Pass the errors through 2
            inStream.on('error', function(err) {
              transformStream.emit('error', err);
            });

            inStream = inStream.pipe(transformStream);
          }
        });

        const runAsPromise = util.promisify(callback => {
          var call = this._grpcClient[methodName](...args, callback);
          inStream.pipe(call);

          inStream.on('error', function(err) {
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
        ['then', 'catch', 'finally'].forEach(function(field) {
          origInStream[field] = resultProm[field].bind(resultProm);
        });

        return origInStream;
      };
    } else if (child.responseStream) {
      RPCBaseServiceClient.prototype[methodName] = function(data, metadata, ...args) {
        metadata = metadata ? metadata.clone() : new grpc.Metadata();

        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        applyContextToMetadata(metadata, data, child.resolvedRequestType);

        var call = this._grpcClient[methodName](data, ...args);
        var outStream = through2.obj();

        call.pipe(outStream);

        call.on('error', function(err) {
          outStream.emit('error', err);
        });

        getResponseTransforms(methodName).forEach(function(transform) {
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
      RPCBaseServiceClient.prototype[methodName] = function(data, metadata, ...args) {
        metadata = metadata ? metadata.clone() : new grpc.Metadata();
        const callingContext = getCallingContext();
        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        applyContextToMetadata(metadata, data, child.resolvedRequestType);

        return new Promise((resolve, reject) => {
          const fibonacciBackoff = backoff.fibonacci({
            randomisationFactor: 0.1,
            initialDelay: opts.retryInitialDelay,
            maxDelay: opts.retryMaxDelay
          });

          const handleErr = err => {
            reject(attachContextToError(err, callingContext));
          };

          const run = () => {
            promisifiedClient
              .call(this._grpcClient, data, metadata, ...args)
              .then(function(result) {
                getResponseTransforms(methodName).forEach(function(transform) {
                  if (transform) {
                    result = transform(result);
                  }
                });

                resolve(result);
              })
              .catch(err => {
                if (err.code && opts.retryOnCodes.includes(err.code)) {
                  fibonacciBackoff.backoff(err);
                } else {
                  handleErr(err);
                }
              });
          };

          fibonacciBackoff.failAfter(opts.retryFailAfter);

          fibonacciBackoff.on('ready', function() {
            run();
          });

          fibonacciBackoff.on('fail', err => {
            handleErr(err);
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

function applyContextToMetadata(metadata, data, requestType) {
  const contextType = getRequestContextType(requestType);

  if (!contextType) {
    return;
  }

  if (
    contextType === '.ortoo.Context' ||
    (contextType === '.ortoo.CommonContext' && data && data.context && data.context.ortoo)
  ) {
    let or2Context;
    if (contextType === '.ortoo.Context') {
      or2Context = data.context || {};
    } else {
      const commonContext = data.context;
      or2Context = { userId: commonContext.userId, requestId: commonContext.requestId };
      Object.assign(or2Context, commonContext.ortoo);
    }

    or2Context.applicationId = or2Context.applicationId || 'governorhub';
    for (let key of Object.keys(or2Context)) {
      metadata.set(`x-or2-context-${kebabCase(key)}`, String(or2Context[key]));
    }
  }
}

function createTransformStream(transformer) {
  return through2.obj(function(obj, enc, callback) {
    callback(null, transformer(obj));
  });
}

function getRequestContextType(requestType) {
  const contextField = requestType.fields.context;
  if (!contextField) {
    return null;
  }

  if (!contextField.resolved) {
    contextField.resolve();
  }

  const contextType = contextField.resolvedType;
  if (!contextType) {
    return null;
  }

  return contextType.fullName;
}
