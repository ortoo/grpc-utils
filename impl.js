const EventEmitter = require('events');
const crypto = require('crypto');

const duplexer2 = require('duplexer2');
const camelCase = require('lodash.camelcase');
const cloneDeep = require('lodash.clonedeep');
const through2 = require('through2');

module.exports = RPCServiceImplementation;

function RPCServiceImplementation(service, impl, transforms = []) {
  const requestTransforms = {};
  const responseTransforms = {};

  var wrappedImpl = new EventEmitter();
  for (let child of service.methodsArray) {
    let methodName = camelCase(child.name);

    // Is this implemented. If not then ignore
    if (!impl[methodName]) {
      continue;
    }

    child.resolve();

    requestTransforms[methodName] = requestTransforms[methodName] || [];
    responseTransforms[methodName] = responseTransforms[methodName] || [];

    // requestTransform1 -> requestTransform2 -> impl -> responseTransform2 -> responseTransform1
    for (let { request, response } of transforms) {
      requestTransforms[methodName].push(request(child.resolvedRequestType));
      responseTransforms[methodName].unshift(response(child.resolvedResponseType));
    }

    if (child.requestStream && child.responseStream) {
      wrappedImpl[methodName] = function(call) {
        var inStream = through2.obj(); // Pass through stream

        call.pipe(inStream);
        call.on('error', function(err) {
          inStream.emit('error', err);
        });

        var outStream = through2.obj();
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
        outStream.on('error', handleErr);

        inStream.on('error', function(err) {
          origOutStream.emit('error', err);
        });

        outStream.pipe(call);

        var duplex = duplexer2(origOutStream, inStream);

        try {
          impl[methodName](duplex, call);
        } catch (err) {
          handleErr(err);
        }

        function handleErr(err) {
          wrappedImpl.emit('callError', err, call, { service: service, methodName });
          call.emit('error', err);
        }
      };
    } else if (child.requestStream) {
      wrappedImpl[methodName] = function(call, callback) {
        var inStream = through2.obj();

        call.on('error', function(err) {
          inStream.emit('error', err);
        });

        call.pipe(inStream);

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

        const prom = new Promise((resolve, reject) => {
          try {
            resolve(impl[methodName](inStream, call));
          } catch (err) {
            reject(err);
          }
        });

        prom
          .then(function(result) {
            getResponseTransforms(methodName).forEach(function(transform) {
              if (transform) {
                result = transform(result);
              }
            });

            callback(null, result);
          })
          .catch(function(err) {
            wrappedImpl.emit('callError', err, call, { service: service, methodName });

            try {
              callback(err);
            } catch (sendErr) {
              console.error('Send error: ', sendErr);
            }
          });
      };
    } else if (child.responseStream) {
      wrappedImpl[methodName] = function(call) {
        var outStream = through2.obj();
        var origOutStream = outStream;

        getResponseTransforms(methodName).forEach(function(transform) {
          if (transform) {
            var transformStream = createTransformStream(transform);
            outStream.on('error', function(err) {
              transformStream.emit('error', err);
            });
            outStream = outStream.pipe(transformStream);
          }
        });

        outStream.pipe(call);
        outStream.on('error', handleErr);

        var data = call.request;

        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        applyContextDefaults(data, call, child.resolvedRequestType);

        try {
          impl[methodName](origOutStream, data, call);
        } catch (err) {
          handleErr(err);
        }

        function handleErr(err) {
          wrappedImpl.emit('callError', err, call, { service: service, methodName });
          call.emit('error', err);
        }
      };
    } else {
      // No streams
      wrappedImpl[methodName] = function(call, callback) {
        var data = Object.assign({}, call.request);

        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        applyContextDefaults(data, call, child.resolvedRequestType);

        const prom = new Promise((resolve, reject) => {
          try {
            resolve(impl[methodName](data, call));
          } catch (err) {
            reject(err);
          }
        });

        prom
          .then(function(result) {
            getResponseTransforms(methodName).forEach(function(transform) {
              if (transform) {
                result = transform(result);
              }
            });

            callback(null, result);
          })
          .catch(function(err) {
            wrappedImpl.emit('callError', err, call, { service: service, methodName });

            try {
              callback(err);
            } catch (sendErr) {
              console.error('Send error: ', sendErr);
            }
          });
      };
    }
  }

  function getRequestTransforms(method) {
    return requestTransforms[method] || [];
  }

  function getResponseTransforms(method) {
    return responseTransforms[method] || [];
  }

  return wrappedImpl;
}

function createTransformStream(transformer) {
  return through2.obj(function(obj, enc, callback) {
    callback(null, transformer(obj));
  });
}

function generateRequestId() {
  return crypto.randomBytes(7).toString('base64');
}

function applyContextDefaults(data, call, requestType) {
  const contextType = getRequestContextType(requestType);

  if (!contextType) {
    return;
  }

  // Add in data to the context
  const context = data.context ? cloneDeep(data.context) : {};

  // Maybe set a requestId if we don't have one
  if (!context.requestId) {
    context.requestId = generateRequestId();
  }

  if (contextType === '.ortoo.Context') {
    // Default to the "governorhub" application
    if (!context.applicationId) {
      context.applicationId = 'governorhub';
    }
  } else if (contextType === '.ortoo.CommonContext' && context.ortoo) {
    if (!context.ortoo.applicationId) {
      context.ortoo.applicationId = 'governorhub';
    }
  }

  // Proxy through the cancelled property on the call
  if (call) {
    Object.defineProperty(context, 'cancelled', {
      enumerable: false,
      get: function() {
        return call.cancelled;
      }
    });
  }

  data.context = context;
}

function getRequestContextType(requestType) {
  const contextField = requestType.fields.context;
  if (!contextField) {
    return null;
  }

  const contextType = contextField.resolvedType;
  if (!contextType) {
    return null;
  }

  return contextType.fullName;
}
