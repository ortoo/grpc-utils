const duplexer2 = require('duplexer2');
const lowerFirst = require('lodash.lowerfirst');
const through2 = require('through2');

module.exports = RPCServiceImplementation;

function RPCServiceImplementation(TService, impl, transforms) {
  const requestTransforms = {};
  const responseTransforms = {};

  var wrappedImpl = {};

  for (let child of TService.service.children) {
    if (child.className !== 'Service.RPCMethod') {
      continue;
    }

    let methodName = lowerFirst(child.name);

    // Is this implemented. If not then ignore
    if (!impl[methodName]) {
      continue;
    }

    requestTransforms[methodName] = requestTransforms[methodName] || [];
    responseTransforms[methodName] = responseTransforms[methodName] || [];

    // requestTransform1 -> requestTransform2 -> impl -> responseTransform2 -> responseTransform1
    for (let {request, response} of transforms) {
      requestTransforms[methodName].push(request(child.resolvedRequestType));
      responseTransforms[methodName].unshift(response(child.resolvedResponseType));
    }

    if (child.requestStream && child.responseStream) {
      wrappedImpl[methodName] = function (call) {
        var inStream = through2.obj(); // Pass through stream

        call.pipe(inStream);
        call.on('error', function (err) {
          inStream.emit('error', err);
        });

        var outStream = through2.obj();
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
        outStream.on('error', function (err) {
          call.emit('error', err);
        });

        inStream.on('error', function (err) {
          origOutStream.emit('error', err);
        });

        outStream.pipe(call);

        var duplex = duplexer2(origOutStream, inStream);
        impl[methodName](duplex, call);
      };
    } else if (child.requestStream) {
      wrappedImpl[methodName] = function (call, callback) {

        var inStream = through2.obj();

        call.on('error', function (err) {
          inStream.emit('error', err);
        });

        call.pipe(inStream);

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

        var prom = impl[methodName](inStream, call);

        prom.then(function (result) {
          getResponseTransforms(methodName).forEach(function(transform) {
            if (transform) {
              result = transform(result);
            }
          });

          callback(null, result);
        }).catch(function (err) {
          callback(err);
        });
      };
    } else if (child.responseStream) {
      wrappedImpl[methodName] = function (call) {
        var outStream = through2.obj();
        var origOutStream = outStream;

        getResponseTransforms(methodName).forEach(function (transform) {
          if (transform) {
            var transformStream = createTransformStream(transform);
            outStream.on('error', function(err) {
              transformStream.emit('error', err);
            });
            outStream = outStream.pipe(transformStream);
          }
        });

        outStream.pipe(call);
        outStream.on('error', function (err) {
          call.emit('error', err);
        });

        var data = call.request;

        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        impl[methodName](origOutStream, data, call);
      };
    } else {
      // No streams
      wrappedImpl[methodName] = function (call, callback) {
        var data = call.request;

        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        impl[methodName](data, call).then(function (result) {
          getResponseTransforms(methodName).forEach(function(transform) {
            if (transform) {
              result = transform(result);
            }
          });

          callback(null, result);
        }).catch(function (err) {
          console.error(err);
          callback(err);
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
