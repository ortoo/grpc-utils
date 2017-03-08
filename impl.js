const EventEmitter = require('events');

const Promise = require('bluebird');
const duplexer2 = require('duplexer2');
const lowerFirst = require('lodash.lowerfirst');
const through2 = require('through2');

var opentracing;
var tracer;
try {
  opentracing = require('opentracing');
  tracer = opentracing.globalTracer();
} catch (err) {
  if (err.code !== 'MODULE_NOT_FOUND') {
    throw err;
  }
  opentracing = null;
  tracer = null;
}

module.exports = RPCServiceImplementation;

function RPCServiceImplementation(TService, impl, transforms) {
  const requestTransforms = {};
  const responseTransforms = {};

  var wrappedImpl = new EventEmitter();

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
        outStream.on('error', handleErr);

        inStream.on('error', function (err) {
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
          wrappedImpl.emit('callError', err, call, {service: TService.service, methodName});
          call.emit('error', err);
        }

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

        var prom = Promise.try(impl[methodName].bind(impl, inStream, call));

        prom.then(function (result) {
          getResponseTransforms(methodName).forEach(function(transform) {
            if (transform) {
              result = transform(result);
            }
          });

          callback(null, result);
        }).catch(function (err) {
          wrappedImpl.emit('callError', err, call, {service: TService.service, methodName});
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
        outStream.on('error', handleErr);

        var data = call.request;

        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        try {
          impl[methodName](origOutStream, data, call);
        } catch (err) {
          handleErr(err);
        }

        function handleErr(err) {
          wrappedImpl.emit('callError', err, call, {service: TService.service, methodName});
          call.emit('error', err);
        }
      };
    } else {
      // No streams
      wrappedImpl[methodName] = function (call, callback) {
        var span;
        if (tracer) {
          // Can we get a span from the metadata
          let md = call.metadata.getMap();
          let parentSpanContext = tracer.extract(opentracing.FORMAT_HTTP_HEADERS, md);

          if (parentSpanContext) {
            span = tracer.startSpan(methodName, {childOf: parentSpanContext});
          } else {
            span = tracer.startSpan(methodName);
          }

          span.setTag('component', 'grpc');
          span.setTag('span.kind', 'server');
          span.setTag('grpc.method_name', methodName);
          // span.setTag('grpc.call_attributes', );
          // span.setTag('grpc.headers');
        }

        var data = Object.assign({}, call.request);

        getRequestTransforms(methodName).forEach(function(transform) {
          if (transform) {
            data = transform(data);
          }
        });

        // Add in data to the context
        var context = Object.assign({}, data.context || {});

        if (span) {
          // Non-enumarable so that we don't pick it up, say with JSON.stringify
          Object.defineProperty(context, 'opentracing', {
            value: {
              spanContext: span.context()
            },
            enumerable: false
          });
        }

        data.context = context;

        Promise.try(impl[methodName].bind(undefined, data, call)).then(function (result) {
          getResponseTransforms(methodName).forEach(function(transform) {
            if (transform) {
              result = transform(result);
            }
          });

          if (span) {
            span.finish();
          }
          callback(null, result);
        }).catch(function (err) {
          wrappedImpl.emit('callError', err, call, {service: TService.service, methodName});

          if (span) {
            span.log({
              event: 'error',
              'error.object': err
            });
            span.setTag('error', true);
            span.finish();
          }
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
