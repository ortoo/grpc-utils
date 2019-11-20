const camelCase = require('lodash.camelcase');
const { makeClientConstructor } = require('@grpc/grpc-js');

const defaultGrpcOptions = {
  convertFieldsToCamelCase: false,
  binaryAsBase64: false,
  longsAsStrings: true,
  enumsAsStrings: true,
  deprecatedArgumentOrder: false
};

function zipObject(props, values) {
  return props.reduce((acc, curr, idx) => {
    return Object.assign(acc, { [curr]: values[idx] });
  }, {});
}

function fullyQualifiedName(value) {
  if (value === null || value === undefined) {
    return '';
  }
  var name = value.name;
  var parent_fqn = fullyQualifiedName(value.parent);
  if (parent_fqn !== '') {
    name = parent_fqn + '.' + name;
  }
  return name;
}

function deserializeCls(cls, options) {
  var conversion_options = {
    defaults: true,
    bytes: options.binaryAsBase64 ? String : Buffer,
    longs: options.longsAsStrings ? String : null,
    enums: options.enumsAsStrings ? String : null,
    oneofs: true
  };
  /**
   * Deserialize a buffer to a message object
   * @param {Buffer} arg_buf The buffer to deserialize
   * @return {cls} The resulting object
   */
  return function deserialize(arg_buf) {
    return cls.toObject(cls.decode(arg_buf), conversion_options);
  };
}

function serializeCls(cls) {
  /**
   * Serialize an object to a Buffer
   * @param {Object} arg The object to serialize
   * @return {Buffer} The serialized object
   */
  return function serialize(arg) {
    var message = cls.fromObject(arg);
    return cls.encode(message).finish();
  };
}

function getProtobufServiceAttrs(service, options) {
  var prefix = '/' + fullyQualifiedName(service) + '/';
  service.resolveAll();
  return zipObject(
    service.methodsArray.map(function(method) {
      return camelCase(method.name);
    }),
    service.methodsArray.map(function(method) {
      return {
        originalName: method.name,
        path: prefix + method.name,
        requestStream: !!method.requestStream,
        responseStream: !!method.responseStream,
        requestType: method.resolvedRequestType,
        responseType: method.resolvedResponseType,
        requestSerialize: serializeCls(method.resolvedRequestType),
        requestDeserialize: deserializeCls(method.resolvedRequestType, options),
        responseSerialize: serializeCls(method.resolvedResponseType),
        responseDeserialize: deserializeCls(method.resolvedResponseType, options)
      };
    })
  );
}

function loadObject(value, options) {
  options = Object.assign({}, defaultGrpcOptions, options);

  var result = {};
  if (!value) {
    return value;
  }
  if (value.hasOwnProperty('methods')) {
    // It's a service object
    var service_attrs = getProtobufServiceAttrs(value, options);
    return makeClientConstructor(service_attrs);
  }

  if (value.hasOwnProperty('nested')) {
    // It's a namespace or root object
    if (value.nested !== null && value.nested !== undefined) {
      var values = Object.keys(value.nested).map(key => value.nested[key]);
      values.forEach(nested => {
        result[nested.name] = loadObject(nested, options);
      });
    }
    return result;
  }

  // Otherwise, it's not something we need to change
  return value;
}

module.exports = loadObject;
