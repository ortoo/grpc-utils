'use strict';

const ObjectId = require('bson-objectid');
const isObject = require('lodash.isobject');
const isUndefined = require('lodash.isundefined');
const isString = require('lodash.isstring');
const through2 = require('through2');
const debug = require('debug')('@ortoo/grpc-utils:index');

const client = require('./client');
const impl = require('./impl');

exports.objSerializeStream = objSerializeStream;
exports.objDeserializeStream = objDeserializeStream;
exports.createObjectSerializer = createObjectSerializer;
exports.createObjectDeserializer = createObjectDeserializer;
exports.createClient = client;
exports.createImpl = impl;

const WRAPPER_RE = /\.wrappers\.(arrays|values)\.\w+$/;

function objSerializeStream(serializer) {
  return through2.obj(function(obj, enc, callback) {
    callback(null, serializer(obj));
  });
}

function objDeserializeStream(deserializer) {
  return through2.obj(function(obj, enc, callback) {
    callback(null, deserializer(obj));
  });
}

function createObjectSerializer(TObj, removeNonExisting) {

  var {timestampPaths,
       objectIdPaths,
       jsonPaths,
       wrapperPaths,
       enumPaths,
       allPaths} = generateConversionPaths(TObj);

  return serializeObject;

  function serializeObject(obj, prefix) {
    if (!prefix) {
      debug('serializing', obj);
    }

    prefix = prefix || '';

    var outObj;
    var isArray = false;
    if (Array.isArray(obj)) {
      outObj = [];
      isArray = true;
    } else if (isObject(obj)) {
      outObj = {};
    } else {
      return obj; // A primitive. Just return it
    }

    for (let key of Object.keys(obj)) {
      let val = obj[key];
      let res = val;

      let path = isArray ? prefix.slice(0, -1) : prefix + key; // remove the last .

      let isWrapper = wrapperPaths.has(path);

      if (Array.isArray(val) && !jsonPaths.has(path)) {
        path += '[]';
      }

      // Ignore anything thats not in a given path
      if (removeNonExisting && !allPaths.has(path)) {
        debug('Ignoring', path, allPaths);
        continue;
      }

      // Ignore null paths
      if (key.startsWith('__') || key.startsWith('$') || val === null) {
        continue;
      }

      debug('Processing path', path);

      try {
        // Is the key in our timestamp paths
        if (jsonPaths.has(path)) {
          res = convertToJSONObject(val);
        } else if (Array.isArray(val)) {
          res = serializeObject(val, `${path}.`);
        } else if (timestampPaths.has(path)) {
          res = convertDateToTimestamp(val);
        } else if (objectIdPaths.has(path)) {
          res = convertFromObjectId(val);
        } else if (enumPaths.has(path)) {
          res = convertStringToEnum(val, path);
        } else if (isObject(val)) {
          res = serializeObject(val, `${path}.`);
        }
      } catch (err) {
        debug('Error converting path', path, val);
        throw err;
      }

      if (!isUndefined(res)) {
        outObj[key] = isWrapper ? convertToWrapper(res) : res;
      }
    }

    if (!prefix) {
      debug('Serialized', outObj);
    }
    return outObj;
  }

  function convertStringToEnum(val, path) {

    if (!isString(val)) {
      return val;
    }

    var TEnum = enumPaths.get(path);
    var child = TEnum.children.find(child => child.name === val);

    if (!child) {
      throw new Error(`Not a valid enum value: ${val}`);
    }

    return child.id;
  }

}

function createObjectDeserializer(TObj) {

  var {timestampPaths,
       objectIdPaths,
       wrapperPaths,
       messagePaths,
       enumPaths,
       jsonPaths} = generateConversionPaths(TObj);

  return deserializeObject;

  function deserializeObject(obj, prefix) {
    if (!prefix) {
      debug('deserializing', obj);
    }

    prefix = prefix || '';

    var outObj;
    var isArray = false;
    if (Array.isArray(obj)) {
      outObj = [];
      isArray = true;
    } else {
      outObj = {};
    }

    for (let key of Object.keys(obj)) {
      let val = obj[key];
      let path = isArray ? prefix.slice(0, -1) : prefix + key; // remove the last .

      if (wrapperPaths.has(path)) {
        val = convertFromWrapper(val);
      }

      if (Array.isArray(val)) {
        path += '[]';
      }

      let res = val;

      debug('Processing path', path);

      // Is the key in our timestamp paths
      if (jsonPaths.has(path)) {
        res = convertFromJSONObject(val);
      } else if (Array.isArray(val)) {
        res = deserializeObject(val, `${path}.`);
      } else if (timestampPaths.has(path)) {
        res = convertTimestampToDate(val);
      } else if (objectIdPaths.has(path)) {
        res = convertToObjectId(val);
      } else if (enumPaths.has(path)) {
        res = convertEnumToString(val, path);
      } else if (isObject(val)) {
        res = deserializeObject(val, `${path}.`);
      } else if (val === null && messagePaths.has(path)) {
        res = undefined;
      }

      if (!isUndefined(res)) {
        outObj[key] = res;
      }
    }

    if (!prefix) {
      debug('Deserialized', outObj);
    }

    return outObj;
  }

  function convertEnumToString(val, path) {
    if (isString(val)) {
      return val;
    }

    var TEnum = enumPaths.get(path);
    var child = TEnum.children.find(child => child.id === val);

    if (!child) {
      throw new Error(`Not a valid enum value: ${val}`);
    }

    return child.name;
  }
}

function generateConversionPaths(TMessage, opts, prefix) {
  prefix = prefix || '';

  if (!opts) {
    opts = {
      objectIdPaths: new Set(),
      timestampPaths: new Set(),
      jsonPaths: new Set(),
      allPaths: new Set(),
      wrapperPaths: new Set(),
      messagePaths: new Set(),
      enumPaths: new Map()
    };
  }

  var {objectIdPaths, timestampPaths, jsonPaths, allPaths, wrapperPaths, messagePaths, enumPaths} = opts;

  for (let field of TMessage.children) {

    if (field.className !== 'Message.Field') {
      continue;
    }

    var pathrep = prefix + field.name;

    // First check if we are a wrapper path. If so mark it and resolve down
    if (field.type.name === 'message' && WRAPPER_RE.test(field.resolvedType.fqn())) {
      wrapperPaths.add(pathrep);

      field = field.resolvedType.getChild('value');
    }

    var suffix = field.repeated ? '[]' : '';

    pathrep = pathrep + suffix;
    allPaths.add(pathrep);

    if (field.type.name === 'message') {
      messagePaths.add(pathrep);

      var fqn = field.resolvedType.fqn();
      if (fqn === '.ortoo.ObjectId') {
        objectIdPaths.add(pathrep);
      } else if (fqn === '.ortoo.JSONObject') {
        jsonPaths.add(pathrep);
      } else if (fqn === '.google.protobuf.Timestamp') {
        timestampPaths.add(pathrep);
      } else {
        generateConversionPaths(field.resolvedType, opts, pathrep + '.');
      }
    } else if (field.type.name === 'enum') {
      enumPaths.set(pathrep, field.resolvedType);
    }
  }

  return opts;
}

function convertToObjectId(msg) {
  return (msg && msg.value && msg.value.length) ? new ObjectId(msg.value) : undefined;
}

function convertTimestampToDate(val) {

  if (!val) {
    return;
  }

  var millis = val.seconds * 1000 + Math.round(val.nanos / 1e6);
  return new Date(millis);
}

function convertDateToTimestamp(val) {
  if (val) {

    if (isString(val)) {
      val = new Date(val);
    }

    return {
      seconds: Math.floor(val.getTime() / 1000),
      nanos: val.getUTCMilliseconds() * 1e6
    };
  }
}

function convertFromObjectId(val) {
  if (val) {
    var strRep = val.toString ? val.toString() : String(val);
    return {value: new Buffer(strRep, 'hex')};
  }
}

function convertToJSONObject(obj) {
  return {
    representation: JSON.stringify(obj)
  };
}

function convertFromJSONObject(obj) {
  try {
    return JSON.parse(obj.representation);
  } catch (err) {
    // ignore
  }
}

function convertToWrapper(val) {
  if (!isUndefined(val)) {
    return {value: val, isNull: val === null};
  }
}

function convertFromWrapper(obj) {
  if (obj) {
    return obj.isNull ? null : obj.value;
  }
}
