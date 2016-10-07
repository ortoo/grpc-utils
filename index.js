'use strict';

const BSON = require('bson');
const isObject = require('lodash.isobject');
const through2 = require('through2');

const ObjectId = BSON.ObjectId;

exports.objSerializeStream = objSerializeStream;
exports.objDeserializeStream = objDeserializeStream;
exports.createObjectSerializer = createObjectSerializer;
exports.createObjectDeserializer = createObjectDeserializer;

function objSerializeStream(serializer) {
  return through2.obj(function(obj, enc, callback) {
    callback(null, serializer(obj));
  });
}

function createObjectSerializer(TObj) {

  var {timestampPaths, objectIdPaths, jsonPaths} = generateConversionPaths(TObj);

  return serializeObject;

  function serializeObject(obj, prefix) {

    prefix = prefix || '';


    var outObj;
    var isArray = false;
    if (Array.isArray(obj)) {
      outObj = [];
      isArray = true;
    } else {
      outObj = {};
    }
    for (let key in obj) {
      let val = obj[key];
      let res = val;

      // Ignore __ or null paths
      if (key.startsWith('__') || val === null) {
        continue;
      }

      let path = isArray ? prefix.slice(0, -1) : prefix + key; // remove the last .

      // Is the key in our timestamp paths
      if (timestampPaths.has(path)) {
        res = convertDateToTimestamp(val);
      } else if (objectIdPaths.has(path)) {
        res = convertFromObjectId(val);
      } else if (jsonPaths.has(path)) {
        res = convertToJSONObject(val);
      } else if (Array.isArray(val)) {
        res = serializeObject(val, `${prefix}${key}[].`);
      } else if (isObject(val)) {
        res = serializeObject(val, `${path}.`);
      }

      outObj[key] = res;
    }

    return outObj;
  }

}

function createObjectDeserializer(TObj) {

  var {timestampPaths, objectIdPaths, jsonPaths} = generateConversionPaths(TObj);

  return deserializeObject;

  function deserializeObject(obj, prefix) {

    prefix = prefix || '';


    var outObj;
    var isArray = false;
    if (Array.isArray(obj)) {
      outObj = [];
      isArray = true;
    } else {
      outObj = {};
    }

    for (let key in obj) {
      let val = obj[key];
      let res = val;

      let path = isArray ? prefix.slice(0, -1) : prefix + key; // remove the last .

      // Is the key in our timestamp paths
      if (timestampPaths.has(path)) {
        res = convertTimestampToDate(val);
      } else if (objectIdPaths.has(path)) {
        res = convertToObjectId(val);
      } else if (jsonPaths.has(path)) {
        res = convertFromJSONObject(val);
      } else if (Array.isArray(val)) {
        res = deserializeObject(val, `${prefix}${key}[].`);
      } else if (isObject(val)) {
        res = deserializeObject(val, `${path}.`);
      }

      outObj[key] = res;
    }

    return outObj;
  }
}

function generateConversionPaths(TMessage, opts, prefix) {
  prefix = prefix || '';

  if (!opts) {
    opts = {
      objectIdPaths: new Set(),
      timestampPaths: new Set(),
      jsonPaths: new Set()
    };
  }

  var objectIdPaths = opts.objectIdPaths;
  var timestampPaths = opts.timestampPaths;
  var jsonPaths = opts.jsonPaths;

  for (let field of TMessage.children) {

    if (field.className !== 'Message.Field') {
      continue;
    }

    var suffix = field.repeated ? '[]' : '';

    if (field.options.hasOwnProperty('(objectId)')) {
      objectIdPaths.add(prefix + field.name + suffix);
    } else if (field.type.name === 'message' && field.resolvedType.fqn() === '.ortoo.JSONObject') {
      jsonPaths.add(prefix + field.name + suffix);
    } else if (field.type.name === 'message' && field.resolvedType.fqn() === '.google.protobuf.Timestamp') {
      timestampPaths.add(prefix + field.name + suffix);
    } else if (field.type.name === 'message') {
      generateConversionPaths(field.resolvedType, opts, prefix + field.name + suffix + '.');
    }
  }

  return opts;
}

function convertToObjectId(val) {
  return val ? new ObjectId(val) : null;
}

function convertTimestampToDate(val) {

  if (!val) {
    return null;
  }

  var millis = val.seconds * 1000 + Math.round(val.nanos / 1e6);
  return new Date(millis);
}

function convertDateToTimestamp(val) {
  return {
    seconds: Math.floor(val.getTime() / 1000),
    nanos: val.getUTCMilliseconds() * 1e6
  };
}

function convertFromObjectId(val) {
  return String(val);
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

function objDeserializeStream(deserializer) {
  return through2.obj(function(obj, enc, callback) {
    callback(null, deserializer(obj));
  });
}
