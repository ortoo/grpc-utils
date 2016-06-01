'use strict';

const isDate = require('lodash.isdate');
const isObject = require('lodash.isobject');
const BSON = require('bson');
const through2 = require('through2');

const ObjectId = BSON.ObjectId;

exports.objSerializeStream = objSerializeStream;
exports.objDeserializeStream = objDeserializeStream;
exports.serializeObject = serializeObject;
exports.createObjectDeserializer = createObjectDeserializer;

function objSerializeStream() {
  return through2.obj(function(obj, enc, callback) {
    callback(null, serializeObject(obj));
  });
}

function serializeObject(obj) {
  var outObj;
  if (Array.isArray(obj)) {
    outObj = [];
  } else {
    outObj = {};
  }
  for (let key in obj) {
    let val = obj[key];

    // Don't include any __ fields or any null fields (it seems null messes up non message fields)
    if (key.startsWith('__') || val === null) {
      continue;
    }


    let res = val;
    if (val instanceof ObjectId) {
      // Object ids to string
      res = String(val);
    } else if (isDate(val)) {
      // Date to timestamp
      res = {
        seconds: Math.floor(val.getTime() / 1000),
        nanos: val.getUTCMilliseconds() * 1e6
      };
    } else if (Array.isArray(val) || isObject(val)) {
      res = serializeObject(val);
    }

    outObj[key] = res;
  }

  return outObj;
}

function createObjectDeserializer(TObj) {

  var timestampPaths = new Set();
  var objectIdPaths = new Set();

  generateConversionPaths(TObj);

  return deserializeObject;

  function generateConversionPaths(TMessage, prefix) {
    prefix = prefix || '';

    for (let field of TMessage.children) {

      if (field.className !== 'Message.Field') {
        continue;
      }

      if (field.repeated) {
        prefix += '[].';
      }

      if (field.options.hasOwnProperty('(objectId)')) {
        objectIdPaths.add(prefix + field.name);
      } else if (field.type.name === 'message' && field.resolvedType.name === 'Timestamp') {
        timestampPaths.add(prefix + field.name);
      } else if (field.type.name === 'message') {
        generateConversionPaths(field.resolvedType, prefix + field.name + '.');
      }
    }
  }

  function deserializeObject(obj, prefix) {

    prefix = prefix || '';


    var outObj;
    if (Array.isArray(obj)) {
      outObj = [];
    } else {
      outObj = {};
    }
    for (let key in obj) {
      let val = obj[key];
      let res = val;

      // Is the key in our timestamp paths
      if (timestampPaths.has(prefix + key)) {
        res = convertTimestampToDate(val);
      } else if (objectIdPaths.has(prefix + key)) {
        res = convertToObjectId(val);
      } else if (Array.isArray(val)) {
        res = deserializeObject(val, `${prefix}[].`);
      } else if (isObject(val)) {
        res = deserializeObject(val, `${prefix}${key}.`);
      }

      outObj[key] = res;
    }

    return outObj;
  }

  function convertToObjectId(val) {
    return new ObjectId(val);
  }

  function convertTimestampToDate(val) {
    var millis = val.seconds * 1000 + Math.round(val.nanos / 1e6);
    return new Date(millis);
  }
}

function objDeserializeStream(deserializer) {
  return through2.obj(function(obj, enc, callback) {
    callback(null, deserializer(obj));
  });
}
