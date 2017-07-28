const path = require('path');

const ObjectId = require('bson-objectid');
const isUndefined = require('lodash.isundefined');
const isString = require('lodash.isstring');
const ProtoBuf = require('@ortoo/protobufjs');

const client = require('./client');
const impl = require('./impl');

exports.createClient = client;
exports.createImpl = impl;
exports.applyCustomWrappers = applyCustomWrappers;
exports.applyProtoRoot = applyProtoRoot;

const wrappers = {};

wrappers['.google.protobuf.Timestamp'] = {
  fromObject: function (val) {
    if (!val) {
      return val;
    }

    if (isString(val)) {
      val = new Date(val);
    }

    return {
      seconds: Math.floor(val.getTime() / 1000),
      nanos: val.getUTCMilliseconds() * 1e6
    };
  },

  toObject: function(val) {
    if (!val) {
      return val;
    }

    var millis = val.seconds * 1000 + Math.round(val.nanos / 1e6);
    return new Date(millis);
  }
};

wrappers['.ortoo.JSONObject'] = {
  fromObject: function (obj) {
    return {
      representation: JSON.stringify(obj)
    };
  },

  toObject: function (obj) {
    if (!obj) {
      return obj;
    }

    try {
      return JSON.parse(obj.representation);
    } catch (err) {
      // ignore
    }
  }
};

wrappers['.ortoo.ObjectId'] = {
  fromObject: function (val) {
    if (!val) {
      return val;
    }

    var strRep = val.toString ? val.toString() : String(val);
    return {value: new Buffer(strRep, 'hex')};
  },

  toObject: function (msg) {
    if (!msg) {
      return msg;
    }

    return (msg.value && msg.value.length) ? new ObjectId(msg.value) : undefined;
  }
};

wrappers['.ortoo.resource.*.wrappers.values.*'] = wrappers['.ortoo.resource.*.wrappers.arrays.*'] = {
  fromObject: function (val) {
    if (val === null) {
      return {isNull: true};
    } else if (!isUndefined(val)) {
      var valField = this.fields.value;
      let resolvedType = valField.resolvedType;

      // Could be map, array or individual value
      if (valField.map) {
        let outVal = {};
        var key;
        for (var keys = Object.keys(val), ii = 0; ii < keys.length ; ++ii) {
          key = keys[ii];
          outVal[key] = performWrap(resolvedType, val[key]);
        }
        return {value: outVal};
      } else if (valField.repeated) {
        return {value: val.map(outVal => performWrap(resolvedType, outVal))};
      } else {
        return {value: performWrap(resolvedType, val)};
      }
    }
  },

  toObject: function (obj, opts) {
    if (obj && obj.isNull) {
      return null;
    } else if (obj) {
      var valField = this.fields.value;
      let resolvedType = valField.resolvedType;
      let val = obj.value;

      if (valField.map) {
        let outVal = {};
        var key;
        for (var keys = Object.keys(val), ii = 0; ii < keys.length ; ++ii) {
          key = keys[ii];
          outVal[key] = performUnwrap(resolvedType, val[key], opts);
        }
        return outVal;
      } else if (valField.repeated) {
        return val.map(outVal => performUnwrap(resolvedType, outVal, opts));
      } else {
        return performUnwrap(resolvedType, val, opts);
      }
    }
  }
};

function applyCustomWrappers(ns) {
  for (let fullName of Object.keys(wrappers)) {
    let wrapper = wrappers[fullName];

    let origArr = getAppliableTypes(fullName, ns);

    for (let orig of origArr) {
      let origSetup = orig.setup;
      orig.setup = function (...args) {
        origSetup.call(this, args);
        let originalThis = Object.create(this);
        originalThis.fromObject = this.fromObject;
        this.fromObject = wrapper.fromObject.bind(originalThis);
        originalThis.toObject = this.toObject;
        this.toObject = wrapper.toObject.bind(originalThis);
      };
      orig.setup();
    }
  }
}

function performWrap(resolvedType, outVal) {
  if (resolvedType instanceof ProtoBuf.Enum) {
    return isString(outVal) ? resolvedType.values[outVal] : outVal;
  }

  return resolvedType ? resolvedType.fromObject(outVal) : outVal;
}

function performUnwrap(resolvedType, outVal, opts) {
  if (resolvedType instanceof ProtoBuf.Enum) {
    return isString(outVal) ? outVal : resolvedType.valuesById[outVal];
  }

  return resolvedType ? resolvedType.toObject(outVal, opts) : outVal;
}

function getAppliableTypes(wrapperName, root) {
  if (!wrapperName) {
    return [root];
  }

  var out = [];
  var pathSpl = wrapperName.split('.*');
  var parent = root.lookup(pathSpl[0]);

  if (parent && pathSpl.length > 1) {
    // Want all children of the parent
    for (let child of parent.nestedArray) {
      out.push(...getAppliableTypes([...pathSpl].splice(1).join('.*').replace(/^\./, ''), child));
    }
  } else if (parent) {
    out.push(parent);
  }

  return out;
}

function applyProtoRoot(filename, root) {
  if (isString(filename)) {
    return filename;
  }
  filename.root = path.resolve(filename.root) + '/';
  root.resolvePath = function(originPath, importPath, alreadyNormalized) {
    return ProtoBuf.util.path.resolve(filename.root,
                                      importPath,
                                      alreadyNormalized);
  };
  return filename.file;
}
