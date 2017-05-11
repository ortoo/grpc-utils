const path = require('path');

const ObjectId = require('bson-objectid');
const isUndefined = require('lodash.isundefined');
const isString = require('lodash.isstring');
const ProtoBuf = require('protobufjs');

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
      let repeated = valField.repeated;

      let valArr = repeated ? val : [val];
      let outValArr = valArr.map((outVal) => {
        return resolvedType ? resolvedType.fromObject(outVal) : outVal;
      });

      return {value: repeated ? outValArr : outValArr[0]};
    }
  },

  toObject: function (obj, opts) {
    if (obj && obj.isNull) {
      return null;
    } else if (obj) {
      var valField = this.fields.value;
      let resolvedType = valField.resolvedType;
      let repeated = valField.repeated;

      let valArr = repeated ? obj.value : [obj.value];
      let outValArr = valArr.map((outVal) => {
        return resolvedType ? resolvedType.toObject(outVal, opts) : outVal;
      });

      return repeated ? outValArr : outValArr[0];
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
