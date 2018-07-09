const isUndefined = require('lodash.isundefined');
const isString = require('lodash.isstring');
const ProtoBuf = require('@ortoo/protobufjs');
const BSON = require('bson');

const ObjectId = BSON.ObjectId;
const bson = new BSON();

const wrappers = {};

module.exports = wrappers;

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

wrappers['.ortoo.BSONObject'] = {
  fromObject: function (obj) {

    return {
      value: objToBson(obj)
    };
  },

  toObject: function (obj) {
    if (!obj) {
      return obj;
    }
    try {
      return objFromBson(obj.value);
    } catch (err) {
      // ignore
    }
  }
};

wrappers['.ortoo.JSONObject'] = {
  fromObject: function (obj) {

    return {
      value: JSON.stringify(obj)
    };
  },

  toObject: function (obj) {
    if (!obj) {
      return obj;
    }
    try {
      return JSON.parse(obj.value);
    } catch (err) {
      // ignore
    }
  }
};

wrappers['.ortoo.HybridObject'] = {
  fromObject: function (obj) {

    if (typeof obj === 'undefined') {
      return;
    }

    return {
      json: JSON.stringify(obj),
      bson: objToBson(obj)
    };
  },

  toObject: function (obj) {
    if (!obj) {
      return obj;
    }

    if (obj.bson) {
      try {
        return objFromBson(obj.bson);
      } catch (err) {
        // ignore
      }
    }

    try {
      return JSON.parse(obj.json);
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
    return {value: Buffer.from(strRep, 'hex')};
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

['Double', 'Float', 'Int64', 'UInt64', 'Int32', 'UInt32', 'Bool', 'String', 'Bytes'].forEach(type => {
  wrappers[`.google.protobuf.${type}Value`] = {
    fromObject(val) {
      if (isUndefined(val) || val === null) {
        return null;
      } else {
        return {
          value: val
        };
      }
    },

    toObject(obj) {
      return obj && obj.value;
    }
  };
});


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

function objToBson(obj) {
  // The root bson object must be an object, so we wrap once here...
  return bson.serialize({bson: obj});
}

function objFromBson(buf) {
  // Unwrap the inner bson wrapper (see above)
  return (bson.deserialize(buf).bson);
}
