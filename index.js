const path = require('path');

const isString = require('lodash.isstring');
const ProtoBuf = require('@ortoo/protobufjs');

const client = require('./client');
const impl = require('./impl');
const wrappers = require('./wrappers');

exports.createClient = client;
exports.createImpl = impl;
exports.applyCustomWrappers = applyCustomWrappers;
exports.applyProtoRoot = applyProtoRoot;


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
