'use strict'

const _ = require('lodash');
const AWS = require('aws-sdk')
const {REC_COLS, REC_REQUIRED_COLS, TABLE_PAR_KEY} = require('../config/table_info.js');

module.exports = class AthenaProvider {
  constructor () {
  }

  fill(params) {
    if (!this._checkRequiredCol(params)) return false
    this._colParams = this._getColParams(params)
    this._objKey = this._getObjKey()
    return true
  }

  save() {
    const S3Append = require('s3-append').S3Append
    const Format = require('s3-append').Format
    const s3config = this._getS3Config()
    return new Promise((resolve, reject) => {
      const service = new S3Append(s3config, this._objKey, Format.Text)
      service.append(this._colParams)
      service.flush()
        .then(() => { resolve() })
        .catch(err => {
          if (err.code === 'NoSuchKey') {
            this._createObject()
              .then(() => { resolve() })
              .catch((coErr) => { reject(coErr) })
          } else {
            return reject(err)
          }
        })
    });
  }

  _createObject() {
    const s3 = new AWS.S3()
    return new Promise((resolve, reject) => {
      return s3.putObject({
        Bucket: process.env.BUCKET,
        Key: this._objKey,
        Body: JSON.stringify(this._colParams)+'\n'
      }, (err, data) => {
        if (err) return reject(err)
        else return resolve()
      })
    })
  }

  _getObjKey() {
    const ymd = this._colParams.created.split(' ')[0]
    return process.env.S3_OBJECT_PREFIXS + '/' + this._getPartitionPrefix(ymd) + '/' + this._getFileName(ymd)
  }

  _getPartitionPrefix(ymd) {
    return TABLE_PAR_KEY + '=' + ymd;
  }

  _getFileName(ymd) {
    return ymd + '.json';
  }

  _checkRequiredCol(params) {
    REC_REQUIRED_COLS.forEach(col => {
      if (!params[col]) return false
    });
    return true
  }

  _getS3Config() {
    const S3Config = require('s3-append').S3Config
    return new S3Config({
      "accessKeyId": process.env.IAM_ACCESS_KEY_ID,
      "secretAccessKey": process.env.IAM_SECRET_ACCESS_KEY,
      "bucket": process.env.BUCKET
    })
  }

  _getColParams(params) {
    return _.mapValues(REC_COLS , (val, col) => {
      if (!params.hasOwnProperty(col)) return null
      return params[col]
    });
  }
}
