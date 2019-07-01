'use strict'

const _ = require('lodash');
const AWS = require('aws-sdk');
const athena = new AWS.Athena({region: 'ap-northeast-1'});
const {
  REC_COLS,
  REC_REQUIRED_COLS,
  TABLE_PAR_KEY,
  BUCKET,
  S3_OBJECT_PREFIXS,
  ATHENA_OUTPUT_LOCATION,
  ATHENA_DATEBASE
} = require('../config/table_info.js');
const { queryGenerator } = require('./util/queryGenerator.js');
module.exports = class AthenaProvider {
  constructor () {
  }

  fill(params) {
    if (!this._checkRequiredCol(params)) return false;
    this._colParams = this._getColParams(params);
    this._objKey = this._getObjKey();
    return true;
  }

  save() {
    const S3Append = require('s3-append').S3Append;
    const Format = require('s3-append').Format;
    const s3config = this._getS3Config();
    return new Promise((resolve, reject) => {
      const service = new S3Append(s3config, this._objKey, Format.Text);
      service.append(this._colParams);
      service.flush()
        .then(() => { resolve() })
        .catch(err => {
          if (err.code === 'NoSuchKey') {
            this._createObject()
              .then(() => { resolve(); })
              .catch((coErr) => { reject(coErr); });
          } else {
            return reject(err);
          }
        });
    });
  }

  find(params) {
    return new Promise((resolve, reject) => {
      Promise.resolve(params)
        .then(this._startQuery)
        .then(this._waitQueryEnd)
        .then(this._getResult)
        .then((result) => {
          resolve(result);
        })
        .catch((error) => {
          reject(error);
        });
    });
  }

  findCount(params) {
    return new Promise((resolve, reject) => {
      Promise.resolve(params)
        .then(this._startCountQuery)
        .then(this._waitQueryEnd)
        .then(this._getResult)
        .then((result) => {
          resolve(result);
        })
        .catch((error) => {
          reject(error);
        });
    });
  }

  setPartitioning(dt) {
    return new Promise((resolve, reject) => {
      Promise.resolve(dt)
        .then(this._startSetPartitioningQuery)
        .then(this._waitQueryEnd)
        .then((result) => {
          resolve(result);
        })
        .catch((error) => {
          reject(error);
        });
    });
  }

  _startQuery(params) {
    return new Promise((resolve, reject) => {
      const query_params = {
        QueryString: queryGenerator.createSelectQuery(params),
        ResultConfiguration: {OutputLocation: ATHENA_OUTPUT_LOCATION},
        QueryExecutionContext: {Database: ATHENA_DATEBASE}
      };
      athena.startQueryExecution(query_params, (err, data) => {
        resolve(data.QueryExecutionId);
      });
    });
  }

  _startCountQuery(params) {
    return new Promise((resolve, reject) => {
      const query_params = {
        QueryString: queryGenerator.createSelectQuery(params, true),
        ResultConfiguration: {OutputLocation: ATHENA_OUTPUT_LOCATION},
        QueryExecutionContext: {Database: ATHENA_DATEBASE}
      };
      athena.startQueryExecution(query_params, (err, data) => {
        resolve(data.QueryExecutionId);
      });
    });
  }

  _startSetPartitioningQuery(dt) {
    return new Promise((resolve, reject) => {
      const query_params = {
        QueryString: queryGenerator.createPartitioningSql(dt),
        ResultConfiguration: {OutputLocation: ATHENA_OUTPUT_LOCATION},
        QueryExecutionContext: {Database: ATHENA_DATEBASE}
      };
      athena.startQueryExecution(query_params, (err, data) => {
        resolve(data.QueryExecutionId);
      });
    });
  }

  _waitQueryEnd(queryExecutionId) {
    return new Promise((resolve, reject) => {
      var time = 0;
      const params = {QueryExecutionId: queryExecutionId};
      (function loop() {
        if (time != 20) {
          athena.getQueryExecution(params, (err, data) => {
            console.log('Status : ' , data.QueryExecution.Status.State);
            if (data.QueryExecution.Status.State == 'SUCCEEDED'){
              resolve(queryExecutionId);
            } else {
              const startMsec = new Date();
              while (new Date() - startMsec < 1000);
              time+=1;
              loop();
            }
          });
        } else {
          reject('408 Request Timeout');
          return false;
        }
      }())
    })
  }

  _getResult(queryExecutionId) {
    return new Promise((resolve, reject) => {
      var params = {
        QueryExecutionId: queryExecutionId, /* required */
      };
      athena.getQueryResults(params, (err, result) => {
        var response = [];
        const resCols = result.ResultSet.Rows[0].Data.map(data => data.VarCharValue)
        for(let i = 1; i < result.ResultSet.Rows.length; i++) {
          const row = result.ResultSet.Rows[i].Data;
          var data = {}
          resCols.forEach((col, index) => {
            data[col] = row[index].VarCharValue
          })
          response.push(data);
        }
        resolve(response);
      });
    });
  }

  _createObject() {
    return new Promise((resolve, reject) => {
      const s3 = new AWS.S3()
      return s3.putObject({
        Bucket: BUCKET,
        Key: this._objKey,
        Body: JSON.stringify(this._colParams)+'\n'
      }, (err, data) => {
        if (err) {
          return reject(err)
        } else {
          const ymd = this._colParams.created.split(' ')[0];
          this.setPartitioning(ymd)
            .then(() => { resolve(); })
            .catch((coErr) => { reject(coErr); });
        }
      })
    })
  }

  _getObjKey() {
    const ymd = this._colParams.created.split(' ')[0]
    return S3_OBJECT_PREFIXS + '/' + this._getPartitionPrefix(ymd) + '/' + this._getFileName(ymd)
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
      "bucket": BUCKET
    })
  }

  _getColParams(params) {
    return _.mapValues(REC_COLS , (val, col) => {
      if (!params.hasOwnProperty(col)) return null
      return params[col]
    });
  }
}
