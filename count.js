'use strict';

const AthenaProvider = require('./src/AthenaProvider.js');

exports.handler = async (event) => {
  const data = JSON.parse(event.body)
  const athenaProvider = new AthenaProvider()

  return athenaProvider.findCount(data)
    .then((result) =>{
      return {
        statusCode: 200,
        body: JSON.stringify({'result':result})
    }})
    .catch((err) => {
      console.log(err)
      return {
        statusCode: 400,
        body: JSON.stringify({'result':0})
      }})
};
