'use strict';

const AthenaProvider = require('./src/AthenaProvider.js');

exports.handler = async (event) => {
  const body = JSON.parse(event.body)
  const athenaProvider = new AthenaProvider()

  if (!athenaProvider.fill(body)) {
    return {
      statusCode: 200,
      body: JSON.stringify({'result':0})
    };
  }

  return athenaProvider.save()
    .then(() =>{
      return {
        statusCode: 200,
        body: JSON.stringify({'result':1})
      }})
    .catch((err) => {
      console.log(err)
      return {
        statusCode: 400,
        body: JSON.stringify({'result':0})
      }})
};
