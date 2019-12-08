'use strict';

const AthenaProvider = require('./src/AthenaProvider.js');

exports.handler = async (event) => {
  const data = JSON.parse(event.body)
  const project = event.pathParameters.system
  if(process.env.DEBUG_MODE) {
    console.log(data)
  }
  const athenaProvider = new AthenaProvider(project)

  if (!athenaProvider.isExistProject()) {
    console.log('Cannot find project.');
    return {
      statusCode: 400,
      body: JSON.stringify({'result':0})
    };
  }

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
