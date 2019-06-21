'use strict';

exports.handler = async (event) => {
    // TODO implement
    const body = JSON.parse(event.body)
    const response = {
        statusCode: 200,
        body: JSON.stringify(body.proid),
    };
    return response;
};
