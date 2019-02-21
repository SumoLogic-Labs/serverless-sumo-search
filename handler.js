'use strict';

const _ = require('lodash');
const assert = require('assert');
const uuidv4 = require('uuid/v4');
const http = require('got');
const log = require('loglevel');
const papa = require('papaparse');
const S3S = require('s3-streams');
const AWS = require('aws-sdk');

log.setLevel('debug');

// Re-enable XRay once I figure out how to connect across
// step function invocations.
// const AWSXRay = require('aws-xray-sdk');
// AWSXRay.captureAWS(AWS);

//
// Lambda functions
//

module.exports.searchAsyncApi = async function (event) {
    log.debug(`searchAsyncApi - Event: ${JSON.stringify(event)}`);

    // Get the request body
    const body = JSON.parse(event.body);
    log.debug(`searchAsyncApi - Body: ${JSON.stringify(body)}`);

    // Invoke the state machine
    const result = await invokeStateMachine(body);
    log.debug(`searchAsyncApi - Result: ${JSON.stringify(result)}`);

    const response = {
        statusCode: 200,
        body: JSON.stringify(result)
    };
    log.debug(`searchAsyncApi - Response: ${JSON.stringify(response)}`);
    return response;
};

module.exports.searchAsync = async function (event) {
    log.setLevel('debug');
    log.debug(`searchAsync - Event: ${JSON.stringify(event)}`);

    // Invoke the state machine and return the result
    const result = await invokeStateMachine(event);
    log.debug(`searchAsync - Result: ${JSON.stringify(result)}`);
    return result;
};


//
// Lambda functions for the state machine
//

module.exports.start = async function (event) {

    // Input validation
    assert(event.endpoint, 'Missing argument "endpoint"');
    assert(["prod", "us2", "au", "de", "eu", "jp"].includes(event.endpoint),
        `Unknown endpoint "${event.endpoint}"`);
    assert(event.accessId, 'Missing argument "accessId"');
    assert(event.accessKey, 'Missing argument "accessKey"');
    assert(event.query, 'Missing argument "query"');
    assert(event.to, 'Missing argument "to"');
    assert(event.from, 'Missing argument "from"');
    assert(event.timeZone, 'Missing argument "timeZone"');
    assert('messages' in event, 'Missing argument "messages"');
    assert('records' in event, 'Missing argument "records"');
    assert(event.s3Bucket, 'Missing argument "s3Bucket"');
    assert(event.s3KeyMessages, 'Missing argument "s3KeyMessages"');
    assert(event.s3KeyRecords, 'Missing argument "s3KeyRecords"');
    assert(event.messages || event.records, 'Either "messages" or "records" need to be true');

    // Start the search job
    const options = createHttpOptions(event, {
        method: 'POST',
        body: {
            query: event.query,
            from: event.from,
            to: event.to,
            timeZone: event.timeZone
        }
    });
    // const subsegment = new AWSXRay.Segment("create-job");
    // console.log(`Segment: ${subsegment}`);
    const response = await http('/api/v1/search/jobs', options);
    // subsegment.close();
    return {
        endpoint: event.endpoint,
        accessId: event.accessId,
        accessKey: event.accessKey,
        messages: event.messages,
        records: event.records,
        s3Bucket: event.s3Bucket,
        s3KeyMessages: event.s3KeyMessages,
        s3KeyRecords: event.s3KeyRecords,
        cookie: response.headers['set-cookie'],
        id: response.body.id,
    };
};

module.exports.poll = async function (event) {

    // Input validation
    assert(event.endpoint, 'Missing argument "endpoint"');
    assert(["prod", "us2", "au", "de", "eu", "jp"].includes(event.endpoint),
        `Unknown endpoint "${event.endpoint}"`);
    assert(event.accessId, 'Missing argument "accessId"');
    assert(event.accessKey, 'Missing argument "accessKey"');
    assert('messages' in event, 'Missing argument "messages"');
    assert('records' in event, 'Missing argument "records"');
    assert(event.s3Bucket, 'Missing argument "s3Bucket"');
    assert(event.s3KeyMessages, 'Missing argument "s3KeyMessages"');
    assert(event.s3KeyRecords, 'Missing argument "s3KeyRecords"');
    assert(event.cookie, 'Missing argument "cookie"');
    assert(event.id, 'Missing argument "id"');

    // Poll the search job status
    const options = createHttpOptions(event, {});
    const response = await http(`/api/v1/search/jobs/${event.id}`, options);
    return {
        endpoint: event.endpoint,
        accessId: event.accessId,
        accessKey: event.accessKey,
        messages: event.messages,
        records: event.records,
        s3Bucket: event.s3Bucket,
        s3KeyMessages: event.s3KeyMessages,
        s3KeyRecords: event.s3KeyRecords,
        cookie: response.headers['set-cookie'],
        id: event.id,
        state: response.body.state,
        messageCount: response.body.messageCount,
        recordCount: response.body.recordCount,
        pendingWarnings: response.body.pendingWarnings,
        pendingErrors: response.body.pendingErrors
    };
};

module.exports.dumpMessages = async function (event) {

    // Input validation
    assert('messages' in event, 'Missing argument "messages"');
    assert(event.s3KeyMessages, 'Missing argument "s3KeyMessages"');

    // Write out result messages
    if (!event.messages) {
        log.debug(`dumpMessages - Not dumping messages. Parameter "messages": ${event.messages}`);
        return;
    }
    return writeSearchResultsToS3(event, "messages");
};

module.exports.dumpRecords = async function (event) {

    // Input validation
    assert('messages' in event, 'Missing argument "records"');
    assert(event.s3KeyRecords, 'Missing argument "s3KeyRecords"');

    // Write out result records
    if (!event.records) {
        log.error(`dumpRecords - Not dumping messages. Parameter "records": ${event.records}`);
        return;
    }

    return writeSearchResultsToS3(event, "records");
};

//
// Non-lambda implementation
//

async function invokeStateMachine(event) {
    assert(event.s3KeyPrefix, 'Missing argument "s3KeyPrefix"');

    log.debug(`invokeStateMachine - Event: ${JSON.stringify(event)}`);

    // Unless S3 keys are explicitly specified, create them from the specified prefix
    const uuid = uuidv4();
    if (!event.s3KeyMessages) {
        event.s3KeyMessages = `${event.s3KeyPrefix}${uuid}_messages.csv`;
    }
    if (!event.s3KeyRecords) {
        event.s3KeyRecords = `${event.s3KeyPrefix}${uuid}_records.csv`;
    }

    // Invoke the state machine asynchronously and get the response
    const params = {
        stateMachineArn: process.env.statemachine_arn,
        input: JSON.stringify(event)
    };
    const stepfunctions = new AWS.StepFunctions();
    const response = await stepfunctions.startExecution(params, function (err, data) {
        if (err) {
            log.error(`invokeStateMachine - Err: ${err}`);
            log.error(`invokeStateMachine - Data: ${JSON.stringify(data)}`);
        } else {
            log.debug(`invokeStateMachine - Data: ${JSON.stringify(data)}`);
        }
    }).promise();
    log.debug(`invokeStateMachine - Response: ${JSON.stringify(response)}`);

    // Return S3 keys and state machine invocation details.
    const result = {
        s3KeyMessages: `s3://${event.s3Bucket}/${event.s3KeyMessages}`,
        s3KeyRecords: `s3://${event.s3Bucket}/${event.s3KeyRecords}`,
        executionArn: response.executionArn,
        startDate: response.startDate
    };
    log.debug(`invokeStateMachine - Result: ${JSON.stringify(result)}`);
    return result;
}

async function writeSearchResultsToS3(event, messagesOrRecords) {

    //
    // Helper functions
    //

    function getFields(data) {
        const result = _.map(data.fields, (field) => {
            return field.name
        });
        log.debug(`writeSearchResultsToS3.getFields - Fields: ${result}`);
        return result;
    }

    function toCSV(isFirst, fields, data) {
        let result = '';
        if (isFirst) {
            result += papa.unparse([fields]);
            result += '\n';
        }
        log.debug(`writeSearchResultsToS3.toCSV - Result: ${result}`);
        let rows = _.map(data, (row) => {
            const map = row.map;
            return _.map(fields, (field) => {
                return map[field]
            })
        });
        result += papa.unparse(rows);
        log.debug(`writeSearchResultsToS3.toCSV - Result: ${result}`);
        return result;
    }

    //
    // Implementation
    //

    // Input validation
    assert(event.endpoint, 'Missing argument "endpoint"');
    assert(["prod", "us2", "au", "de", "eu", "jp"].includes(event.endpoint),
        `Unknown endpoint "${event.endpoint}"`);
    assert(event.accessId, 'Missing argument "accessId"');
    assert(event.accessKey, 'Missing argument "accessKey"');
    assert(event.s3Bucket, 'Missing argument "s3Bucket"');
    assert(event.s3KeyMessages, 'Missing argument "s3KeyMessages"');
    assert(event.s3KeyRecords, 'Missing argument "s3KeyRecords"');
    assert(event.cookie, 'Missing argument "cookie"');
    assert(event.id, 'Missing argument "id"');
    assert(event.messageCount, 'Missing argument "messageCount"');
    assert(event.recordCount, 'Missing argument "recordCount"');

    // Create S3 stream to write the result
    const s3Key = (messagesOrRecords === 'messages') ? event.s3KeyMessages : event.s3KeyRecords;
    const s3Stream = S3S.WriteStream(new AWS.S3(), {
        'Bucket': event.s3Bucket,
        'Key': s3Key
    });
    // Imminent insanity
    const fin = new Promise((resolve, reject) => {
        s3Stream.on('error', function (err) {
            log.error(`writeSearchResultsToS3 - Error!`);
            log.error(err);
            reject(err);
        }).on('finish', function () {
            const s3Path = `s3://${event.s3Bucket}/${s3Key}`;
            log.debug(`writeSearchResultsToS3 - S3 upload finished. Path: ${s3Path}`);
            if (messagesOrRecords === 'messages') {
                resolve({messagesPath: s3Path});
            } else {
                resolve({recordsPath: s3Path});
            }
        });
    });

    // Setup poll/write loop
    const options = createHttpOptions(event, {});
    const maxLimit = 100;
    const count = (messagesOrRecords === 'messages') ? event.messageCount : event.recordCount;
    let totalDataSize = 0;
    let isFirst = true;
    let fields;
    for (let offset = 0; offset < count; offset += maxLimit) {

        // Get the next set of results
        const limit = Math.min(maxLimit, count - offset);
        const path = `/api/v1/search/jobs/${event.id}/${messagesOrRecords}?offset=${offset}&limit=${limit}`;
        const response = await http(path, options);
        const data = response.body;

        // Special case the first set of results to extract header fields
        if (isFirst) {
            fields = getFields(data);
        }

        // Write the results to S3
        const output = toCSV(isFirst, fields,
            (messagesOrRecords === 'messages') ? data.messages : data.records);
        totalDataSize += output.length;
        log.debug(`writeSearchResultsToS3 - Got data for offset: ${offset}, limit: ${limit}. Total data now: ${totalDataSize}`);
        s3Stream.write(output);
        isFirst = false;
    }
    s3Stream.end();
    return await fin;
}

function createHttpOptions(event, options) {

    // Figure out the endpoint, deal with prod deployment special case
    let endpoint = `.${event.endpoint}`;
    if (endpoint === '.prod') {
        endpoint = '';
    }

    // Return the options
    return Object.assign(options, {
        baseUrl: `https://api${endpoint}.sumologic.com`,
        auth: `${event.accessId}:${event.accessKey}`,
        headers: {
            'Content-Type': 'application/json',
            'Cookie': event.cookie
        },
        json: true,
    });
}