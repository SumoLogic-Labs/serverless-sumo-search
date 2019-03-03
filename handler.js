const _ = require('lodash');
const assert = require('assert');
const uuidv4 = require('uuid/v4');
const http = require('got');
const log = require('loglevel');
const papa = require('papaparse');
const S3S = require('s3-streams');
const AWS = require('aws-sdk');
var Ajv = require('ajv');

log.setLevel('debug');

//
// Lambda functions
//

const searchAsyncSchema = {
    type: 'object',
    properties: {
        endpoint: {type: 'string', pattern: '^prod|us2|au|de|eu|jp$'},
        accessId: {type: 'string'},
        accessKey: {type: 'string'},
        query: {type: 'string'},
        to: {type: 'string', pattern: '^\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:[0-5]\\d$'},
        from: {type: 'string', pattern: '^\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:[0-5]\\d$'},
        timeZone: {type: 'string'},
        messages: {type: 'boolean'},
        records: {type: 'boolean'},
        s3Bucket: {type: 'string'},
        s3KeyPrefix: {type: 'string'},
    },
    required: ['endpoint', 'accessId', 'accessKey', 'query', 'to', 'from', 'timeZone',
        'messages', 'records', 's3Bucket', 's3KeyPrefix']
};

module.exports.searchAsyncApi = async function (event) {
    validateSchema(searchAsyncSchema, event);

    // Get the request body
    const body = JSON.parse(event.body);
    debug(d`Body: ${body}`);

    // Invoke the state machine
    body.id = uuidv4();
    const result = await invokeStateMachine(body);
    debug(d`Result: ${result}`);

    const response = {
        statusCode: 200,
        body: JSON.stringify(result)
    };
    debug(d`Response: ${response}`);
    return response;
};

module.exports.searchAsync = async function (event) {
    validateSchema(searchAsyncSchema, event);

    // Invoke the state machine and return the result
    event.id = uuidv4();
    const result = await invokeStateMachine(event);
    debug(d`Result: ${result}`);
    return result;
};


//
// Lambda functions for the state machine
//

const startSchema = {
    type: 'object',
    properties: {
        endpoint: {type: 'string', pattern: '^prod|us2|au|de|eu|jp$'},
        accessId: {type: 'string'},
        accessKey: {type: 'string'},
        query: {type: 'string'},
        to: {type: 'string', pattern: '^\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:[0-5]\\d$'},
        from: {type: 'string', pattern: '^\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:[0-5]\\d$'},
        timeZone: {type: 'string'},
        messages: {type: 'boolean'},
        records: {type: 'boolean'},
        s3Bucket: {type: 'string'},
        s3KeyMessages: {type: 'string'},
        s3KeyRecords: {type: 'string'},
        id: {type: 'string'},
    },
    required: ['endpoint', 'accessId', 'accessKey', 'query', 'to', 'from', 'timeZone',
        'messages', 'records', 's3Bucket', 's3KeyMessages', 's3KeyRecords', 'id']
};
module.exports.start = async function (event) {
    validateSchema(startSchema, event);

    // Start the search job
    debug(d`Starting search job: ${event}`);
    const options = createHttpOptions(event, {
        method: 'POST',
        body: {
            query: event.query,
            from: event.from,
            to: event.to,
            timeZone: event.timeZone
        }
    });
    const response = await http('/api/v1/search/jobs', options);
    debug(d`Starting search job response: ${response.body}`);
    return {
        endpoint: event.endpoint,
        accessId: event.accessId,
        accessKey: event.accessKey,
        query: event.query,
        from: event.from,
        to: event.to,
        messages: event.messages,
        records: event.records,
        s3Bucket: event.s3Bucket,
        s3KeyMessages: event.s3KeyMessages,
        s3KeyRecords: event.s3KeyRecords,
        snsTopic: event.snsTopic,
        id: event.id,
        cookie: response.headers['set-cookie'],
        searchJobId: response.body.id,
    };
};

const pollSchema = {
    type: 'object',
    properties: {
        endpoint: {type: 'string', pattern: '^prod|us2|au|de|eu|jp$'},
        accessId: {type: 'string'},
        accessKey: {type: 'string'},
        messages: {type: 'boolean'},
        records: {type: 'boolean'},
        s3Bucket: {type: 'string'},
        s3KeyMessages: {type: 'string'},
        s3KeyRecords: {type: 'string'},
        id: {type: 'string'},
        // cookie: {type: 'string'},
        searchJobId: {type: 'string'}
    },
    required: ['endpoint', 'accessId', 'accessKey', 'messages', 'records',
        's3Bucket', 's3KeyMessages', 's3KeyRecords', 'id', 'cookie', 'searchJobId']
};
module.exports.poll = async function (event) {
    validateSchema(pollSchema, event);

    // Poll the search job status
    debug(d`Polling search job: ${event}`);
    const options = createHttpOptions(event, {});
    const response = await http(`/api/v1/search/jobs/${event.searchJobId}`, options);
    debug(d`Polling search job response: ${response.body}`);
    return {
        endpoint: event.endpoint,
        accessId: event.accessId,
        accessKey: event.accessKey,
        query: event.query,
        from: event.from,
        to: event.to,
        messages: event.messages,
        records: event.records,
        s3Bucket: event.s3Bucket,
        s3KeyMessages: event.s3KeyMessages,
        s3KeyRecords: event.s3KeyRecords,
        snsTopic: event.snsTopic,
        id: event.id,
        cookie: response.headers['set-cookie'],
        searchJobId: event.searchJobId,
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
        debug(d`Not dumping messages. Parameter "messages": ${event.messages}`);
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

function validateSchema(schema, data) {
    const ajv = new Ajv({verbose: true, allErrors: true});
    const valid = ajv.validate(schema, data);
    if (!valid) {
        log.error(`validateSchema - ${JSON.stringify(data)}`);
        log.error(`validateSchema - ${ajv.errors}`);
        throw Error(JSON.stringify(ajv.errors));
    }
}

const invokeStateMachineSchema = {
    type: 'object',
    properties: {
        endpoint: {type: 'string', pattern: '^prod|us2|au|de|eu|jp$'},
        accessId: {type: 'string'},
        accessKey: {type: 'string'},
        query: {type: 'string'},
        to: {type: 'string', pattern: '^\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:[0-5]\\d$'},
        from: {type: 'string', pattern: '^\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:[0-5]\\d$'},
        timeZone: {type: 'string'},
        messages: {type: 'boolean'},
        records: {type: 'boolean'},
        s3Bucket: {type: 'string'},
        s3KeyPrefix: {type: 'string'},
        id: {type: 'string'}
    },
    required: ['endpoint', 'accessId', 'accessKey', 'query', 'to', 'from', 'timeZone',
        'messages', 'records', 's3Bucket', 's3KeyPrefix', 'id']
};

async function invokeStateMachine(event) {
    validateSchema(invokeStateMachineSchema, event);

    // Unless S3 keys are explicitly specified, create them from the specified prefix
    if (!event.s3KeyMessages) {
        event.s3KeyMessages = `${event.s3KeyPrefix}${event.id}_messages.csv`;
    }
    if (!event.s3KeyRecords) {
        event.s3KeyRecords = `${event.s3KeyPrefix}${event.id}_records.csv`;
    }

    // Invoke the state machine asynchronously and get the response
    const params = {
        stateMachineArn: process.env.statemachine_arn,
        input: JSON.stringify(event)
    };
    debug(d`State machine invocation params: ${params}`);
    const stepfunctions = new AWS.StepFunctions();
    const response = await stepfunctions.startExecution(params).promise();
    debug(d`Response: ${response}`);

    // Return S3 keys and state machine invocation details.
    const result = {
        id: event.id,
        s3KeyMessages: `s3://${event.s3Bucket}/${event.s3KeyMessages}`,
        s3KeyRecords: `s3://${event.s3Bucket}/${event.s3KeyRecords}`
    };
    debug(d`Result: ${result}`);
    return result;
}

const writeSearchResultsToS3Schema = {
    type: 'object',
    properties: {
        endpoint: {type: 'string', pattern: '^prod|us2|au|de|eu|jp$'},
        accessId: {type: 'string'},
        accessKey: {type: 'string'},
        s3Bucket: {type: 'string'},
        s3KeyMessages: {type: 'string'},
        s3KeyRecords: {type: 'string'},
        id: {type: 'string'},
        // cookie: {type: 'string'},
        searchJobId: {type: 'string'},
        messageCount: {type: 'number'},
        recordCount: {type: 'number'}
    },
    required: ['endpoint', 'accessId', 'accessKey', 's3Bucket', 's3KeyMessages', 's3KeyRecords',
        'id', 'cookie', 'searchJobId', 'messageCount', 'recordCount']
};

async function writeSearchResultsToS3(event, messagesOrRecords) {

    //
    // Helper functions
    //

    function getFields(data) {
        const result = _.map(data.fields, (field) => {
            return field.name
        });
        debug(d`Fields: ${result}`);
        return result;
    }

    function toCSV(isFirst, fields, data) {
        let result = '';
        if (isFirst) {
            result += papa.unparse([fields]);
            result += '\n';
        }
        debug(d`Result: ${result}`);
        let rows = _.map(data, (row) => {
            const map = row.map;
            return _.map(fields, (field) => {
                return map[field]
            })
        });
        result += papa.unparse(rows);
        debug(d`Result: ${result}`);
        return result;
    }

    //
    // Implementation
    //
    validateSchema(writeSearchResultsToS3Schema, event);

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
            debug(d`S3 upload finished. Path: ${s3Path}`);
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
        const path = `/api/v1/search/jobs/${event.searchJobId}/${messagesOrRecords}?offset=${offset}&limit=${limit}`;
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
        debug(d`Got data for offset: ${offset}, limit: ${limit}. Total data now: ${totalDataSize}`);
        s3Stream.write(output);
        isFirst = false;
    }
    s3Stream.end();
    const result = await fin;

    // Send SNS notification if desired
    if (event.snsTopic) {
        debug(d`snsTopic: ${event.snsTopic}`);
        const sns = new AWS.SNS();
        const subject = `Sumo Logic Serverless Search ${event.id}`;
        const message = JSON.stringify({
            query: event.query,
            from: event.from,
            to: event.to,
            id: event.id,
            type: messagesOrRecords,
            s3: messagesOrRecords === 'messages' ? result.messagesPath : result.recordsPath
        });
        await sns.publish({
            TopicArn: event.snsTopic,
            Subject: subject,
            Message: message
        }).promise();
        debug(d`Sent message to snsTopic: ${event.snsTopic}`);
    } else {
        debug(d`No SNS topic specified, not sending message`);
    }

    return result;
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

//
// Borkdrive 12000hp
//

function debug(message) {
    log.debug(`${new Date().toISOString()} ${debug.caller.name} - ${message}`);
}

function d(strings) {
    let result = "";
    let counter = 0;
    let length = arguments.length - 1;
    for (let i = 0; i < strings.length; i++) {
        result += strings[i];
        if (counter < length) {
            const argument = arguments[++counter];
            if (typeof argument === 'object') {
                result += JSON.stringify(argument);
            } else {
                result += argument;
            }
        }
    }
    return result;
}