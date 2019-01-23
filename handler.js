const assert = require('assert');
const http = require('got');
const AWS = require('aws-sdk');
const S3S = require('s3-streams');

function createOptions(event, options) {
    let endpoint = `.${event.endpoint}`;
    console.log(event.endpoint + " " + endpoint);
    if (endpoint == '.prod') { endpoint = ''; }
    const baseUrl = `https://api${endpoint}.sumologic.com`;
    console.log(baseUrl);
    return Object.assign(options, { 
        baseUrl: baseUrl,
        auth: event.accessId + ':' + event.accessKey,
        headers: { 
            'Content-Type': 'application/json',
            'Cookie': event.cookie
        },
        json: true,
    });
}

module.exports.start = async function(event) {
    assert(event.endpoint, 'Missing argument "endpoint"');
    assert(["prod", "us2", "au", "de", "eu", "jp"].includes(event.endpoint), `Unknown endpoint "${event.endpoint}"`);
    assert(event.accessId, 'Missing argument "accessId"');
    assert(event.accessKey, 'Missing argument "accessKey"');
    assert(event.s3Bucket, 'Missing argument "s3Bucket"');
    assert(event.s3KeyPrefix, 'Missing argument "s3KeyPrefix"');
    assert(event.query, 'Missing argument "query"');
    assert(event.to, 'Missing argument "to"');
    assert(event.from, 'Missing argument "from"');
    assert(event.timeZone, 'Missing argument "timeZone"');

    const options = createOptions(event, {
        method: 'POST',
        body: { 
            query: event.query, 
            from: event.from, 
            to: event.to, 
            timeZone: event.timeZone
        }
    });
    const response = await http('/api/v1/search/jobs', options);
    return {    
        endpoint: event.endpoint,
        accessId: event.accessId,
        accessKey: event.accessKey,
        s3Bucket: event.s3Bucket,
        s3KeyPrefix: event.s3KeyPrefix,
        cookie: response.headers['set-cookie'],
        id: response.body.id,
    };
};

module.exports.poll = async function(event) {
    assert(event.endpoint, 'Missing argument "endpoint"');
    assert(["prod", "us2", "au", "de", "eu", "jp"].includes(event.endpoint), `Unknown endpoint "${event.endpoint}"`);
    assert(event.accessId, 'Missing argument "accessId"');
    assert(event.accessKey, 'Missing argument "accessKey"');
    assert(event.s3Bucket, 'Missing argument "s3Bucket"');
    assert(event.s3KeyPrefix, 'Missing argument "s3KeyPrefix"');
    assert(event.cookie, 'Missing argument "cookie"');
    assert(event.id, 'Missing argument "id"');

    const options = createOptions(event, {});
    const response = await http('/api/v1/search/jobs/' + event.id, options);
    return {    
        endpoint: event.endpoint,
        accessId: event.accessId,
        accessKey: event.accessKey,
        s3Bucket: event.s3Bucket,
        s3KeyPrefix: event.s3KeyPrefix,
        cookie: response.headers['set-cookie'],
        id: event.id,
        state: response.body.state, 
        messageCount: response.body.messageCount,
        recordCount: response.body.recordCount,
        pendingWarnings: response.body.pendingWarnings,
        pendingErrors: response.body.pendingErrors
    };
};

module.exports.dump = async function(event) {
    assert(event.endpoint, 'Missing argument "endpoint"');
    assert(["prod", "us2", "au", "de", "eu", "jp"].includes(event.endpoint), `Unknown endpoint "${event.endpoint}"`);
    assert(event.accessId, 'Missing argument "accessId"');
    assert(event.accessKey, 'Missing argument "accessKey"');
    assert(event.s3Bucket, 'Missing argument "s3Bucket"');
    assert(event.s3KeyPrefix, 'Missing argument "s3KeyPrefix"');
    assert(event.cookie, 'Missing argument "cookie"');
    assert(event.id, 'Missing argument "id"');
    assert(event.messageCount, 'Missing argument "messageCount"');

    const bucket = event.s3Bucket;
    const messagesKey = event.s3KeyPrefix + event.id + '_messages'
    const messagesStream = S3S.WriteStream(new AWS.S3(), {
        'Bucket': bucket,
        'Key': messagesKey
    });
    // Imminent insanity
    const fin = new Promise((resolve, reject) => {
        console.log("Registering error and finish handlers for messagesStream");
        messagesStream.on('error', function(err) {
            console.log("Error!");
            console.log(err);
            reject(err);
        }).on('finish', function () {
            const messagesPath = 's3://' + bucket + '/' + messagesKey;
            console.log(`S3 upload finished. Path: ${messagesPath}`);
            resolve({ messagesPath: messagesPath });
        });
    });
    const options = createOptions(event, {});
    const maxLimit = 100;
    const messageCount = event.messageCount;
    let totalDataSize = 0
    for (var offset = 0; offset < messageCount; offset += maxLimit) {
        const limit = Math.min(maxLimit, messageCount - offset);
        const path = '/api/v1/search/jobs/' + event.id + '/messages?offset=' + offset + '&limit=' + limit;
        const response = await http(path, options);
        const output = JSON.stringify(response.body);
        totalDataSize += output.length;
        console.log("Got data for " + offset + " " + limit + " total data now " + totalDataSize);
        messagesStream.write(output);               
    };
    messagesStream.end();
    return await fin;
};
