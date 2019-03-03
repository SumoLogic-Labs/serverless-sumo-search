function d(strings) {
    let result = new Date().toISOString() + " ";
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

function debug(message) {
    console.log(debug.caller.name + " " + message);
}

function foofun() {
    let foo = 1;
    let schnitzel = {mit: "sauce", borkdrive: 3000};
    debug(d`Hello: ${foo} schnick ${schnitzel} schnak`);
}

foofun();
console.log(Date.now());

const Ajv = require('ajv');
const schema = {
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
        'messages', 'records', 's3Bucket']
};
const schema2 = {
    $merge: {
        source: {...schema}
    },
    with: {
        properties: {
            foo: {type: 'string'}
        }
    }
};
console.log(JSON.stringify(schema2));
const data = {
    "endpoint": "jp",
    "accessId": "suPa1yFuncZOdh",
    "accessKey": "f5KaKpTae8IzDWRvDXBXdUVjXBmxIeVr3Ulv1go9nsb0FZgc7DfMnrSjru4HT8Cw",
    "query": "error | count _sourcecategory",
    "from": "2019-02-19T00:00:00",
    "to": "2019-02-19T00:00:01",
    "timeZone": "America/Los_Angeles",
    "messages": true,
    "records": true,
    "s3Bucket": "raychaser-long-us-west-2-encrypted",
    "s3KeyPrefix": "s1ss/s1ss_sls02_"
};
var ajv = new Ajv();
var valid = ajv.validate(schema, data);
if (!valid) {
    console.log(`validateSchema - ${JSON.stringify(ajv.errors)}`);
}

