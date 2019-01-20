const https = require('https');

module.exports.start = async (event, context) => {

    return new Promise((resolve, reject) => {
        
        const body = JSON.stringify({ 
            query: event.query, 
            from: event.from, 
            to: event.to, 
            timeZone: event.timeZone
        })
        console.log(body);
            
        const options = {
            host: 'api.us2.sumologic.com',
            port: 443,
            path: '/api/v1/search/jobs',
            method: 'POST',
            headers: { 
                'Content-Type': 'application/json',
                'Authorization': 'Basic ' + Buffer.from(event.accessId + ':' + event.accessKey).toString('base64'),
            }
        };
        console.log(JSON.stringify(options));

        var result = "";
        const req = https.request(options, (res) => {
          res.on('data', data => result += data);
          res.on('end', () => {
              console.log(result);
              resolve({
                  id: JSON.parse(result).id,
                  cookie: res.headers['set-cookie'],
                  accessId: event.accessId,
                  accessKey: event.accessKey
              });
          })
        }).on('error', err => reject(err.message));
        req.write(body);
        req.end();
    });
};

module.exports.poll = async (event, context) => {

    return new Promise((resolve, reject) => {
        
        const options = {
            host: 'api.us2.sumologic.com',
            port: 443,
            path: '/api/v1/search/jobs/' + event.id,
            method: 'GET',
            headers: { 
                'Content-Type': 'application/json',
                'Cookie': event.cookie,
                'Authorization': 'Basic ' + Buffer.from(event.accessId + ':' + event.accessKey).toString('base64'),
            }
        };
        console.log(JSON.stringify(options));

        var result = "";
        const req = https.request(options, (res) => {
          res.on('data', data => result += data);
          res.on('end', () => {
              console.log(result);
              resolve({
                  state: JSON.parse(result).state, 
                  messageCount: JSON.parse(result).messageCount,
                  recordCount: JSON.parse(result).recordCount,
                  pendingWarnings: JSON.parse(result).pendingWarnings,
                  pendingErrors: JSON.parse(result).pendingErrors,
                  cookie: res.headers['set-cookie'],
                  accessId: event.accessId,
                  accessKey: event.accessKey
              });
          })
        }).on('error', err => reject(err.message));
        req.end();
    });
};

module.exports.end = async (event) => {
    console.log(JSON.stringify(event));
    const response = {
        statusCode: 200,
        body: JSON.stringify(event),
    };
    return response;
};
