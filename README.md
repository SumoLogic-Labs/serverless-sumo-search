Work in progress:
- ~~Doesn't actually create proper CSV output~~
- API frontend
- Deal with records also (aggregation queries)
- Should send SNS notification at the end
- Error handling
- Tests!!!

In the meantime:

```
npm install -g serverless
git clone https://github.com/SumoLogic/serverless-sumo-search-query.git
cd serverless-sumo-search-query
npm install # Ignore warnings
serverless deploy
serverless invoke stepf -n sumosearch --data \
'{"endpoint": "[prod|us2|...]", "accessId": "[sumo-access-id]", "accessKey": "[sumo-access-key]", "query": "error | count", "from": "2019-01-19T16:00:00", "to": "2019-01-19T16:01:00", "timeZone": "Asia/Kolkata", "messages": true, "records": false, "s3Bucket": "[your-bucket]", "s3KeyPrefix": "[your-prefix]"}'
```

Note: Need to have AWS credentials in `~/.aws/credentials` under profile `s1ss_sls` 
(or change profile name in `serverless.yml`).
