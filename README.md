# Lambda-JsonLogs_FilterTo_DynamoDB

Ingests json.gz logs from S3 then filters and maps records to attributes in DynamoDB table. 

Designed as PoC for log filtering. Requires mapping json dict to attributes. 
Built batchwriter method over using batch_writer() to track response/consumed capacity.

Workflow initiated by new log-file S3 event --> SQS --> Lambda (set number of logs to consume) --> DynamoDB Table.


