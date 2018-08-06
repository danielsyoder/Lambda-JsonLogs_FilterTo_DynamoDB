import gzip
import boto3
import json

client = boto3.client('dynamodb')
s3 = boto3.client('s3')
table = 'outputLogTable' #DynamoDB table to aggregate filtered logs
filter = "substring" #sub-string to filter out logs
#Configure ddbObj to map log vars to table attributes (line 61)

def writeBatch(records):
	#BatchLoad Items to DynamoDB
	#Created instead of batch_writer() to return response & consumed capacity
	#TODO test "UnprocessedItems"
	recCount = len(records)
	index = 0
	if recCount == 0:
		print("No records in batch")
		return True
	while index <= recCount:
		if (index+25)>recCount: recDict = {table:records[index:]}
		else: recDict = {table:records[index:(index+25)]}
		response = client.batch_write_item(RequestItems=recDict, ReturnConsumedCapacity='INDEXES')
		print("Batch response: ",response)
		if len(response['UnprocessedItems']) > 0:
			try: writeBatch(response['UnprocessedItems'][table])
			except Exception as e:raise e
		index += 25

def addRecord_DeDup(rec, recordList):
	#Check for duplicates and add record to batch list
	if recordList == None: return [rec]
	else:
		for item in recordList:
			if rec == item: return recordList
		recordList.append(rec)
		return recordList

def lambda_handler(event, context):
  #Ingest list of log files then send filtered/mapped records to DynamoDB
  
	recJsonList = []
	for record in event['Records']:
		try:
			#Read SQS Message for log file source
			recDict = json.loads(record['body'])
			bucket = recDict['Records'][0]['s3']['bucket']['name']
			key = recDict['Records'][0]['s3']['object']['key']
			print(key)

			#Download log file from S3
			download_path = '/tmp/recordDL.json.gz'
			s3.download_file(Bucket = bucket,Key = key,Filename = download_path)

			#Read file, filter out records, map JSON to DDB item, compile DDB objects
			with gzip.open(download_path,mode = 'rt') as records:
				for record in records:
					if filter not in record:
						recordDict = json.loads(record)
						requestPayload = str(recordDict['requestParameters'])
						ddbObj = {"PutRequest": {
								"Item":{
								"uid": {"S": recordDict["userIdentity"]["accountId"]},
								"time_or_uidmeta": {"S": recordDict["eventTime"]},
								"requestType": {"S": recordDict['eventName']},
								"requestDict":{"S": requestPayload}
								}
							}
							}
						recJsonList = addRecord_DeDup(ddbObj,recJsonList)

		except KeyError as e:
			print(e)

	#Send items to DynamoDB for batch write
	writeBatch(recJsonList)
