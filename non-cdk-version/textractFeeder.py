import logging
import boto3
import time
from boto3.dynamodb.types import TypeDeserializer
from botocore.config import Config
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute
import threading
from threading import Lock

logger = logging.getLogger(__name__)

config = Config(retries = {'max_attempts': 10,'mode': 'adaptive'})

_tracking_table = "s3ObjectNamesforTextract"
_input_bucket = "mybucket-testfiles-fewer"
_input_prefix = ""
_output_bucket = "mybucket-testfiles-fewer-out"

threadCountforTextractAPICall = 20 # Number of threads used to call Textract
dynamoDBMaxlistCount = 200 #  Max number of rows to pull at a time from DynamoDB and store in memory to be processed by threads

dbDynoSelect = f"SELECT objectName, bucketName FROM \"{_tracking_table}\" WHERE txJobId=?"
dbDynoUpdate = f"UPDATE \"{_tracking_table}\" SET txJobId=?, outputbucketName=? WHERE objectName=? AND bucketName=?"

deserializer = TypeDeserializer()

s3_client = boto3.client('s3', config=config)
ddb = boto3.client('dynamodb', config=config)
txract = boto3.client('textract', config=config)

"""Main method that creates the threads and keeps the processing moving along"""
def orchestrate():
    
    totalCount = 0

    fileList = FileNameObjectsThreadSafeList()
    
    """Fetch max rows from DynamoDB and Initialize in memory list"""
    results = getFilesToSendToTextractfromDynamoDB()
    totalCount += len(results)    

    print("Starting process of sending files to Textract")

    while len(results) > 0:
        for record in results: # put these records into our thread safe list
            fileList.append(record)    
        
        """create our threads for processing Textract"""
        threadsforTextractAPI = [threading.Thread(name="Thread - " + str(i), target=procestTextractFunction, args=(fileList,)) for i in range(threadCountforTextractAPICall)]    
        
        """start up threads for processing Textract"""
        for thread in threadsforTextractAPI:
            thread.start()

        """wait for threads to complete"""
        for thread in threadsforTextractAPI:
            thread.join()

        """Get more rows from DynamoDB and repeat until no more rows"""
        results = getFilesToSendToTextractfromDynamoDB()
        totalCount += len(results)
    
    print("Finished sending " + str(totalCount) + " files to Textract")



"""Method that calls Textract API and stores the Job ID"""
def procestTextractFunction(fileList):
    while fileList.length() > 0:
        try:
            record = fileList.pop()
            response = txract.start_document_text_detection(
                DocumentLocation={
                    'S3Object': {
                    'Bucket': record['bucketName'],
                    'Name': record['objectName']
                }},
                OutputConfig={
                    'S3Bucket': _output_bucket
                }
            )            
            
            """Update the DynamoDB table with the JobId of the Textract call"""
            ddb.execute_statement(Statement=dbDynoUpdate, Parameters=[{'S': response["JobId"]}, {'S': _output_bucket}, {'S': record['objectName']}, {'S': record['bucketName']}])
        
        except Exception as e:
            logger.error(e)
            print(e.response)
            print(record)
            """update the DynamoDB table with a -1 for the JobId and an empty string for the output bucket name Swallow exception and continue, a retry for this file will occur next time row is retrived from DynamoDB"""
            ddb.execute_statement(Statement=dbDynoUpdate, Parameters=[{'S': '-1'}, {'S': ''}, {'S': record['objectName']}, {'S': record['bucketName']}])

"""Enumarete over bucket objects and put into DynamoDB table"""
def fetchAllObjectsInBucketandStoreName():
    
    print("method - fetchAllObjectsInBucketandStoreName")
    logger.info(f"started - {time.perf_counter()}")
    s3_profile = S3Profile(_input_bucket, _input_prefix)
    counter = 0
    for item in s3_profile.profile_object_list:
        DocumentObjStatusModel(
            objectName=item['fileObj'],
            bucketName=_input_bucket,
            createDate=round(time.time() * 1000),
            outputbucketName='',
            txJobId='').save()
        counter += 1

    logger.info(f"finished - {time.perf_counter()}")
    print("Populated " + str(counter) + " rows in DynamoDB table " + _tracking_table)


"""select rows from DyanmoDB table"""
def getFilesToSendToTextractfromDynamoDB():

    results = []

    """Query DynamoDB Table for object names and stop when reached max count"""
    try:
        ddbresponse = ddb.execute_statement(Statement=dbDynoSelect, Limit=500, Parameters=[{'S': ""}])                                         
        while 'NextToken' in ddbresponse and len(results) < dynamoDBMaxlistCount:
            for record in ddbresponse['Items']:
                deserialized_document = {k: deserializer.deserialize(v) for k, v in record.items()}
                results.append(deserialized_document)
                if len(results) == dynamoDBMaxlistCount:
                    break
            ddbresponse = ddb.execute_statement(Statement=dbDynoSelect, Limit=500, NextToken=ddbresponse['NextToken'], Parameters=[{'S': ""}]) 
        if (len(results) < dynamoDBMaxlistCount):
            for record in ddbresponse['Items']:
                deserialized_document = {k: deserializer.deserialize(v) for k, v in record.items()}
                results.append(deserialized_document)
                if len(results) == dynamoDBMaxlistCount:
                    break
        
        return results

    except Exception as e:
        logger.error(e)
        return 1


"""Create DynamoDB table if it does not exist"""
def createDynamoDB():
    if not DocumentObjStatusModel.exists():
        print("creating DynamoDB table " + _tracking_table + " in " + boto3.Session().region_name + " for tracking")
        # create the table, wait for it to finish creating, then return the table resource
        
        DocumentObjStatusModel.create_table(read_capacity_units=150,
                                         write_capacity_units=150,
                                         wait=True)
        print("DynamoDB table " + _tracking_table + " created")


"""custom class, wrapping a list in order to make it thread safe""" 
class FileNameObjectsThreadSafeList():
    def __init__(self):
        self._list = list()
        self._lock = Lock()

    def append(self, value):
        with self._lock:
            self._list.append(value)

    def pop(self):
        with self._lock:
            response = self._list.pop()
            return response
    
    def length(self):
        with self._lock:
            return len(self._list)

"""DynamoDB Class"""
class DocumentObjStatusModel(Model):

    class Meta:
        table_name = _tracking_table
        region = boto3.Session().region_name

    objectName = UnicodeAttribute(hash_key=True)
    bucketName = UnicodeAttribute(null=True)
    createDate = NumberAttribute(null=True)
    txJobId = UnicodeAttribute(null=True)
    outputbucketName = UnicodeAttribute(null=True)

"""S3 Class for retrieving Object Names"""
class S3Profile:
    def __init__(self, bucketName, prefixName):
        self.bucketName = bucketName
        self.prefixName = prefixName
        self.s3_client = boto3.client('s3')
        self.profile_object_list = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucketName, Prefix=self.prefixName)
        try:
            for page in pages:
                for content in page.get('Contents'):
                    file = content.get('Key')
                    self.profile_object_list.append({"fileObj": file})
        except Exception as e:
            print("Error, most likely caused by either invalid bucket name or invalid prefix name")
            logger.error(e)




"""Main entry point into script --- Start Here"""
if __name__ == "__main__":    
    now = time.perf_counter()
    print("started")

    """created DynamoDB table for tracking object names"""
    createDynamoDB()

    """Fetch all object names in bucket and upsert rows into DynamoDB"""
    fetchAllObjectsInBucketandStoreName()

    """Start the process of sending files to Textract using multiple threads"""
    orchestrate()
    
    print(f"completed in - {time.perf_counter()-now} seconds")
