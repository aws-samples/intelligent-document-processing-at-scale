import logging
import boto3
import json
import time
import threading
from botocore.config import Config
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute


logger = logging.getLogger(__name__)

config = Config(retries = {'max_attempts': 10,'mode': 'adaptive'})

dynamoDBMaxlistCount = 200 #  Max number of rows to pull at a time from DynamoDB and store in memory

_tracking_table = "s3ObjectNamesforTextract" # name of DynamoDB table used to track objects that have been sent to Textract
_textractFolder = "textract_output/" # name of folder that Textract created when it wrote results out to S3

threadCountforTextExtract = 50 # Number of threads used to call Textract

def orchestrateTextExtraction():
    print("Convert JSON from Textract into blobs of Text and save to S3")
    totalDocs = 0
    try:
        while True:
            txtThreads = []
            threadCounter = 0
            objRows = DocumentObjStatusModel.scan(DocumentObjStatusModel.outputTextObjName.does_not_exist(), limit=dynamoDBMaxlistCount)
            exitLoop = True
            for objRow in objRows:
                exitLoop = False
                if (len(objRow.outputTextObjName)==0 and len(objRow.txJobId) > 2):
                    txtThreads.append(threading.Thread(name="Thread - " + str(threadCounter), target=extractText, args=(objRow, create_s3_client(),)))
                    threadCounter+=1
                    totalDocs+=1
                    if threadCounter == threadCountforTextExtract:
                        for thread in txtThreads:
                            thread.start()
                        for thread in txtThreads:
                            thread.join()
                        threadCounter = 0
                        txtThreads.clear()

            if len(txtThreads) > 0:
                for thread in txtThreads:
                    thread.start()
                for thread in txtThreads:
                    thread.join()
            
            if exitLoop: 
                print (f"Total text documents created {totalDocs}")
                break

    except Exception as e:
        logger.error(e)
        print ("Unable to run script")


def extractText(objRow, s3):
    
    outPutFolder = _textractFolder + objRow.txJobId
    outPutTextObjName = outPutFolder
    objRow.outputTextObjName = outPutTextObjName

    txJSONoutputFiles = []
    s3_profile = S3Profile(objRow.outputbucketName , objRow.outputTextObjName)
    try:
        for item in s3_profile.profile_object_list:
            if item['fileObj'].split("/")[-1].isnumeric():
                txJSONoutputFiles.append(item['fileObj'])

        txJSONoutputFiles.sort()
        fileText = ""
        for file in txJSONoutputFiles:
            jsonContent = json.loads(s3.get_object(Bucket=objRow.outputbucketName, Key=file)['Body'].read().decode('utf-8'))
            for block in jsonContent["Blocks"]:
                if block["BlockType"] == "LINE":
                    fileText += block["Text"] + "\n"    

        objRow.outputTextObjName = objRow.outputTextObjName + "/" + objRow.objectName + ".txt"
        #print(objRow.outputTextObjName)
    except Exception as e:
        logger.error(e + "Unable to parse text from JSON " + objRow.objectName)
        print ("Unable to parse text from JSON " + objRow.objectName)      
        objRow.outputTextObjName = "-1"


    try:
        # write fileText content out to S3
        s3.put_object(
            Body=fileText, 
            Bucket=objRow.outputbucketName, 
            Key=objRow.outputTextObjName
        )
    except Exception as e:
        logger.error(e)
        print("Unable to write to S3")

    
    try:
        # update DynamoDB table with text file info
        objRow.save()   
    except Exception as e:
        logger.error(e)
        print("Unable to update DynamoDB table")



def create_s3_client():
    return boto3.client('s3', config=config)

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
    outputTextObjName = UnicodeAttribute(null=True, default="")

"""S3 Class for retrieving Object Names"""
class S3Profile:
    def __init__(self, bucketName, prefixName):
        self.bucketName = bucketName
        self.prefixName = prefixName
        self.s3_client = boto3.client('s3')
        self.profile_object_list = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucketName, Prefix=self.prefixName)
        for page in pages:
            for content in page.get('Contents'):
                file = content.get('Key')
                self.profile_object_list.append({"fileObj": file})

"""Main entry point into script --- Start Here"""
if __name__ == "__main__":   
    now = time.perf_counter()
    print("started")   
    
    orchestrateTextExtraction()
    
    print(f"completed in - {time.perf_counter()-now} seconds")