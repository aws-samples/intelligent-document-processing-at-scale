
from typing import Tuple
import os 
import logging
import json
import datetime

from utils.analyze_textract import AnalyzeTextract
import boto3
s3 = boto3.client('s3')

logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logger.setLevel(log_level)

version = "0.0.1"

def split_s3_path_to_bucket_and_key(s3_path: str) -> Tuple[str, str]:
    if len(s3_path) <= 7 or not s3_path.lower().startswith("s3://"):
        raise ValueError(
            f"s3_path: {s3_path} is no s3_path in the form of s3://bucket/key."
        )
    s3_bucket, s3_key = s3_path.replace("s3://", "").split("/", 1)
    return (s3_bucket, s3_key)
        

def get_file_from_s3(s3_path: str, range=None) -> bytes:
    s3_bucket, s3_key = split_s3_path_to_bucket_and_key(s3_path)
    if range:
        o = s3.get_object(Bucket=s3_bucket, Key=s3_key, Range=range)
    else:
        o = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    return o.get('Body').read()

def lambda_handler(event, _):
    logger.setLevel('INFO')
    logger.info(f"version: {version}")
    logger.info("Starting Textract Analysis")
    
    timestamp = datetime.datetime.now().astimezone().replace(
                microsecond=0).isoformat()
    date = datetime.datetime.today().strftime('%Y-%m-%d')
    logger.info(json.dumps(event))

    if event.get('Payload').get('Payload').get("textract_result"):
        textract_result = event.get('Payload').get('Payload').get("textract_result")
    elif event.get('Payload').get("textract_result"):
        textract_result = event.get('Payload').get("textract_result")
    else:
        textract_result = event.get("textract_result")
    
    
    logger.info(f"Get Textract JSON {textract_result.get('TextractOutputJsonPath')}")
    textract_s3_byte = get_file_from_s3(textract_result.get('TextractOutputJsonPath'))
    s3_bucket, s3_key = split_s3_path_to_bucket_and_key(textract_result.get('TextractOutputJsonPath'))
    file_name = s3_key.split('/')[-1].split('.')[0]    
    logger.info("Reading the JSON")    
    textract_json = json.loads(textract_s3_byte)
    
    logger.info("Analyzing JSON")
    analysis = AnalyzeTextract(textract_json=textract_json)
    
    json_analysis = analysis.metrics_to_json()
    analytics_outputKey = f"analytics_output_{date}/{timestamp}_{file_name}_analytics.json"
    
    logger.info("Finished Analyzing")
    logger.info(f"Writing to {analytics_outputKey}")
    s3.put_object(Body=json_analysis, Bucket=s3_bucket, Key=analytics_outputKey)
    
    textract_result["TextractAnalyticsOutputPath"] = f"s3://{s3_bucket}/{analytics_outputKey}"
        
    return event
