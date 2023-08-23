
from typing import Tuple
import os 
import logging
import json
import datetime

from utils.format_ocr_text import FormatOCR
import boto3
s3 = boto3.client('s3')


logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
s3_txt_output_prefix = os.environ.get('OUTPUT_PREFIX')
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
    """
    Retrieves a file from S3.

    Args:
        s3_path (str): The path of the file in S3.
        range (str, optional): The range of bytes to retrieve from the file. Defaults to None.

    Returns:
        bytes: The content of the file.

    Raises:
        Exception: If there is an error retrieving the file.

    Examples:
        >>> get_file_from_s3('s3://my-bucket/my-file.txt')
        b'File content'
        
        >>> get_file_from_s3('s3://my-bucket/my-file.txt', 'bytes=0-100')
        b'Partial file content'
    """
    s3_bucket, s3_key = split_s3_path_to_bucket_and_key(s3_path)
    if range:
        o = s3.get_object(Bucket=s3_bucket, Key=s3_key, Range=range)
    else:
        o = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    return o.get('Body').read()

def lambda_handler(event, _):
    logger.setLevel('INFO')
    logger.info(f"version: {version}")
    
    timestamp = datetime.datetime.now().astimezone().replace(
                microsecond=0).isoformat()
    logger.info(json.dumps(event))
    
    if event.get('Payload').get("textract_result"):
        textract_result = event.get('Payload').get("textract_result")
    else:
        textract_result = event.get("textract_result")
    
    logger.info(f"Get Textract JSON {textract_result.get('TextractOutputJsonPath')}")
    textract_s3_byte = get_file_from_s3(textract_result.get('TextractOutputJsonPath'))
    s3_bucket, s3_key = split_s3_path_to_bucket_and_key(textract_result.get('TextractOutputJsonPath'))
    file_name = s3_key.split('/')[-1].split('.')[0]    
    logger.info("Reading the JSON")
    
    textract_json = json.loads(textract_s3_byte)
    
    logger.info("Formatting OCR")
    formatter = FormatOCR(j=textract_json)
    
    full_text, lines = formatter.json_to_text()
    txt_outputKey = f"{s3_txt_output_prefix}/{timestamp}_{file_name}_txt.txt"
    
    logger.info(f"Writing to {txt_outputKey}")
    s3.put_object(Body=full_text, Bucket=s3_bucket, Key=txt_outputKey)
   
    textract_result["TextractOutputTextPath"] = f"s3://{s3_bucket}/{txt_outputKey}"
    
    return event
