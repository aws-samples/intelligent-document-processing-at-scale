import json
import logging
import os
from urllib.parse import unquote_plus
import textractmanifest as tm
from datetime import datetime
import re

import boto3

from datetime import timezone
logger = logging.getLogger(__name__)

step_functions_client = boto3.client(service_name='stepfunctions')
TRIGGER_TYPES = []


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    state_machine_arn = os.environ.get('STATE_MACHINE_ARN', None)
    if not state_machine_arn:
        raise Exception("no STATE_MACHINE_ARN set")
    logger.info(f"STATE_MACHINE_ARN: {state_machine_arn}")

    s3_bucket = ""
    s3_key = ""

    for record in event['Records']:
        event_source = record["eventSource"]
        if event_source == "aws:s3":
            s3_bucket = record['s3']['bucket']['name']
            s3_key = unquote_plus(record['s3']['object']['key'])
        elif event_source == "aws:sqs":
            message = json.loads(record["body"])
            s3_bucket = message['bucket']
            s3_key = message['key']
        else:
            logger.error(f'unsupported event_source: {event_source}')

        filename = os.path.basename(s3_key) + datetime.now(timezone.utc).isoformat()

        filename = re.sub(r'[^A-Za-z0-9-_]', '', filename)
        filename = filename[:80]
        if not s3_bucket or not s3_key:
            raise ValueError(
                f"no s3_bucket: {s3_bucket} and/or s3_key: {s3_key} given.")
        manifest: tm.IDPManifest = tm.IDPManifest()
        manifest.s3_path = f"s3://{s3_bucket}/{s3_key}"        
        logger.debug(f"manifest: {tm.IDPManifestSchema().dumps(manifest)}")

        response = step_functions_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=filename,
            input=tm.IDPManifestSchema().dumps(manifest))
        logger.info(response)
