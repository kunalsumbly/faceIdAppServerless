
import boto3
import os
import logging
import json
import base64
import traceback

LOG_LEVEL = logging.DEBUG
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
sns=boto3.client('sns')
sns_topic = os.environ['SnsTopic']
region = os.environ['AWS_REGION'] 

def lambda_handler(event, context):
    unmatchcounter=0
    try:
        logger.info("Parsing request data.")
        for record in event['Records']:
            #Kinesis data is base64 encoded so decode here
            logger.info("Getting payload")
            payload=base64.b64decode(record["kinesis"]["data"])
            logger.info("After payload, getting json data=")
            data=json.loads(payload)
            logger.info("Json Data success=")
            matchedFace=data['FaceSearchResponse'][0]['MatchedFaces']
            if (len(matchedFace) == 0):
                unmatchcounter=unmatchcounter+1
            else:
                faceId = matchedFace[0]['Face']['FaceId']
                if (faceId == '132f5dfd-21dc-4e95-801c-358bbb81d3a4'):    
                    logger.info("Sending Email Notification")
                    sns.publish(
                        TopicArn=sns_topic,
                        Message="Match found for faceId 132f5dfd-21dc-4e95-801c-358bbb81d3a4"
                        )

                else:
                    logger.info('no matching faceId found')
                    sns.publish(
                        TopicArn=sns_topic,
                        Message="Unknown person count="+unmatchcounter
                        )
    except Exception as e:
         logger.error(e)
         traceback.format_exc()
