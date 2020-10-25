
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
missing_person_table = os.environ['PersonData']
dynamodb = boto3.client('dynamodb',region)

def lambda_handler(event, context):
    unmatchcounter=0
    try:
        logger.info("Parsing request data.")
        for record in event['Records']:
            #Kinesis data is base64 encoded so decode here
            payload=base64.b64decode(record["kinesis"]["data"])
            logger.info("Getting payload")
            data=json.loads(payload)
            logger.info("Json Data success=")
            matchedFace=data['FaceSearchResponse'][0]['MatchedFaces']
            if (len(matchedFace) == 0):
                logger.info("No match found")
                unmatchcounter=unmatchcounter+1
                logger.info('no matching faceId found')
                sns.publish(
                    TopicArn=sns_topic,
                    Message="Unknown person count="+unmatchcounter
                    )
            else:
                logger.info("Match Found")
                faceId = matchedFace[0]['Face']['FaceId']
                logger.info("Face Id returned="+faceId)
                db_response=findPersonDataByFaceId(faceId)
                logger.info("After Db response")
                payload=db_response['Item']
                logger.info("returned payload")
                if (len(payload) != 0):    
                    firstname = [*payload['firstname'].values()]
                    lastname=[*payload['lastname'].values()]
                    lastseendate=[*payload['dateofreport'].values()]
                    lastseenlocation=[*payload['missingfromlocation'].values()]
                    contactcentreemail=[*payload['reportingcentrecontact'].values()]
                    message = firstname[0]+","+lastname[0]+" , missing since="+lastseendate[0]+", from location="+lastseenlocation[0]
                    logger.info("Sending Email Notification="+message)
                    sns.publish(
                        TopicArn=sns_topic,
                        Message=message
                        )
    except Exception as e:
         logger.error(e)
         traceback.format_exc()


def findPersonDataByFaceId(faceId):
    logger.info("Get details from dynamodb for faceId="+faceId)
    response = dynamodb.get_item (
                TableName=missing_person_table,
                    Key={
                        "faceid" :{"S":faceId}
                    }
            )
    return response