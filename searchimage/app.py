import boto3
import os
import logging
import json


BUCKET = os.environ['BUCKETNAME']
COLLECTION_ID = os.environ['REKOGNITIONCOLLECTION']
FACE_MATCH_THRESHOLD = int(os.environ['REKOGNITIONFACEMATCHTHRESHOLD'])
LOG_LEVEL = logging.DEBUG


s3 = boto3.client('s3')
sns=boto3.client('sns')
rekognition = boto3.client('rekognition')
region = os.environ['AWS_REGION'] 
missing_person_table = os.environ['PersonData']
sns_topic = os.environ['SnsTopic']
dynamodb = boto3.client('dynamodb',region)
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

def lambda_handler(event, context):
    try:
        logger.info("Parsing request data.")
        data = json.loads(event['body'])
        image = data['image']
        
        
        logger.info("Searching faces.")
        response = rekognition.search_faces_by_image(
            Image={
                "S3Object": {
                    "Bucket": BUCKET,
                    "Name": image
                }
            },
            CollectionId=COLLECTION_ID,
            FaceMatchThreshold=FACE_MATCH_THRESHOLD,
            MaxFaces=1
        )
    
        logger.info("Getting results.")
        if response['ResponseMetadata']['HTTPStatusCode'] != 200 or len(response['FaceMatches']) == 0:
            raise Exception("Fail to find similar face")
    
        faceFound = response['FaceMatches'][0]
        faceId = faceFound['Face']['FaceId']
        confidence = faceFound['Similarity']
        logger.info("Found faceId: {}. Confidence level: {}".format(faceId, confidence))

        db_response=findPersonDataByFaceId(faceId)
        
        payload=db_response['Item']
        firstname = [*payload['firstname'].values()]
        lastname=[*payload['lastname'].values()]
        lastseendate=[*payload['dateofreport'].values()]
        lastseenlocation=[*payload['missingfromlocation'].values()]
        contactcentreemail=[*payload['reportingcentrecontact'].values()]
        message = firstname[0]+","+lastname[0]+" , missing since="+lastseendate[0]+", from location="+lastseenlocation[0]
        
        logger.info("Sending Email Notification")
        
        sns.publish(
            TopicArn=sns_topic,
            Message=message
        )

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "message": "Succeed to find="+message,
                "faceId": faceId,
                "confidence": confidence
            }),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "message": "Error: {}".format(e),
            }),
        }

def findPersonDataByFaceId(faceId):
    response = dynamodb.get_item (
                TableName=missing_person_table,
                    Key={
                        "faceid" :{"S":faceId}
                    }
            )
    return response