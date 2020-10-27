import boto3
import os
import logging
import json

BUCKET = os.environ['BUCKETNAME']
COLLECTION_ID = os.environ['REKOGNITIONCOLLECTION']
FACE_MATCH_THRESHOLD = int(os.environ['REKOGNITIONFACEMATCHTHRESHOLD'])
LOG_LEVEL = logging.DEBUG

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition')
region = os.environ['AWS_REGION'] 
missing_person_table = os.environ['PersonData']
dynamodb = boto3.client('dynamodb',region)

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

# image name :- mugshot_kusu.png
def lambda_handler(event, context):
    try:
        logger.info("Parsing request data.")
        data = json.loads(event['body'])
        image = data['image']
        missing_person_data = data['missingpersondata']
        logger.info("Indexing faces.")
        
        # before we add the image to index check if the face is already registered in rekognition
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


        logger.info("After Searching in rekognition")

        if len(response['FaceMatches']) == 0:
            logger.info("Face Not Found, so it is a register case")
            # if the face does not exist already then we add that face to the index collection
            response = registerFaceInRekognitionCollection(image, response, missing_person_data)
            logger.info("return register response")
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Content-Type": "application/json"
                },
                "body": json.dumps({
                    "message": "Success. Face recorded"
                }),
            }
        else: # face already is registered so get the already registered details
            # in either case we will get a face id 
            logger.info("Face already exists")
            message = searchExistingDataByFaceId(response)
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Content-Type": "application/json"
                },
                "body": json.dumps({
                    "message": message
                }),
            }
    except Exception as e:
        logger.error(e)
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

def searchExistingDataByFaceId(response):
    logger.info("inside searchExistingDataByFaceId")
    faceId = response['FaceMatches'][0]['Face']['FaceId']
    logger.info("Recorded faceId: {}".format(faceId))
    faceIdResponse = findPersonDataByFaceId(faceId)
    # save the attributes in the person missing data
    if(faceIdResponse['Item']):
        logger.info("fetch db item record")
        payload=faceIdResponse['Item']
        firstname = [*payload['firstname'].values()]
        lastname=[*payload['lastname'].values()]
        message="Face already registered under faceid="+faceId+", firstname="+firstname[0]+",lastname="+lastname[0]
    return message

def registerFaceInRekognitionCollection(image, response, missing_person_data):
    # if the face does not exist already then we add that face to the index collection
    response = rekognition.index_faces(
                Image={
                    "S3Object": {
                        "Bucket": BUCKET,
                        "Name": image
                    }
                },
                CollectionId=COLLECTION_ID
            )
    logger.info("After index in the Rekognition")

    if response['ResponseMetadata']['HTTPStatusCode'] != 200 or len(response['FaceRecords']) == 0:
        raise Exception("Fail to register a face to Rekognition.")
    # new person data create record flow
    faceId = response['FaceRecords'][0]['Face']['FaceId']
    logger.info("Recorded faceId: {}".format(faceId))
    # save the attributes in the person missing data
    logger.info("Saving in the Dynamodb starts")
    saveMissingPersonData(missing_person_data,faceId)
    logger.info("Saving in the Dynamodb done")
    return response

# This method saves the missing person data in Dynamodb
def saveMissingPersonData(missing_person_data, faceId):
    logger.info("Inside saveMissingPersonData")
    response = dynamodb.put_item (
                    TableName=missing_person_table,
                     Item = {
                                "faceid":{"S":faceId}, # face id is the partition key
                                "firstname": {"S":missing_person_data['firstname']},
                                "lastname": {"S":missing_person_data['lastname']},
                                "dateofbirth": {"S":missing_person_data['dateofbirth']},
                                "missingfromlocation": {"S":missing_person_data['missingfromlocation']},
                                "age": {"S":str(missing_person_data['age'])},
                                "familycontactphone": {"S":str(missing_person_data['familycontactphone'])},
                                "reportingcentrecontact": {"S":missing_person_data['reportingcentrecontact']},
                                "dateofreport":{"S":missing_person_data['dateofreport']}
                            }
                )
def findPersonDataByFaceId(faceId):
    logger.info("findPersonDataByFaceId starts")
    response = dynamodb.get_item (
                TableName=missing_person_table,
                    Key={
                        "faceid" :{"S":faceId}
                    }
            )
    logger.info("findPersonDataByFaceId ends")
    return response
