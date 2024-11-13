import json
import logging
import boto3
import random

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# client = boto3.client('sns')

# # Variables for the SNS:
# SNS_TOPIC_ARN = "arn:aws:sns:eu-west-2:137068251264:ingestion-topic"


def lambda_handler(event, context):
    # logger.error("SNS practice")
    # response = client.publish(
    #         TopicArn = SNS_TOPIC_ARN,
    #         Message = "We failed"
    # )
    # logger.info(f'Message {response}')
    # logger.info("Event recieved")
    # logger.info(json.dumps(event, indent=2))
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Event processed successfully')
    # }
    information = ['test', 'Event recieved']
    i = random.randint(0,1)
    logger.info(information[i])
    logger.info(json.dumps(event, indent=2))
    return {
        'statusCode': 200,
        'body': json.dumps('Event processed successfully')
    }
    