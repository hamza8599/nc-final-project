import json
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Event received:")
    logger.info(json.dumps(event, indent=2))
    return {
        'statusCode': 200,
        'body': json.dumps('Event processed successfully')
    }