import os
import boto3
from datetime import datetime, timezone

def lambda_handler(event, context):
    # before anything else, let's get the current time
    end_time = datetime.now(timezone.utc).isoformat(timespec='microseconds')
    
    athena_table_id = os.environ.get('ATHENA_TABLE')
    profile_name = os.environ.get('PROFILE_NAME')
    region_name = os.environ.get('REGION_NAME')
    client = boto3.client('dynamodb')
    
    key = event['Records'][0]['s3']['object']['key']
    execution_id = key.split('/')[-1].replace('.csv', '')
    client.update_item(
        TableName=athena_table_id,
        Key={
            'EXECUTION_ID':{
                    'S': execution_id
            }
        },
        UpdateExpression='SET EXECUTION_END_TIME = :et',
        ExpressionAttributeValues={
            ':et': {
                'S': end_time
            }
        }
    )
