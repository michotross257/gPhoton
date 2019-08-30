import os
import boto3
from datetime import datetime, timezone

def lambda_handler(event, context):
    # before anything else, let's get the current time
    end_time = datetime.now(timezone.utc).isoformat(timespec='microseconds')
    
    ATHENA_COST_PER_BYTE = 5/1e+12 # $5/TB scanned
    athena_table_id = os.environ.get('ATHENA_TABLE')
    profile_name = os.environ.get('PROFILE_NAME')
    region_name = os.environ.get('REGION_NAME')
    athena_client = boto3.client('athena')
    db_client = boto3.client('dynamodb')
    
    key = event['Records'][0]['s3']['object']['key']
    execution_id = key.split('/')[-1].replace('.csv', '')
    
    response = athena_client.get_query_execution(
        QueryExecutionId=execution_id
    )
    bytes_scanned = int(response['QueryExecution']['Statistics']['DataScannedInBytes'])
    start_time = db_client.get_item(
        TableName=athena_table_id,
        Key={
            'EXECUTION_ID':{
                'S': execution_id
            }
        },
        AttributesToGet=[
            'EXECUTION_START_TIME'
        ],
    )
    start_time = start_time['Item']['EXECUTION_START_TIME']['S']
    time_difference = datetime.fromisoformat(end_time) - datetime.fromisoformat(start_time)
    time_difference = time_difference.seconds + (time_difference.microseconds*1e-6)
    db_client.update_item(
        TableName=athena_table_id,
        Key={
            'EXECUTION_ID':{
                'S': execution_id
            }
        },
        UpdateExpression='SET EXECUTION_END_TIME=:et, QUERY_COST=:cost, DATA_SCANNED=:data, TIME_ELAPSED_IN_SECONDS=:tm',
        ExpressionAttributeValues={
            ':et': {
                'S': end_time
            },
            ':cost': {
                'S': '${:.10f}'.format(bytes_scanned*ATHENA_COST_PER_BYTE)
            },
            ':data': {
                'S': '{:.10f} GB'.format(bytes_scanned*1e-9)
            },
            ':tm': {
                'S': '{:.6f}'.format(time_difference)
            }
        }
    )
