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
    
    db_client.update_item(
        TableName=athena_table_id,
        Key={
            'EXECUTION_ID':{
                    'S': execution_id
            }
        },
        UpdateExpression='SET EXECUTION_END_TIME=:et, QUERY_COST=:cost, DATA_SCANNED=:data',
        ExpressionAttributeValues={
            ':et': {
                'S': end_time
            },
            ':cost': {
                'S': '${:.10f}'.format(bytes_scanned*ATHENA_COST_PER_BYTE)
            },
            ':data': {
                'S': '{:.10f} GB'.format(bytes_scanned*1e-9)
            }
        }
    )
