import boto3
import json
from functools import reduce
from collections import OrderedDict
import time
from datetime import datetime, timezone
import argparse

parser = argparse.ArgumentParser(description='Get queries, AWS credentials, and Athena, DynamoDB, and S3 info.')
parser.add_argument('-p', '--profile', default='default', metavar='',
                    help='Name of the AWS profile to use (default="default").')
parser.add_argument('region', help='Name of AWS region.')
parser.add_argument('database', help='Name of Athena Database containing the table to query.')
parser.add_argument('workgroup', help='Name of Athena workgroup.')
parser.add_argument('output_location', help='S3 location where to save query results.')
parser.add_argument('table', help='Name of the DynamoDB table to write to.')
parser.add_argument('-n', '--querycycles', default=2, type=int, metavar='',
                    help='Number of query cycles - i.e. number of times to run the queries')
parser.add_argument('-l', '--querylife', default=10, type=int, metavar='',
                    help='Maximum number of seconds query should be allowed to run before stopping/cancelling (default=10).')
parser.add_argument('-w', '--wait', default=1, type=int, metavar='',
                    help='Time to wait between checks to see if a query is still running (default=1).')
args = parser.parse_args()

if __name__ == '__main__':
    sess = boto3.Session(profile_name=args.profile,
                         region_name=args.region)
    athena_client = sess.client('athena')
    dynamodb_client = sess.client('dynamodb')
    
    queries = {
        "1":
        '''
        SELECT *
        from gphoton
        WHERE zoneID BETWEEN 10829 AND 10831
        LIMIT 1000000;
        ''',
        "2":
        '''
        SELECT *
        FROM gphoton
        WHERE zoneID BETWEEN 10829 AND 10831
        AND dec BETWEEN 0.24622267 AND 0.26288933000000003
        AND ra BETWEEN 323.05927358775426 AND 323.07594041224576
        AND (0.7993371853621515*cx + -0.6008663124164834*cy + 0.0044428257146426255*cz) > 0.9999999894230147
        AND time >= 740229107995 AND time < 1012464073985
        AND flag = 0;
        ''',
        "3":
        '''
        SELECT COUNT(*) AS item_count
        FROM gphoton
        WHERE zoneID BETWEEN 10829 AND 10831
        AND dec BETWEEN 0.24622267 AND 0.26288933000000003
        AND ra BETWEEN 323.05927358775426 AND 323.07594041224576
        AND (0.7993371853621515*cx + -0.6008663124164834*cy + 0.0044428257146426255*cz) > 0.9999999894230147
        AND time >= 740229107995 AND time < 1012464073985
        AND flag = 0;
        '''
    }
    
    
    for i in range(args.querycycles):
        msg = 'Query cycle {} of {}\n'.format(i+1, args.querycycles)
        print(msg + '-'*(len(msg)-1))
        for query in queries:
            response = athena_client.start_query_execution(
                    QueryString=queries[query],
                    QueryExecutionContext={
                        'Database': args.database
                    },
                    ResultConfiguration={
                        'OutputLocation': args.output_location
                    },
                    WorkGroup=args.workgroup
            )
            start_time = datetime.now(timezone.utc).isoformat(timespec='microseconds')
            item = {
                'EXECUTION_ID':
                    {"S": response['QueryExecutionId']},
                'QUERY_ID':
                    {"S": str(query)},
                'EXECUTION_START_TIME':
                    {"S": start_time}
            }
            dynamodb_client.put_item(TableName=args.table, Item=item)
            print('Query Execution ID: {}'.format(response['QueryExecutionId']))
            rsp = athena_client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
            succeeded_query = True if rsp['QueryExecution']['Status']['State'] == 'SUCCEEDED' else False
            num_sec_query_has_been_running = 0
            # check to see if the query has succeeded
            while not succeeded_query:
                if num_sec_query_has_been_running >= args.querylife:
                    print('QUERY CANCELLED: Query {} has been running for ~{} seconds.'.format(response['QueryExecutionId'],
                                                                                                num_sec_query_has_been_running))
                    _ = athena_client.stop_query_execution(QueryExecutionId=response['QueryExecutionId'])
                    break
                if num_sec_query_has_been_running % 60 == 0 and num_sec_query_has_been_running:
                    duration = int(num_sec_query_has_been_running/60)
                    word = 'minutes' if duration > 1 else 'minute'
                    print('...Query has been running for ~{} {}.'.format(duration, word))
                # wait until query has succeeded to start the next query
                if num_sec_query_has_been_running + args.wait > args.querylife:
                    sleep_time = args.querylife - num_sec_query_has_been_running
                else:
                    sleep_time = args.wait
                time.sleep(sleep_time)
                num_sec_query_has_been_running += sleep_time
                rsp = athena_client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
                succeeded_query = True if rsp['QueryExecution']['Status']['State'] == 'SUCCEEDED' else False
        print('='*60)
