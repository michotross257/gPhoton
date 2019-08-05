import boto3
import json
from functools import reduce
from datetime import datetime, timezone
import argparse

parser = argparse.ArgumentParser(description='Get queries, AWS credentials, and Athena, DynamoDB, and S3 info.')
parser.add_argument('queries', help='Path to JSON file containing Athena queries.')
parser.add_argument('-p', '--profile', default='default', metavar='',
                    help='Name of the AWS profile to use (default="default").')
parser.add_argument('region', help='Name of AWS region.')
parser.add_argument('database', help='Name of Athena Database containing the table to query.')
parser.add_argument('tables', help='Comma separated list of Athena tables to query.')
parser.add_argument('workgroup', help='Name of Athena workgroup.')
parser.add_argument('output_location', help='S3 location where to save query results.')
parser.add_argument('table', help='Name of the DynamoDB table to write to.')
parser.add_argument('-r', '--ramin', default=0.0, type=float, metavar='',
                    help='Minimum of ra range to be queried (default=0.0).')
parser.add_argument('-s', '--ramax', default=1.0, type=float, metavar='',
                    help='Maximum of ra range to be queried (default=1.0).')
parser.add_argument('-d', '--decmin', default=0.0, type=float, metavar='',
                    help='Minimum of dec range to be queried (default=0.0).')
parser.add_argument('-e', '--decmax', default=1.0, type=float, metavar='',
                    help='Maximum of dec range to be queried (default=1.0).')
args = parser.parse_args()

if __name__ == '__main__':
    sess = boto3.Session(profile_name=args.profile,
                         region_name=args.region)
    athena_client = sess.client('athena')
    dynamodb_client = sess.client('dynamodb')
    
    tables = [tbl.strip() for tbl in args.tables.split(',')]
    with open(args.queries) as query_file:
        queries = query_file.read()
    queries = json.loads(queries)
    queries = {int(key): reduce(lambda x, y: x+'\n'+y, queries[key]) for key in queries}

    for query in queries:
        for table in tables:
            query_args = {
                1: (table,),
                2: (table,),
                3: (table,
                    args.ramin,
                    args.ramax,
                    args.decmin,
                    args.decmax)
            }
            response = athena_client.start_query_execution(
                            QueryString=queries[query].format(*query_args[query]),
                            QueryExecutionContext={
                                'Database': args.database
                            },
                            ResultConfiguration={
                                'OutputLocation': args.output_location
                            },
                            WorkGroup=args.workgroup
                        )
            start_time = datetime.now(timezone.utc)
            item = {
                'EXECUTION_ID':
                    {"S": response['QueryExecutionId']},
                'QUERY_ID':
                    {"S": str(query)},
                'EXECUTION_START_TIME':
                    {"S": start_time.strftime('%H:%M:%S.%f %p %Z')}
            }
            dynamodb_client.put_item(TableName=args.table, Item=item)
            print('Query #{} for table {}'.format(query, table))
            print('Query Execution ID: {}\n'.format(response['QueryExecutionId']))
