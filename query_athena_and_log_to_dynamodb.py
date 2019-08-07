import boto3
import json
from functools import reduce
from collections import OrderedDict
import time
from datetime import datetime, timezone
import argparse

parser = argparse.ArgumentParser(description='Get queries, AWS credentials, and Athena, DynamoDB, and S3 info.')
parser.add_argument('queries', help='Path to JSON file containing Athena queries.')
parser.add_argument('-p', '--profile', default='default', metavar='',
                    help='Name of the AWS profile to use (default="default").')
parser.add_argument('region', help='Name of AWS region.')
parser.add_argument('database', help='Name of Athena Database containing the table to query.')
parser.add_argument('tables', help='Comma separated list of Athena table(s) to query.')
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
parser.add_argument('-n', '--querycycles', default=2, type=int, metavar='',
                    help='Number of query cycles - i.e. number of times to reduce the query range by half (default=2).')
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
    
    tables = [tbl.strip() for tbl in args.tables.split(',')]
    with open(args.queries) as query_file:
        queries = query_file.read()
    queries = json.loads(queries)
    queries = {int(key): reduce(lambda x, y: x+'\n'+y, queries[key]) for key in queries}
    query_args = {
        1: OrderedDict(zip(['TABLE_ID'],
                           [   None   ])),
        2: OrderedDict(zip(['TABLE_ID'],
                           [   None   ])),
        3: OrderedDict(zip(['TABLE_ID',  'RA_MIN' ,  'RA_MAX' ,  'DEC_MIN' ,  'DEC_MAX' ],
                           [   None   , args.ramin, args.ramax, args.decmin, args.decmax]))
    }
    for i in range(args.querycycles):
        msg = 'Query cycle {} of {}\n'.format(i+1, args.querycycles)
        print(msg + '-'*(len(msg)-1))
        for query in queries:
            for table in tables:
                query_args[query]['TABLE_ID'] = table
                response = athena_client.start_query_execution(
                                QueryString=queries[query].format(*query_args[query].values()),
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
                for key in query_args[query].keys():
                    item[key] = {'S': str(query_args[query][key])}
                dynamodb_client.put_item(TableName=args.table, Item=item)
                print('Query #{} for table {}'.format(query, table))
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
        query_args[3]['RA_MIN'] = query_args[3]['RA_MIN'] + ((query_args[3]['RA_MAX'] - query_args[3]['RA_MIN'])/2)
        query_args[3]['DEC_MIN'] = query_args[3]['DEC_MIN'] + ((query_args[3]['DEC_MAX'] - query_args[3]['DEC_MIN'])/2)
