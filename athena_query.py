import boto3
import argparse
import numpy as np
import pandas as pd

parser = argparse.ArgumentParser(description='Get query args and AWS credentials.')
parser.add_argument('-p', '--profile', default='default', metavar='',
                    help='Name of the AWS profile to use (default="default").')
parser.add_argument('region', help='Name of AWS region.')
parser.add_argument('database', help='Name of Athena Database containing the table to query.')
parser.add_argument('workgroup', help='Name of Athena workgroup.')
parser.add_argument('output_location', help='S3 location where to save query results.')
parser.add_argument('ramin', type=float, help='Minimum of ra range to be queried.')
parser.add_argument('ramax', type=float, help='Maximum of ra range to be queried.')
parser.add_argument('decmin', type=float, help='Minimum of dec range to be queried.')
parser.add_argument('decmax', type=float, help='Maximum of dec range to be queried.')
parser.add_argument('radius', type=float, help='Radius used to determine the zone to be searched.')
parser.add_argument('-l', '--querylife', default=10, type=int, metavar='',
                    help='Maximum number of seconds query should be allowed to run before stopping/cancelling (default=10).')
parser.add_argument('-w', '--wait', default=0.1, type=int, metavar='',
                    help='Time to wait between checks to see if a query is still running (default=0.1 seconds).')
args = parser.parse_args()
assert 0 <= args.ramin <= 360, '0<= RA min <= 360.'
assert 0 <= args.ramax <= 360, '0<= RA max <= 360.'
assert 0 <= args.decmin <= 360, '0<= DEC min <= 360.'
assert 0 <= args.decmax <= 360, '0<= DEC max <= 360.'
assert args.radius > 0, 'Radius must be greater than 0.'
assert args.ramin <= args.ramax, 'RA min must be less than or equal to RA max.'
assert args.decmin <= args.decmax, 'DEC min must be less than or equal to DEC max.'

ra_partitions = [
    '0<=ra<36',
    '36<=ra<72',
    '72<=ra<108',
    '108<=ra<144',
    '144<=ra<180',
    '180<=ra<216',
    '216<=ra<252',
    '252<=ra<288',
    '288<=ra<324',
    '324<=ra<=360'
]

def get_min_max_zoneids(dec, radius):
    zoneHeight = 30.0/3600.0
    min_zoneid = np.floor((dec - radius + 90.0) / zoneHeight)
    max_zoneid = np.floor((dec + radius + 90.0) / zoneHeight)
    return (min_zoneid, max_zoneid)
def get_ra_partition_index(ra_value):
    ra_factor = 36
    return int((ra_value/ra_factor))

if __name__ == '__main__':
    # ==============================
    # query 1: create external table
    # query 2: select data
    # query 3: delete external table
    # ==============================
    sess = boto3.Session(profile_name=args.profile,
                         region_name=args.region)
    athena_client = sess.client('athena')
    s3_client = sess.client('s3')
    #s3://trossbach-gphoton-bucket/athena/results/13b94fee-5e47-46cc-8b15-6b1c08e6311f.csv
    
    min_zoneid, max_zoneid = get_min_max_zoneids(args.decmin, args.radius)
    min_ra_partition_index = get_ra_partition_index(args.ramin)
    max_ra_partition_index = get_ra_partition_index(args.ramax)
    print('min: {}, max: {}'.format(min_ra_partition_index, max_ra_partition_index))
    # endpoint of ra range is not inclusive, except for the last range (i.e. 324<=ra<=360)
    if min_ra_partition_index == max_ra_partition_index:
        if min_ra_partition_index == len(ra_partitions):
            min_ra_partition_index -= 1
        selected_ra_partitions = ra_partitions[min_ra_partition_index]
    else:
        if args.ramax < 360:
            max_ra_partition_index += 1
        selected_ra_partitions = ra_partitions[min_ra_partition_index:max_ra_partition_index]

    queries = {
        'create': '''
                  CREATE EXTERNAL TABLE gPhoton (
                  zoneID INT,
                  time BIGINT,
                  cx DOUBLE,
                  cy DOUBLE,
                  cz DOUBLE,
                  x DOUBLE,
                  y DOUBLE,
                  xa INT,
                  ya INT,
                  q INT,
                  xi DOUBLE,
                  eta DOUBLE,
                  ra DOUBLE,
                  dec DOUBLE,
                  flag TINYINT
                  ) STORED AS PARQUET
                  LOCATION 's3://trossbach-gphoton-bucket/{}/{}'
                  tblproperties ("parquet.compress"="SNAPPY")
                  ''',
        'select': '''
                  SELECT COUNT(*)
                  FROM gPhoton
                  WHERE ra BETWEEN {} AND {};
                  ''',
        'delete': '''
                  DROP TABLE `gphoton`;
                  '''
    }
    
    query_args = {
        'select': (args.ramin, args.ramax),
        'delete': (None,)
    }

    execution_ids = []
    for zoneid in range(min_zoneid, max_zoneid):
        for ra_partition in selected_ra_partitions:
            query_args['create'] = (zoneid, ra_partition)
            for query in ['create', 'select', 'delete']:
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
                if query == 'SELECT':
                    execution_ids.append(response['QueryExecutionId'])
    # accumulate the CSVs from the different SELECT statements
    dfs = []
    for cnt, execution_id in enumerate(execution_ids):
        path_to_csv = s3_path + execution_id + '.csv'
        s3_client.download_file(bucket, path_to_csv, where_to_save)
        dfs.append(pd.read_csv(where_to_save))
    df = pd.concat(dfs)
