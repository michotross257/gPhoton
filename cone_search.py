import os
import boto3
import argparse
import time
from uuid import uuid4
import numpy as np
from numpy import math
import pandas as pd
import multiprocessing as mp

def get_alpha(radius, dec):
    if abs(dec) + radius > 89.9:
        return 180
    return math.degrees(abs(math.atan(
                  math.sin(math.radians(radius)) /
                  np.sqrt(abs(math.cos(math.radians(dec - radius)) * math.cos(math.radians(dec + radius))
                  ))
            )))

def cone_search(ra, dec, radius, time_start, time_end, flag,
                aws_profile, aws_region, s3_output_location, local_output_location,
                athena_database, athena_workgroup, query_life=10, wait_time=0.1, single_query=True):
    
    def query_athena(query, query_args):
        query = query.format(*query_args)
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': athena_database
            },
            ResultConfiguration={
                'OutputLocation': s3_output_location
            },
            WorkGroup=athena_workgroup
        )
        print('Query submitted:\n{}\n'.format(query))
        rsp = athena_client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
        succeeded_query = True if rsp['QueryExecution']['Status']['State'] == 'SUCCEEDED' else False
        num_sec_query_has_been_running = 0
        # check to see if the query has succeeded
        while not succeeded_query:
            if num_sec_query_has_been_running >= query_life:
                print('QUERY CANCELLED: Query {} has been running for ~{} seconds.'.format(response['QueryExecutionId'],
                                                                                           num_sec_query_has_been_running))
                _ = athena_client.stop_query_execution(QueryExecutionId=response['QueryExecutionId'])
                return None
            if num_sec_query_has_been_running % 60 == 0 and num_sec_query_has_been_running:
                duration = int(num_sec_query_has_been_running/60)
                word = 'minutes' if duration > 1 else 'minute'
                print('...Query has been running for ~{} {}.'.format(duration, word))
            # wait until query has succeeded to start the next query
            if num_sec_query_has_been_running + wait_time > query_life:
                sleep_time = query_life - num_sec_query_has_been_running
            else:
                sleep_time = wait_time
            time.sleep(sleep_time)
            num_sec_query_has_been_running += sleep_time
            rsp = athena_client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
            succeeded_query = True if rsp['QueryExecution']['Status']['State'] == 'SUCCEEDED' else False
        return response['QueryExecutionId']
            
                
    sess = boto3.Session(profile_name=aws_profile,
                         region_name=aws_region)
    athena_client = sess.client('athena')
    s3_client = sess.client('s3')
    s3_output_path = s3_output_location.replace('s3://', '').split('/')
    bucket = s3_output_path[0]
    additional_s3_path = s3_output_location.replace('s3://{}/'.format(bucket), '')
    queries = {
        'single': '''
            SELECT *
            FROM gPhoton_partitioned
            WHERE zoneID BETWEEN {} AND {}
                AND dec BETWEEN {} AND {}
                AND ra BETWEEN {} AND {}
                AND ({}*cx + {}*cy + {}*cz) > {}
                AND time >= {} AND time < {}
                AND flag = {};
         ''',
        'multiple': '''
            SELECT *
            FROM gPhoton_partitioned
            WHERE zoneID = {}
                AND dec BETWEEN {} AND {}
                AND ra BETWEEN {} AND {}
                AND ({}*cx + {}*cy + {}*cz) > {}
                AND time >= {} AND time < {}
                AND flag = {};
         '''
    }
    
    cx = math.cos(math.radians(dec)) * math.cos(math.radians(ra))
    cy = math.cos(math.radians(dec)) * math.sin(math.radians(ra))
    cz = math.sin(math.radians(dec))
    alpha = get_alpha(radius, dec)
    if (ra - alpha) < 0:
        ra = ra + 360
    zoneHeight = 30.0/3600.0
    min_zoneid = int(np.floor((dec - radius + 90.0) / zoneHeight))
    max_zoneid = int(np.floor((dec + radius + 90.0) / zoneHeight))
    
    query_args_collection = {
        'non-conditional': [
            dec - radius, dec + radius,
            ra - alpha, ra + alpha,
            cx, cy, cz, math.cos(math.radians(radius)),
            time_start, time_end,
            flag
        ],
        'conditional': [
            dec - radius, dec + radius,
            0, ra - 360 + alpha,
            cx, cy, cz, math.cos(math.radians(radius)),
            time_start, time_end,
            flag
        ]
    }

    query_collection = ''
    query_argument_collection = []
    for zoneid in range(min_zoneid, max_zoneid+1):
        query = queries['single'] if single_query else queries['multiple']
        zone_args = [min_zoneid, max_zoneid] if single_query else [zoneid]
        query_args = zone_args + query_args_collection['non-conditional']
        if (ra + alpha) > 360:
            query = query.replace(';', '') + '\n            UNION ALL\n' + query
            additional_args = [min_zoneid, max_zoneid] if single_query else [zoneid]
            query_args = query_args + additional_args + query_args_collection['conditional']
        temp_query = query.replace(';', '') + '\n            UNION ALL\n' if not single_query and zoneid != max_zoneid else query
        query_collection += temp_query
        query_argument_collection.extend(query_args)
        if single_query:
            break
    start_time = time.time()
    execution_id = query_athena(query_collection, query_argument_collection)
    print('Time taken to query: ~{:.4f} seconds'.format(time.time()-start_time))

    # get single CSV or accumulate the CSVs from the different SELECT statements
    dfs = []
    download_paths = []
    if execution_id is not None:
        path_to_csv = os.path.join(additional_s3_path, execution_id + '.csv')
        download_path = os.path.join(local_output_location, execution_id + '.csv')
        start_time = time.time()
        s3_client.download_file(bucket,
                                path_to_csv,
                                download_path)
        print('Time taken to download: ~{:.4f} seconds'.format(time.time()-start_time))
        dfs.append(pd.read_csv(download_path, engine='python'))
        download_paths.append(download_path)
    if len(dfs):
        df = pd.concat(dfs)
        output_location = os.path.join(local_output_location, str(uuid4()) + '.csv')
        df.to_csv(output_location, index=False)
        print('\nData written to {}\n'.format(output_location))
        for download_path in download_paths:
            os.remove(download_path)
        print(df.head())
    else:
        print('No CSVs were found.')
