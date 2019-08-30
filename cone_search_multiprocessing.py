import os
import boto3
import argparse
import time
from uuid import uuid4
import numpy as np
from numpy import math
import pandas as pd
import multiprocessing as mp


class cone_search:
    def __init__(self, ra, dec, radius, time_start, time_end, flag,
                 aws_profile, aws_region, s3_output_location, local_output_location,
                 athena_database, athena_workgroup, query_life=10, wait_time=0.1, single_query=True):
        self.ra = ra
        self.dec = dec
        self.radius = radius
        self.time_start = time_start
        self.time_end = time_end
        self.flag = flag
        self.s3_output_location = s3_output_location
        self.local_output_location = local_output_location
        self.athena_database = athena_database
        self.athena_workgroup = athena_workgroup
        self.query_life = query_life
        self.wait_time = wait_time
        self.single_query = single_query
        sess = boto3.Session(profile_name=aws_profile,
                             region_name=aws_region)
        global athena_client, s3_client
        athena_client = sess.client('athena')
        s3_client = sess.client('s3')
        self.s3_output_path = s3_output_location.replace('s3://', '').split('/')
        self.bucket = self.s3_output_path[0]
        self.additional_s3_path = s3_output_location.replace('s3://{}/'.format(self.bucket), '')
        self.query = '''
                SELECT *
                FROM gPhoton_partitioned
                WHERE zoneID = {}
                AND dec BETWEEN {} AND {}
                AND ra BETWEEN {} AND {}
                AND ({}*cx + {}*cy + {}*cz) > {}
                AND time >= {} AND time < {}
                AND flag = {};
            '''

        # calculate_query_parameters
        self.cx = math.cos(math.radians(self.dec)) * math.cos(math.radians(self.ra))
        self.cy = math.cos(math.radians(self.dec)) * math.sin(math.radians(self.ra))
        self.cz = math.sin(math.radians(self.dec))
        self.alpha = self._get_alpha()
        if (self.ra - self.alpha) < 0:
            self.ra = self.ra + 360
        self.zoneHeight = 30.0/3600.0
        self.min_zoneid = int(np.floor((self.dec - self.radius + 90.0) / self.zoneHeight))
        self.max_zoneid = int(np.floor((self.dec + self.radius + 90.0) / self.zoneHeight))
    
        self.query_args_collection = {
            'non-conditional': [
                self.dec - self.radius, self.dec + self.radius,
                self.ra - self.alpha, self.ra + self.alpha,
                self.cx, self.cy, self.cz, math.cos(math.radians(self.radius)),
                self.time_start, self.time_end,
                self.flag
            ],
            'conditional': [
                self.dec - self.radius, self.dec + self.radius,
                0, self.ra - 360 + self.alpha,
                self.cx, self.cy, self.cz, math.cos(math.radians(self.radius)),
                self.time_start, self.time_end,
                self.flag
            ]
        }        

    def _get_alpha(self):
        if abs(self.dec) + self.radius > 89.9:
            return 180
        return math.degrees(abs(math.atan(
                        math.sin(math.radians(self.radius)) /
                        np.sqrt(abs(math.cos(math.radians(self.dec - self.radius)) * math.cos(math.radians(self.dec + self.radius))
                        ))
                    )))

    
    def _query_athena(self, query, query_args):
        query = query.format(*query_args)
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': self.athena_database
            },
            ResultConfiguration={
                'OutputLocation': self.s3_output_location
            },
            WorkGroup=self.athena_workgroup
        )
        print('Query submitted:\n{}\n'.format(query))
        rsp = athena_client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
        succeeded_query = True if rsp['QueryExecution']['Status']['State'] == 'SUCCEEDED' else False
        num_sec_query_has_been_running = 0
        # check to see if the query has succeeded
        while not succeeded_query:
            if num_sec_query_has_been_running >= self.query_life:
                print('QUERY CANCELLED: Query {} has been running for ~{} seconds.'.format(response['QueryExecutionId'],
                                                                                           num_sec_query_has_been_running))
                _ = athena_client.stop_query_execution(QueryExecutionId=response['QueryExecutionId'])
                return None
            if num_sec_query_has_been_running % 60 == 0 and num_sec_query_has_been_running:
                duration = int(num_sec_query_has_been_running/60)
                word = 'minutes' if duration > 1 else 'minute'
                print('...Query has been running for ~{} {}.'.format(duration, word))
            # wait until query has succeeded to start the next query
            if num_sec_query_has_been_running + self.wait_time > self.query_life:
                sleep_time = self.query_life - num_sec_query_has_been_running
            else:
                sleep_time = self.wait_time
            time.sleep(sleep_time)
            num_sec_query_has_been_running += sleep_time
            rsp = athena_client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
            succeeded_query = True if rsp['QueryExecution']['Status']['State'] == 'SUCCEEDED' else False
        return response['QueryExecutionId']
    
    
    def _get_execution_id(self, zoneid):
        query = self.query
        query_args = [zoneid] + self.query_args_collection['non-conditional']
        if (self.ra + self.alpha) > 360:
            query = query.replace(';', '') + '\n            UNION ALL\n' + query
            query_args = query_args + [zoneid] + self.query_args_collection['conditional']
        start_time = time.time()
        execution_id = self._query_athena(query, query_args)
        print('Time taken to query (Zone ID: {}; Execution ID: {}): ~{:.4f} seconds'.format(zoneid,
                                                                                            execution_id,
                                                                                            time.time()-start_time))
        return execution_id
    
    
    def _download_csvs(self, execution_id):
        if execution_id is not None:
            path_to_csv = os.path.join(self.additional_s3_path, execution_id + '.csv')
            download_path = os.path.join(self.local_output_location, execution_id + '.csv')
            start_time = time.time()
            s3_client.download_file(self.bucket,
                                    path_to_csv,
                                    download_path)
            print('Time taken to download (Execution ID: {}): ~{:.4f} seconds'.format(execution_id,
                                                                                      time.time()-start_time))
            return (download_path, pd.read_csv(download_path, engine='python'))
    
    def search_and_get(self):
        execution_ids = []
        zoneid_range = list(range(self.min_zoneid, self.max_zoneid+1))
        num_processes = os.cpu_count() if len(zoneid_range) >= os.cpu_count() else len(zoneid_range)
        pool = mp.Pool(processes=num_processes)
        for execution_id in pool.map(self._get_execution_id, zoneid_range):
            execution_ids.append(execution_id)
        
        download_paths = []
        dfs = []
        num_processes = os.cpu_count() if len(execution_ids) >= os.cpu_count() else len(execution_ids)
        pool = mp.Pool(processes=num_processes)
        for item in pool.map(self._download_csvs, execution_ids):
            download_paths.append(item[0])
            dfs.append(item[1])
            
        if len(dfs):
            df = pd.concat(dfs)
            output_location = os.path.join(self.local_output_location, str(uuid4()) + '.csv')
            df.to_csv(output_location, index=False)
            print('\nData written to {}\n'.format(output_location))
            for download_path in download_paths:
                os.remove(download_path)
            print(df.head())
        else:
            print('No CSVs were found.')
