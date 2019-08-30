import os
import boto3
import argparse
import time
from uuid import uuid4
import numpy as np
from numpy import math
import pandas as pd
import multiprocessing as mp
from cone_search_multiprocessing import cone_search

kwargs = {
    'ra': 323.067607,
    'dec': 0.254556,
    'radius': 0.008333333,
    'time_start': 810137366795,
    'time_end': 969718479645,
    'flag': 0,
    'aws_profile': 'aws-stsci-dmd',
    'aws_region': 'us-east-1',
    's3_output_location': 's3://trossbach-gphoton-bucket/athena/results',
    'local_output_location': '/Users/mtrossbach/gPhoton/query-results',
    'athena_database': 'mydatabase',
    'athena_workgroup': 'primary',
    'query_life': 6000,
    'single_query': False
}

start = time.time()
search = cone_search(**kwargs)
search.search_and_get()
print('\nElapsed time: ~{:.4f} seconds'.format(time.time() - start))
