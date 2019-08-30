import os
import boto3
import argparse
import time
from uuid import uuid4
import numpy as np
from numpy import math
import pandas as pd

from cone_search import cone_search, get_alpha

parser = argparse.ArgumentParser(description='Get query args and AWS credentials.')
parser.add_argument('-p', '--profile', default='default', metavar='',
                    help='Name of the AWS profile to use (default="default").')
parser.add_argument('region', help='Name of AWS region.')
parser.add_argument('database', help='Name of Athena Database containing the table to query.')
parser.add_argument('workgroup', help='Name of Athena workgroup.')
parser.add_argument('s3_output_location', help='S3 location where to save query results.')
parser.add_argument('ra', type=float, help='RA query parameter.')
parser.add_argument('dec', type=float, help='DEC query parameter.')
parser.add_argument('radius', type=float, help='Radius used to determine the zone to be searched.')
parser.add_argument('timestart', type=int, help='Start time to be searched in query.')
parser.add_argument('timeend', type=int, help='End time to be searched in query.')
parser.add_argument('flag', type=int, help='?')
parser.add_argument('-o', '--local_output_location', default=os.getcwd(), metavar='',
                    help='Where to save CSV files on local machine (default=Current directory).')
parser.add_argument('-l', '--querylife', default=10, type=int, metavar='',
                    help='Maximum number of seconds query should be allowed to run before stopping/cancelling (default=10).')
parser.add_argument('-w', '--wait', default=0.1, type=int, metavar='',
                    help='Time to wait between checks to see if a query is still running (default=0.1 seconds).')
parser.add_argument('-i', '--numiterations', default=5, type=int, metavar='',
                    help='Number of times to test the querying speed (default=5).')
args = parser.parse_args()
assert 0 <= args.ra <= 360, '0 <= RA <= 360.'
assert -90 <= args.dec <= 90, '-90 <= DEC <= 90.'
assert args.radius >= 0, 'Radius must be greater than or equal to 0.'

if __name__ == '__main__':
    query_approaches = {
        'single': {
            'value': True,
            'time-record': 0
        },
        'multiple': {
            'value': False,
            'time-record': 0
        }
    }
    for i in range(args.numiterations):
        for approach in query_approaches:
            print('\nQuery approach: {}\n'.format(approach))
            start = time.time()
            cone_search(args.ra, args.dec, args.radius, args.timestart, args.timeend, args.flag,
                        args.profile, args.region, args.s3_output_location, args.local_output_location,
                        args.database, args.workgroup, args.querylife, args.wait, query_approaches[approach]['value'])
            elapsed = time.time()-start
            query_approaches[approach]['time-record'] += elapsed
            print('='*130 + '\nElapsed Time: ~{:.4f} seconds'.format(elapsed))
    print('\nRESULTS\n')
    for approach in query_approaches:
        print('Query Approach: {}\nAverage query time after {} iterations: ~{:.4f} seconds\n'.format(approach,
                                                                                                     args.numiterations,
                                                                                                     query_approaches[approach]['time-record']/args.numiterations))
