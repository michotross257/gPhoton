import os
import boto3
import subprocess
import argparse

parser = argparse.ArgumentParser(description='Get AWS info and directory of PARQUET files.')
parser.add_argument('region', help='Name of AWS region.')
parser.add_argument('-p', '--profile', metavar='', default='default',
                    help='Name of AWS profile.')
parser.add_argument('bucket', help='Name of AWS S3 bucket to which the PARQUET files will be uploaded.')
parser.add_argument('folder', help='Path to folder where PARQUET files are stored.')
args = parser.parse_args()


sess = boto3.Session(region_name=args.region,
                     profile_name=args.profile)
client = sess.client('s3')

for file in os.listdir(args.folder):
	command = ['aws', 's3', 'cp',
               os.path.join(args.folder, file),
               's3://{}'.format(os.path.join(args.bucket, file)),
               '--profile', args.profile]
	subprocess.run(command)
