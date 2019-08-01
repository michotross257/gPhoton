import os
import boto3
import multiprocessing as mp
import subprocess
import argparse
import numpy as np

parser = argparse.ArgumentParser(description='Get AWS info and directory of PARQUET files.')
parser.add_argument('region', help='Name of AWS region.')
parser.add_argument('-p', '--profile', metavar='', default='default',
                    help='Name of AWS profile (default: "default").')
parser.add_argument('bucket', help='Name of AWS S3 bucket to which the PARQUET files will be uploaded.')
parser.add_argument('folder', help='Path to folder where PARQUET files are stored.')
parser.add_argument('-m', '--multiprocessing', action='store_true',
                    help='Whether to use multiprocessing to do the upload.')
args = parser.parse_args()

def file_upload(file_names):
    '''
        Upload files to S3.
        
        Parameters
        ----------
        file_names: list
            name(s) of file(s) to be uploaded
        
        Returns
        -------
        None
    '''
    for file in file_names:
        command = [
            'aws', 's3', 'cp',
            os.path.join(args.folder, file),
            's3://{}'.format(os.path.join(args.bucket, file)),
            '--profile', args.profile
        ]
        output = subprocess.run(command, capture_output=True, text=True)
        print(output.stdout)
        #subprocess.run(command)

if __name__ == '__main__':
    sess = boto3.Session(region_name=args.region,
                         profile_name=args.profile)
    client = sess.client('s3')

    files = sorted(os.listdir(args.folder))
    files = list(filter(lambda x: x.endswith('parquet'), files))
    if args.multiprocessing:
        processing_segments = np.linspace(0, len(files), mp.cpu_count()+1).astype(int)
        processes = []
        for index in range(mp.cpu_count()):
            start, stop = processing_segments[index], processing_segments[index+1]
            processes.append(mp.Process(target=file_upload,
                                        args=(files[start: stop],)))
        [p.start() for p in processes]
        [p.join() for p in processes]
    else:
        file_upload(files)
