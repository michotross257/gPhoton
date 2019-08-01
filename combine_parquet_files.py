import os
import argparse
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

parser = argparse.ArgumentParser(description='Get path to read PARQUET files and path to write PARQUET files.')
parser.add_argument('read', help='Path to PARQUET files to be read.')
parser.add_argument('write', help='Path where to write new PARQUET files.')
parser.add_argument('-f', '--factor', type=int, default=10, metavar='',
                    help='Factor of 10 used to reduce the number of files (default: 10).')
args = parser.parse_args()

read_path = args.read
write_path = args.write
files = sorted(os.listdir(read_path))
files = list(filter(lambda x: x.endswith('parquet'), files))
root = files[0].split('.')[0]
# split the range of files into chunks
segments = np.linspace(0, len(files), int(len(files)/args.factor)).astype(int)
for index in range(len(segments)-1):
    start, end = segments[index], segments[index+1]
    file_range = files[start:end]
    tbl = pq.read_table(os.path.join(read_path, file_range[0]))
    file_name = root + '.' + str(index).zfill(2) + '.parquet'
    with pq.ParquetWriter(os.path.join(write_path, file_name),
                          tbl.schema) as writer:
        msg = 'Writing {} to {}'.format(file_range[0], file_name)
        print(msg)
        writer.write_table(tbl)
        for file in file_range[1:]:
            tbl = pq.read_table(os.path.join(read_path, file))
            print('Writing {} to {}'.format(file, file_name))
            writer.write_table(tbl)
    print('='*len(msg))
