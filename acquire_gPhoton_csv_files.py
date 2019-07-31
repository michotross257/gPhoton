import os
import sys
import requests
import csv
import argparse
import multiprocessing as mp
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

parser = argparse.ArgumentParser(description='Get path to TXT file containing names of CSVs to be downloaded.')
parser.add_argument('txt_file', help='Path to TXT file.')
parser.add_argument('save_path', help='Path to folder where to save PARQUET files.')
args = parser.parse_args()
header = [       'zoneID',     'time',        'cx',        'cy',         'cz',        'x',           'y',         'xa',
                   'ya',        'q',          'xi',        'eta',        'ra',       'dec',         'flag']
header_dtypes = [np.int32,    np.int64,    np.float64,   np.float64,  np.float64,  np.float64,    np.float64,   np.int16,
                 np.int16,    np.int16,    np.float64,   np.float64,  np.float64,  np.float64,    np.int8                ]

def mp_content_download(file_names):
    '''
        Uses multiprocessing module to download CSVs and incrementally create parquet
        files from the CSV files.
        
        Parameters
        ----------
        file_names: list
            name(s) of file(s) to be downloaded by the multiprocessing process
        
        Returns
        -------
        None
    '''
    for file_name in file_names:
        print('Downloading content from: {}'.format(file_name))
        # ----- lazy -----
        downloaded_csv = requests.get(file_name, stream=True)
        lines = (line.decode('utf-8') for line in downloaded_csv.iter_lines())
        reader = csv.reader(lines, delimiter='|')
        # ----- lazy -----
        content_length = int(downloaded_csv.headers['Content-Length'])
        max_size = int(content_length/np.ceil(content_length/1e+7))
        collected_data = []
        inc = 0
        for cnt, row in enumerate(reader):
            if sys.getsizeof(collected_data) >= max_size:
                path = os.path.join(args.save_path, file_name.split('/')[-1].replace('csv', '') + str(inc).zfill(2) + '.parquet')
                print('Generating DataFrame and writing Parquet file to {}'.format(path))
                df = pd.DataFrame(collected_data, columns=header)
                for i in range(len(header)):
                    df[header[i]] = df[header[i]].astype(header_dtypes[i])
                tbl = pa.Table.from_pandas(df)
                pq.write_table(tbl, path, compression='snappy')
                collected_data = []
                inc += 1
            else:
                collected_data.append(row)

if __name__ == '__main__':
    with open(args.txt_file) as txt_file:
        file_names = list(filter(lambda x: len(x), txt_file.read().split('\n')))
    processing_segments = np.linspace(0, len(file_names), mp.cpu_count()+1).astype(int)
    processes = []
    for index in range(mp.cpu_count()):
        start, stop = processing_segments[index], processing_segments[index+1]
        processes.append(mp.Process(target=mp_content_download,
                                    args=(file_names[start: stop],)))
    [p.start() for p in processes]
    [p.join() for p in processes]
