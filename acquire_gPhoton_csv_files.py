import os
import sys
import re
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
parser.add_argument('-t', '--test', action='store_true',
                    help='To test the search_partitions function. If flagged, only test will be run.')
args = parser.parse_args()

header = [       'zoneID',     'time',        'cx',        'cy',         'cz',        'x',           'y',         'xa',
                   'ya',        'q',          'xi',        'eta',        'ra',       'dec',         'flag'              ]
header_dtypes = [np.int32,    np.int64,    np.float64,   np.float64,  np.float64,  np.float64,    np.float64,   np.int16,
                 np.int16,    np.int16,    np.float64,   np.float64,  np.float64,  np.float64,    np.int8                ]
ra_max = 360
ra_factor = 36
dec_max = 90
dec_factor = 18


def test():
    tests = [
        {
             'zoneID': 10829,
             'ra': 0.0,
             'dec': -90.0
        },
        {
             'zoneID': 10830,
             'ra': 360.0,
             'dec': 90.0
        },
        {
             'zoneID': 10831,
             'ra': 45.0,
             'dec': 0.0
        },
        {
             'zoneID': 10829,
             'ra': 108.0,
             'dec': -54.0
        },
        {
             'zoneID': 10830,
             'ra': 216.0,
             'dec': 18.0
        },
        {
             'zoneID': 10831,
             'ra': 359.99999,
             'dec': 89.99999
        },
        {
             'zoneID': 10829,
             'ra': 83.12345,
             'dec': -2.09876
        },
        {
             'zoneID': 10830,
             'ra': 258.00001,
             'dec': 7.8910001
        }
    ]

    expected = [
        '/Users/mtrossbach/gPhoton/data/partitioned/zoneID=10829/0<=ra<36/-90<=dec<-72',
        '/Users/mtrossbach/gPhoton/data/partitioned/zoneID=10830/324<=ra<=360/72<=dec<=90',
        '/Users/mtrossbach/gPhoton/data/partitioned/zoneID=10831/36<=ra<72/0<=dec<18',
        '/Users/mtrossbach/gPhoton/data/partitioned/zoneID=10829/108<=ra<144/-54<=dec<-36',
        '/Users/mtrossbach/gPhoton/data/partitioned/zoneID=10830/216<=ra<252/18<=dec<36',
        '/Users/mtrossbach/gPhoton/data/partitioned/zoneID=10831/324<=ra<=360/72<=dec<=90',
        '/Users/mtrossbach/gPhoton/data/partitioned/zoneID=10829/72<=ra<108/-18<=dec<0',
        '/Users/mtrossbach/gPhoton/data/partitioned/zoneID=10830/252<=ra<288/0<=dec<18'
    ]

    for index, test in enumerate(tests):
        result = search_partitions(test)
        assert result == expected[index], 'TEST #{} FAILED\nTest    : {}\nExpected: {}\nGot     : {}\n'.format(index+1,
                                                                                                               test,
                                                                                                               expected[index],
                                                                                                               result)
    print('All tests passed.')


def search_partitions(params, index=0, path=args.save_path):
    folders = list(filter(lambda x: os.path.isdir(os.path.join(path, x)), os.listdir(path)))
    if index == 0:
        # ZONE ID
        fldrs = [int(re.findall('\d+', fldr)[0]) for fldr in folders]
        fldr_index = fldrs.index(params['zoneID'])
        return search_partitions(params=params, index=index+1, path=os.path.join(path, folders[fldr_index]))
    else:
        folders = sorted(folders, key=lambda x: int(re.findall('[-\d]+', x)[0]))
        if index == 1:
            # RA
            if params['ra'] == ra_max:
                fldr_index = int((params['ra']/ra_factor))-1
            else:
                fldr_index = int((params['ra']/ra_factor))
            return search_partitions(params=params, index=index+1, path=os.path.join(path, folders[fldr_index]))
        elif index == 2:
            # DEC
            if params['dec'] == dec_max:
                fldr_index = int((params['dec']/dec_factor))-1
            else:
                fldr_index = int(np.floor(params['dec']/dec_factor))
            # shift by the number of partitions under 0
            fldr_index += 5
            return os.path.join(path, folders[fldr_index])


def write_parquet_file(path, data):
    save_path = path + '.' + str(data[path]['incrementor']).zfill(2) + '.parquet'
    print('\tGenerating DataFrame and writing Parquet file to {}'.format(save_path))
    df = pd.DataFrame(data[path]['data'], columns=header)
    for i in range(len(header)):
        df[header[i]] = df[header[i]].astype(header_dtypes[i])
    tbl = pa.Table.from_pandas(df)
    pq.write_table(tbl,
                   save_path,
                   compression='snappy')



def content_download(file_names):
    '''
        Download CSVs and incrementally create parquet files from the CSV files.
        
        Parameters
        ----------
        file_names: list
            name(s) of file(s) to be downloaded by the multiprocessing process
        
        Returns
        -------
        None
    '''
    data_collection = {}
    for file_name in file_names:
        print('Downloading content from: {}'.format(file_name))
        # ----- lazy -----
        downloaded_csv = requests.get(file_name, stream=True)
        lines = (line.decode('utf-8') for line in downloaded_csv.iter_lines())
        reader = csv.reader(lines, delimiter='|')
        # ----- lazy -----
        content_length = int(downloaded_csv.headers['Content-Length'])
        max_size = int(content_length/np.ceil(content_length/1e+8))
        print('Maximum file size: ~{:.4f} MB'.format(max_size*1e-6))
        for cnt, row in enumerate(reader):
            print('\rReading row #{:,}'.format(cnt), end='', flush=True)
            if cnt and cnt % 50000 == 0:
                print()
                for _ in data_collection:
                    print('\t{} -> {:,} elements, ~{:.4f} MB'.format(_,
                                                                     len(data_collection[_]['data']),
                                                                     sys.getsizeof(data_collection[_]['data'])*1e-6))
            try:
                params = {
                    'zoneID': int(row[0]),
                    'ra': float(row[12]),
                    'dec': float(row[13])
                }
            except:
                print('\nRow is not the expected length:\n{}\n'.format(row))
                continue
            path = search_partitions(params)
            path = os.path.join(path, file_name.split('/')[-1].replace('.csv', ''))
            if path not in data_collection:
                data_collection[path] = {
                    'incrementor': 0,
                           'data': []
                }
            if sys.getsizeof(data_collection[path]['data']) >= max_size:
                write_parquet_file(path, data_collection)
                data_collection[path]['data'] = []
                data_collection[path]['incrementor'] += 1
            else:
                data_collection[path]['data'].append(row)

    for path in data_collection:
        if len(data_collection[path]['data']):
            write_parquet_file(path, data_collection)


if __name__ == '__main__':
    if args.test:
        test()
    else:
        with open(args.txt_file) as txt_file:
            file_names = list(filter(lambda x: len(x), txt_file.read().split('\n')))
        content_download(file_names)
        '''
        processing_segments = np.linspace(0, len(file_names), mp.cpu_count()+1).astype(int)
        processes = []
        for index in range(mp.cpu_count()):
            start, stop = processing_segments[index], processing_segments[index+1]
            processes.append(mp.Process(target=mp_content_download,
                                        args=(file_names[start: stop],)))
        [p.start() for p in processes]
        [p.join() for p in processes]
        '''
