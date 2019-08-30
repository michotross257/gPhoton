import os
import sys
import re
import requests
import csv
import argparse
from uuid import uuid4
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
parser.add_argument('-m', '--multiprocessing', action='store_true',
                    help='To parallelize the downloads.')
args = parser.parse_args()

header = [       'zoneID',     'time',        'cx',        'cy',         'cz',        'x',           'y',         'xa',
                   'ya',        'q',          'xi',        'eta',        'ra',       'dec',         'flag'              ]
header_dtypes = [np.int32,    np.int64,    np.float64,   np.float64,  np.float64,  np.float64,    np.float64,   np.int16,
                 np.int16,    np.int16,    np.float64,   np.float64,  np.float64,  np.float64,    np.int8                ]
ra_max = 360
ra_factor = 36

def test():
    tests = [
        {
             'zoneID': 10829,
             'ra': 0.0,
        },
        {
             'zoneID': 10830,
             'ra': 360.0,
        },
        {
             'zoneID': 10829,
             'ra': 72.0,
        },
        {
             'zoneID': 10831,
             'ra': 45.0,
        },
        {
             'zoneID': 10829,
             'ra': 108.0,
        },
        {
             'zoneID': 10830,
             'ra': 216.0,
        },
        {
             'zoneID': 10831,
             'ra': 359.99999,
        },
        {
             'zoneID': 10829,
             'ra': 83.12345,
        },
        {
             'zoneID': 10830,
             'ra': 258.00001,
        },
        {
             'zoneID': 10829,
             'ra': 36,
        }
    ]

    expected = [
        'zoneID=10829/0<=ra<36',
        'zoneID=10830/324<=ra<=360',
        'zoneID=10829/72<=ra<108',
        'zoneID=10831/36<=ra<72',
        'zoneID=10829/108<=ra<144',
        'zoneID=10830/216<=ra<252',
        'zoneID=10831/324<=ra<=360',
        'zoneID=10829/72<=ra<108',
        'zoneID=10830/252<=ra<288',
        'zoneID=10829/36<=ra<72',
    ]
    expected = [os.path.join(args.save_path, e) for e in expected]

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
        # ZONE ID partitions
        fldrs = [int(re.findall('\d+', fldr)[0]) for fldr in folders]
        fldr_index = fldrs.index(params['zoneID'])
        return search_partitions(params=params, index=index+1, path=os.path.join(path, folders[fldr_index]))
    else:
        # RA partitions
        folders = sorted(folders, key=lambda x: int(re.findall('[-\d]+', x)[0]))
        if params['ra'] == ra_max:
            fldr_index = int((params['ra']/ra_factor))-1
        else:
            fldr_index = int((params['ra']/ra_factor))
        return os.path.join(path, folders[fldr_index])


def write_parquet_file(path, data):
    i = -1
    while path[i] != '/':
        i -= 1
    save_path = os.path.join(path[:i], str(uuid4()) + '.parquet')
    print('Generating DataFrame and writing Parquet file to {}'.format(save_path))
    df = pd.DataFrame(data[path]['data'], columns=header)
    for i in range(len(header)):
        df[header[i]] = df[header[i]].astype(header_dtypes[i])
    df = df.sort_values(['ra'])
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(tbl,
                   save_path,
                   compression='snappy')

def content_download(file_names):
    '''
    Download CSVs and incrementally create parquet files from the CSV files.
        
    Parameters
    ----------
    file_names: list
        name(s) of file(s) to be downloaded
        
    Returns
    -------
    None
    '''
    
    num_rows_read = 0
    num_rows_retained = 0
    data_collection = {}
    for file_name in file_names:
        print('Downloading content from: {}'.format(file_name))
        # ------------------------------- lazy -------------------------------
        downloaded_csv = requests.get(file_name, stream=True)
        lines = (line.decode('utf-8') for line in downloaded_csv.iter_lines())
        reader = csv.reader(lines, delimiter='|')
        # ------------------------------- lazy -------------------------------
        max_size = int(2.5e+7)
        if not args.multiprocessing:
            print('Maximum file size: {} MB'.format(int(max_size*1e-6)))
        root = file_name.split('/')[-1].replace('.csv', '')
        for cnt, row in enumerate(reader):
            num_rows_read += 1
            if not args.multiprocessing:
                print('\rReading row #{:,}'.format(cnt), end='', flush=True)
            if cnt and cnt % 50000 == 0:
                if not args.multiprocessing:
                    print()
                else:
                    print('Process:', file_name)
                for _ in data_collection:
                    print('\t{} -> {:,} rows, ~{:.4f} MB'.format(_,
                                                                 len(data_collection[_]['data']),
                                                                 sys.getsizeof(data_collection[_]['data'])*1e-6))
            if None in row:
                print('\n\nNone type found in row.\n{}'.format(row))
                continue
            if len(row) != len(header):
                print('\n\nRow is not the expected length of {} elements:\n{} elements -> {}\n'.format(len(header),
                                                                                                       len(row),
                                                                                                       row))
                continue
            else:
                params = {}
                for element in ['zoneID', 'ra']:
                    try:
                        index = header.index(element)
                        temp = header_dtypes[index](row[index])
                        params[element] = temp
                    except:
                        print('Value {} for element {} not convertable to dtype {}'.format(row[index],
                                                                                           element,
                                                                                           header_dtypes[index].__name__))
                        continue
            num_rows_retained += 1
            path = search_partitions(params)
            path = os.path.join(path, root)
            if path not in data_collection:
                data_collection[path] = {
                           'data': []
                }
            if sys.getsizeof(data_collection[path]['data']) >= max_size:
                write_parquet_file(path, data_collection)
                data_collection[path]['data'] = []
            else:
                data_collection[path]['data'].append(row)

        for path in data_collection:
            if len(data_collection[path]['data']):
                write_parquet_file(path, data_collection)
        data_collection.clear()
        
    return {'num_rows_read': num_rows_read, 'num_rows_retained': num_rows_retained}

if __name__ == '__main__':
    if args.test:
        test()
    else:
        with open(args.txt_file) as txt_file:
            file_names = list(filter(lambda x: len(x), txt_file.read().split('\n')))
        total_num_rows_read = 0
        total_num_rows_retained = 0
        if args.multiprocessing:
            num_processes = os.cpu_count() if len(file_names) >= os.cpu_count() else len(file_names)
            file_names = [[x] for x in file_names]
            pool = mp.Pool(processes=num_processes)
            for result in pool.map(content_download, file_names):
                total_num_rows_read += result['num_rows_read']
                total_num_rows_retained += result['num_rows_retained']
            
        else:
            result = content_download(file_names)
            total_num_rows_read += result['num_rows_read']
            total_num_rows_retained += result['num_rows_retained']
        print('\nTotal number of rows read: {}\nTotal number of rows retained: {}'.format(total_num_rows_read,
                                                                                          total_num_rows_retained))
