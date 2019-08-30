import os
import sys

def make_folders(partitions, index=0, paths=None):
    '''
    Recursively create the directory structure and then make the directories.
    '''
    if paths is None:
        paths = [PATH]
    temp_paths = []
    for path in paths:
        try:
            for partition in partitions[index]:
                temp_path = os.path.join(path, partition)
                temp_paths.append(temp_path)
        except:
            for path in paths:
                try:
                    os.mkdir(path)
                except:
                    os.makedirs(path)
            return
    make_folders(partitions=partitions, index=index+1, paths=temp_paths)

def get_partitions(params):
    '''
    For a given ID, create a list of partitions based on the min, max, and step values.
    '''
    partitions = []
    for val in range(params['min'], params['max'], params['step']):
        if val == (params['max'] - params['step']):
            partitions.append('{}<={}<={}'.format(val, params['id'], val+params['step']))
        else:
            partitions.append('{}<={}<{}'.format(val, params['id'], val+params['step']))
    return partitions

if __name__ == '__main__':
    PATH = sys.argv[1]
    partition_factor = 10
    zoneIDs = [10829, 10830, 10831]
    ra = {
        'id': 'ra',
        'min': 0,
        'max': 360
    }
    ra['step'] = int((ra['max'] - ra['min'])/partition_factor)
    dec = {
        'id': 'dec',
        'min': -90,
        'max': 90
    }
    dec['step'] = int((dec['max'] - dec['min'])/partition_factor)

    p = [
         ['zoneID={}'.format(id) for id in zoneIDs],
         get_partitions(ra),
         #get_partitions(dec)
    ]
    make_folders(p)
