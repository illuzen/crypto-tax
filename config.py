import datetime
import os

cutoff_year = datetime.datetime(2018,1,1)
derived_folder = './derived_data'
final_file = './derived_data/transactions-final.json'


# 1 day in seconds
one_day = 24 * 60 * 60
one_minute = 55 * 1000

dollar_epsilon = 100
dollar_pct = .2

num_processes = 4
parallel = True


def maybe_open(path):
    maybe_print('Trying to open file %s' % path)
    if os.path.isfile(path) is False:
        maybe_print('Cannot find file %s' % path)
        return None
    return open(path, 'r')


def maybe_print(s):
    if True:
        print(s)
