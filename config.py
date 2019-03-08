import datetime
import os
from credentials.credentials import initials

target_year = 2017
cutoff_year = datetime.datetime(target_year + 1,1,1)
derived_folder = './derived_data/%s' % initials
final_file = '%s/transactions-final.json' % derived_folder
input_folder = './input_data/%s' % initials

# 1 day in seconds
one_day = 24 * 60 * 60
dollar_pct = .2

num_processes = 4
parallel = False
ignore_margins = True
reload_data = True
likekind_threshold = 1000
likekind_cutoff_year = 2018

def maybe_open(path):
    maybe_print('Trying to open file %s' % path)
    if os.path.isfile(path) is False:
        maybe_print('Cannot find file %s' % path)
        return None
    return open(path, 'r')


def maybe_print(s):
    if True:
        print(s)
