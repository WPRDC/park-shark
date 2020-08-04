# This script can be run to pull some days of parking data (from earlier 
# in the week) through the process_data script and (with the argument 
# push_to_CKAN = True in that function call), cause the resulting data 
# to be pushed to the CKAN instance specified by modules and settings 
# called by process_data.

# In contrast with pull_from_last_hour.py, this script is designed to pull from 
# days prior to the current day (to avoid any issues where some parking 
# transactions are still ongoing) and to pull transaction data from many days
# all at once but be run daily, rather than pulling in small increments and 
# updating frequently like the quasi-live pull_from_last_hour.py.

import sys, pytz
from datetime import datetime, timedelta
import process_data
from read_entire_history import list_of_servers
from pprint import pprint

# This script seems to be malfunctioning.
def main(*args,**kwargs):
    raw_only = kwargs.get('raw_only',False)
    test_mode = kwargs.get('test_mode',False)
    mute_alerts = kwargs.get('mute_alerts',False)
    if test_mode:
        output_to_csv = True
        push_to_CKAN = False
    else:
        output_to_csv = False
        push_to_CKAN = True
    pgh = pytz.timezone('US/Eastern')
    slot_width = process_data.DEFAULT_TIMECHUNK.seconds
    slot_start = process_data.beginning_of_day(datetime.now(pgh) - timedelta(days=6))
    halting_time = process_data.beginning_of_day(datetime.now(pgh) - timedelta(days=2))
    # Note that these days are chosen to be within the last 7 days so that 
    # the data can be pulled from the API without using the bulk API 
    # (and without doing any caching). It might be more efficient to pull 
    # all of that data in larger chunks and then use the default 
    # timechunk value for the processing; this would require reworking 
    # process_data.py.
    script_start = datetime.now()
    print("Started processing at {}.".format(script_start))
    success = process_data.main(raw_only = raw_only, output_to_csv = output_to_csv, push_to_CKAN = push_to_CKAN, slot_start = slot_start, halting_time = halting_time, threshold_for_uploading = 1000, mute_alerts = mute_alerts)
    print("Started processing at {} and finished at {}.".format(script_start,datetime.now()))

if __name__ == '__main__':
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        output_to_csv = False
        push_to_CKAN = False
        raw_only = True
        test_mode = False
        mute_alerts = False
        server = 'testbed'

        copy_of_args = list(args)

        kwparams = {}
        server = 'debug'
        # This is a new way of parsing command-line arguments that cares less about position
        # and just does its best to identify the user's intent.
        for k,arg in enumerate(copy_of_args):
            if arg in ['scan', 'save', 'csv']:
                output_to_csv = True
                args.remove(arg)
            elif arg in ['pull', 'push', 'ckan']:
                push_to_CKAN = True
                args.remove(arg)
            elif arg in ['raw','raw_only']:
                raw_only = True
            elif arg in ['cooked','well-done','well_done','done']:
                raw_only = False
            elif arg in ['test','test_mode']:
                test_mode = True
            elif arg in ['mute', 'mute_alerts']:
                mute_alerts = True
            elif arg in list_of_servers:
                kwparams['server'] = arg
                args.remove(arg)
            else:
                print("I have no idea what do with args[{}] = {}.".format(k,arg))

        kwparams['mute_alerts'] = mute_alerts
        kwparams['raw_only'] = raw_only
        kwparams['test_mode'] = test_mode
        kwparams['server'] = server

        kwparams['output_to_csv'] = output_to_csv
        kwparams['push_to_CKAN'] = push_to_CKAN
        pprint(kwparams)
        main(**kwparams)
    else:
        raise ValueError("Please specify some command-line parameters")
