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

# The main() function returns a Boolean indicating whether this operation 
# succeeded or failed. In this way, this function can still be called by 
# some kind of pipeline/job manager that can send out notifications if 
# a particular ETL job fails.

import sys, pytz
from datetime import datetime, timedelta
import process_data

def main(*args,**kwargs):
    raw_only = kwargs.get('raw_only',False)
    test_mode = kwargs.get('test_mode',False)
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
    success = process_data.main(raw_only = raw_only, output_to_csv = output_to_csv, push_to_CKAN = push_to_CKAN, slot_start = slot_start, halting_time = halting_time, threshold_for_uploading = 1000)
    print("Started processing at {} and finished at {}.".format(script_start,datetime.now()))
    return success

if __name__ == '__main__':
    raw_only = True
    test_mode = False
    if len(sys.argv) > 1 and sys.argv[1] in ['raw','raw_only']:
        raw_only = True
    elif len(sys.argv) > 1 and sys.argv[1] in ['cooked','well-done','well_done','done']:
        raw_only = False
    if len(sys.argv) > 2 and sys.argv[2] in ['test','test_mode']:
        test_mode = True

    if len(sys.argv) > 1:
        print("raw_only = {}, test_mode = {}".format(raw_only,test_mode))
    main(raw_only = raw_only, test_mode = test_mode)
