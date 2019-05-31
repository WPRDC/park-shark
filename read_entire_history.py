# This script can be run to read some days of parking data (from earlier in 
# the week) through the process_data script and optionally (based on
# command-line arguments), cause the resulting data to be pushed to the 
# CKAN instance specified by modules and settings called by process_data
# and/or save the results to a local CSV file.


## Legacy comment:
# In contrast with pull_from_last_hour.py, this script is designed to pull 
# from days prior to the current day (to avoid any issues where some parking 
# transactions are still ongoing) and to pull transaction data from many days
# all at once but be run daily, rather than pulling in small increments and 
# updating frequently like the quasi-live pull_from_last_hour.py.

# The main() function returns a Boolean indicating whether this operation 
# succeeded or failed. In this way, this function can still be called by 
# some kind of pipeline/job manager that can send out notifications if a 
# particular ETL job fails.

import pytz, requests
from datetime import datetime, timedelta
from dateutil import parser
import process_data
import sys
from pprint import pprint

from util.util import get_terminals

list_of_servers = ["meters-etl", #"official-terminals",
    "transactions-production",
    "transactions-payment-time-of",
    "transactions-prototype",
    "transactions-by-pdl",
    "split-transactions-by-pdl",
    "debug",
    "testbed",
    "sandbox",
    #"aws-test"
    ] # This list could be automatically harvested from SETTINGS_FILE.

def main(*args,**kwargs):
    pgh = pytz.timezone('US/Eastern')
    slot_width = process_data.DEFAULT_TIMECHUNK.seconds
    #slot_start = process_data.beginning_of_day(datetime.now(pgh) - timedelta(days=6))
    #halting_time = process_data.beginning_of_day(datetime.now(pgh) - timedelta(days=2))
    output_to_csv = kwargs.get('output_to_csv',False)
    push_to_CKAN= kwargs.get('push_to_CKAN',False)
    slot_start = kwargs.get('slot_start',pgh.localize(datetime(2012,7,23,0,0)))
    halting_time = kwargs.get('halting_time', pgh.localize(datetime(3030,4,13,0,0)))
    spacetime = kwargs.get('spacetime','zone')
    server = kwargs.get('server','debug')
    caching_mode = 'utc_sqlite'
    csv_filename = 'test-{}.csv'.format(slot_start.date())
    #utc_json_folder = 'utc_json_completed'
    utc_json_folder = 'utc_json'
    # Note that these days are chosen to be within the last 7 days so that 
    # the data can be pulled from the API without using the bulk API (and 
    # without doing any caching). It might be more efficient to pull all of 
    # that data in larger chunks and then use the default timechunk value 
    # for the processing; this would require reworking process_data.py.
    script_start = datetime.now()
    print("Started processing at {}.".format(script_start))
    use_cache = False
    try:
        terminals = get_terminals(use_cache)
    except requests.exceptions.ConnectionError:
        use_cache = True
    success = process_data.main(use_cache=use_cache, server=server, output_to_csv = output_to_csv, push_to_CKAN = push_to_CKAN, spacetime = spacetime, caching_mode = caching_mode, utc_json_folder = utc_json_folder, slot_start = slot_start, halting_time = halting_time, threshold_for_uploading = 5000, filename = csv_filename)
    print("Started processing at {} and finished at {}.".format(script_start,datetime.now()))
    return success

if __name__ == '__main__':
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        output_to_csv = False
        push_to_CKAN = False

        copy_of_args = list(args)

        pgh = pytz.timezone('US/Eastern')

        slot_start = None
        halting_time = None
        kwparams = {}
        # This is a new way of parsing command-line arguments that cares less about position
        # and just does its best to identify the user's intent.
        for k,arg in enumerate(copy_of_args):
            if arg in ['scan', 'save', 'csv']:
                output_to_csv = True
                args.remove(arg)
            elif arg in ['pull', 'push', 'ckan']:
                push_to_CKAN = True
                args.remove(arg)
            elif arg in list_of_servers:
                kwparams['server'] = arg
                args.remove(arg)
            elif slot_start is None:
                slot_start_string = arg
                slot_start = pgh.localize(parser.parse(slot_start_string))
                kwparams['slot_start'] = slot_start
                args.remove(arg)
                #except:
                #    slot_start = pgh.localize(datetime.strptime(slot_start_string,'%Y-%m-%dT%H:%M:%S'))
            elif halting_time is None:
                halting_time_string = arg
                halting_time = pgh.localize(parser.parse(halting_time_string))
                kwparams['halting_time'] = halting_time
                args.remove(arg)
            else:
                print("I have no idea what do with args[{}] = {}.".format(k,arg))

        kwparams['output_to_csv'] = output_to_csv
        kwparams['push_to_CKAN'] = push_to_CKAN
        pprint(kwparams)
        main(**kwparams)
    else:
        raise ValueError("Please specify some command-line parameters")

