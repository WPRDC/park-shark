# This script can be run to read 24-hours of day-old parking data 
# through the process_data script and push the resulting data to the 
# CKAN instance specified by modules and settings called by process_data.

import sys, pytz, requests, traceback
from datetime import datetime, timedelta
from dateutil import parser
from pprint import pprint

import process_data
from util.util import get_terminals
from read_entire_history import main, list_of_servers
from notify import send_to_slack

if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            args = sys.argv[1:]
            output_to_csv = False
            push_to_CKAN = True
            mute_alerts = False

            copy_of_args = list(args)

            pgh = pytz.timezone('US/Eastern')
            current_hour = process_data.round_time(datetime.now(),60*60,"down") # Round current time down to the beginning of the hour.

            kwparams = {}
            slot_start = pgh.localize(current_hour - timedelta(days=3))
            kwparams['slot_start'] = slot_start
            halting_time = pgh.localize(current_hour - timedelta(days=1))
            kwparams['halting_time'] = halting_time
            kwparams['caching_mode'] = 'none'
            print("slot_start = {}, halting_time = {}".format(slot_start, halting_time))

            # This is a new way of parsing command-line arguments that cares less about position
            # and just does its best to identify the user's intent.
            for k,arg in enumerate(copy_of_args):
                if arg in ['scan', 'save', 'csv']:
                    output_to_csv = True
                    args.remove(arg)
                elif arg in ['pull', 'push', 'ckan']:
                    push_to_CKAN = True
                    args.remove(arg)
                elif arg in ['mute', 'mute_alerts']:
                    mute_alerts = True
                    args.remove(arg)
                elif arg in list_of_servers:
                    kwparams['server'] = arg
                    args.remove(arg)
                else:
                    print("I have no idea what do with args[{}] = {}.".format(k,arg))

            kwparams['mute_alerts'] = mute_alerts
            kwparams['output_to_csv'] = output_to_csv
            kwparams['push_to_CKAN'] = push_to_CKAN
            pprint(kwparams)
            main(**kwparams)
        except:
            e = sys.exc_info()[0]
            msg = "Error: {} : \n".format(e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            msg = ''.join('!! ' + line for line in lines)
            msg = 'push_day_old_data_cacheless.py: ' + msg
            print(msg) # Log it or whatever here
            send_to_slack(msg,username='park-shark (tools)',channel='@david',icon=':mantelpiece_clock:')
    else:
        raise ValueError("Please specify some command-line parameters")
