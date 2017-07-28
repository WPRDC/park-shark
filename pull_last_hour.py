# This script can be run to pull the last hour of parking data through 
# the process_data script and (with the argument push_to_CKAN = True in 
# that function call), cause the resulting data to be pushed to the 
# CKAN instance specified by modules and settings called by process_data.

# The main() function returns a Boolean indicating whether this operation 
# succeeded or failed. In this way, this function can still be called by 
# some kind of pipeline/job manager that can send out notifications if a 
# particular ETL job fails.


# An intermediate step between making pipe_data_to_CKAN work and this 
# function would be running this script with output_to_csv = True and 
# push_to_CKAN = False and then calling a pipeline function to pipe the 
# CSV to the CKAN resource.
import sys, pytz
from datetime import datetime, timedelta
import process_data

def main(*args,**kwargs):
    raw_only = kwargs.get('raw_only',False)
    pgh = pytz.timezone('US/Eastern')
    slot_width = process_data.DEFAULT_TIMECHUNK.seconds
    slot_start = process_data.roundTime(datetime.now(pgh) - timedelta(hours=1), slot_width) 
    halting_time = datetime.now(pgh) #process_data.roundTime(datetime.now(pgh), slot_width)
    script_start = datetime.now()
    print("Started processing at {}.".format(script_start))
    success = process_data.main(raw_only = raw_only, output_to_csv = True, push_to_CKAN = False, slot_start = slot_start, halting_time = halting_time, threshold_for_uploading = 100)
    print("Started processing at {} and finished at {}.".format(script_start,datetime.now()))
    return success

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] in ['raw','raw_only']:
        main(raw_only = True)
    elif len(sys.argv) > 1 and sys.argv[1] in ['cooked','well-done','well_done','done']:
        main(raw_only = False)
    else:
        main()
