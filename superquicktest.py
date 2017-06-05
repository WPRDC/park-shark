# This script can be run to pull the last hour of parking data through 
# the process_data script and (with the argument push_to_CKAN = True 
# in that function call), cause the resulting data to be pushed to 
# the CKAN instance specified by modules and settings called by process_data.

# The main() function returns a Boolean indicating whether this operation 
# succeeded or failed. In this way, this function can still be called by 
# some kind of pipeline/job manager that can send out notifications if a 
# particular ETL job fails.

import pytz
from datetime import datetime, timedelta
import process_data

def main():
    pgh = pytz.timezone('US/Eastern')
    slot_width = process_data.DEFAULT_TIMECHUNK.seconds
    slot_start = process_data.roundTime(datetime.now(pgh) - timedelta(minutes=10), slot_width) 
    halting_time = datetime.now(pgh) #process_data.roundTime(datetime.now(pgh), slot_width)

    slot_start = pgh.localize(process_data.beginning_of_day(datetime(2012,7,23,0,0)))
#    slot_start = pgh.localize(process_data.beginning_of_day(datetime(2016,9,29,0,0)))
    halting_time = slot_start+timedelta(hours = 24)
    slot_start = pgh.localize(datetime(2013,1,23,11,45,18))
    halting_time = slot_start+timedelta(seconds=1)
    script_start = datetime.now()
    print("Started processing at {}.".format(script_start))
    success = process_data.main(output_to_csv = True, push_to_CKAN = False, slot_start = slot_start, halting_time = halting_time, threshold_for_uploading = 100, timechunk = timedelta(seconds=1))
    print("Started processing at {} and finished at {}.".format(script_start,datetime.now()))
    return success

if __name__ == '__main__':
    main()
