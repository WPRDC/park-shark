# This is a variant of pull_last_hour.py which just pulls ten minutes of data 
# (though process_data still insists on going back many hours to help 
# aggregate transactions together properly) to test that the system is working.

import pytz
from datetime import datetime, timedelta
import process_data

def main():
    pgh = pytz.timezone('US/Eastern')
    slot_width = process_data.DEFAULT_TIMECHUNK.seconds
    slot_start = process_data.beginning_of_day(datetime.now(pgh) - timedelta(days=53)) + timedelta(hours=8)
    slot_start = process_data.beginning_of_day(datetime.now(pgh) - timedelta(days=5)) + timedelta(hours=8)
    slot_start = process_data.beginning_of_day(datetime.now(pgh)) + timedelta(hours=8)
    halting_time = process_data.beginning_of_day(datetime.now(pgh)) + timedelta(hours=8) + timedelta(minutes=1)
    script_start = datetime.now()
    print("Started processing at {}. (Processing transactions between {} and {})".format(script_start, slot_start, halting_time))
    success = process_data.main(output_to_csv = True, push_to_CKAN = False, slot_start = slot_start, halting_time = halting_time, threshold_for_uploading = 1000)
    print("Started processing at {} and finished at {}.".format(script_start,datetime.now()))
    return success

if __name__ == '__main__':
    main()
