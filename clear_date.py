"""A simple script to clear the SQLite cache (both cached_dates
table and SQLite files) for a single given date."""
import sys
from util.sqlite_util import clear_cache_for_date
from datetime import timedelta
from dateutil import parser

from parameters.local_parameters import path

#reference_time = 'purchase_time' # For use with sqlite-pdl
reference_time = 'purchase_time_utc' # For use with sqlite-pdu

if len(sys.argv) == 2:
    dt = parser.parse(sys.argv[1])
    date_i = dt.date()
    print("Attempting to clear the SQLite cache for date = {}".format(date_i))
    clear_cache_for_date(path,reference_time,date_i)
    print("SQLite cache cleared for {}".format(date_i))
elif len(sys.argv) == 3:
    first_dt = parser.parse(sys.argv[1])
    first_date_i = first_dt.date()
    last_dt = parser.parse(sys.argv[2])
    last_date_i = last_dt.date()
    print("Attempting to clear the SQLite cache for dates {} through {}".format(first_date_i,last_date_i))
    date_i = first_date_i
    while date_i <= last_date_i:
        clear_cache_for_date(path,reference_time,date_i)
        print("     SQLite cache cleared for {}".format(date_i))
        date_i += timedelta(days=1)
    print("SQLite cache cleared for {} through {}".format(first_date_i,last_date_i))
else:
    raise ValueError("Incorrect number of command-line parameters found.")
