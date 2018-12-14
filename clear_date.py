"""A simple script to clear the SQLite cache (both cached_dates
table and SQLite files) for a single given date."""
import sys
from util.sqlite_util import clear_cache_for_date
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
else:
    raise ValueError("Incorrect number of command-line parameters found.")
