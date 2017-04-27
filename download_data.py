import xmltodict
import os
import re

import json
from collections import OrderedDict, Counter, defaultdict
from util import to_dict, value_or_blank, unique_values, zone_name, is_a_lot, lot_code, is_virtual, centroid_np, get_terminals, is_timezoneless, write_or_append_to_csv, pull_from_url, remove_field, round_to_cent, corrected_zone_name, lot_list, pure_zones_list, numbered_reporting_zones_list, special_groups, add_element_to_set_string, add_if_new, group_by_code, numbered_zone, censor, only_these_fields, cast_fields
from fetch_terminals import pull_terminals_return_special_zones_and_parent_zones
import requests
import zipfile, StringIO
from copy import copy

import time
import pprint
from datetime import datetime, timedelta
import pytz

from credentials_file import CALE_API_user, CALE_API_password
from local_parameters import path
from process_data import roundTime, build_url, convert_doc_to_purchases, get_parking_events, get_recent_parking_events, get_batch_parking_for_day

last_date_cache = None
all_day_ps_cache = []
dts = []

def get_batch_parking(slot_start,slot_end,cache,tz):
    global last_date_cache, all_day_ps_cache, dts
    if last_date_cache != slot_start.date():
        print("last_date_cache ({}) doesn't match slot_start.date() ({})".format(last_date_cache, slot_start.date()))
        ps_for_whole_day = get_batch_parking_for_day(slot_start,cache)
        ps_all = ps_for_whole_day
        all_day_ps_cache = ps_all
        dts = [tz.localize(datetime.strptime(p['@PurchaseDateLocal'],'%Y-%m-%dT%H:%M:%S')) for p in ps_all]
        time.sleep(3)
    else:
        ps_all = all_day_ps_cache
    #ps = [p for p in ps_all if slot_start <= tz.localize(datetime.strptime(p['@PurchaseDateLocal'],'%Y-%m-%dT%H:%M:%S')) < slot_end] # This takes like 3 seconds to
    # execute each time for busy days since the time calculations
    # are on the scale of tens of microseconds.
    # So let's generate the datetimes once (above), and do
    # it this way:
    ps = [p for p,dt in zip(ps_all,dts) if slot_start <= dt < slot_end]
    # Now instead of 3 seconds it takes like 0.03 seconds.
    last_date_cache = slot_start.date()
    return ps

def main():
    output_to_csv = False
    push_to_CKAN = False

    turbo_mode = True # When turbo_mode is true, skip time-consuming stuff,
    # like correct calculation of durations.
    #turbo_mode = False
    skip_processing = True

    zone_kind = 'new' # 'old' maps to enforcement zones
    # (specifically corrected_zone_name). 'new' maps to numbered reporting
    # zones.
    if zone_kind == 'old':
        zonelist = lot_list + pure_zones_list
    else:
        zonelist = numbered_reporting_zones_list

    pgh = pytz.timezone('US/Eastern')

    timechunk = timedelta(minutes=10) #10 minutes
  #  timechunk = timedelta(seconds=1)
    if skip_processing:
        timechunk = timedelta(hours=24)

    # Start 24 hours ago (rounded to the nearest hour).
    # This is a naive (timezoneless) datetime, so let's try it this way:
    # It is recommended that all work be done in UTC time and that the conversion to a local time zone only happen at the end, when presenting something to humans.
    slot_start = pgh.localize(datetime(2014,2,7,0,0))
    #slot_start = pgh.localize(datetime(2012,8,1,0,0)) # Possibly the earliest available data.


########
    halting_time = slot_start + timedelta(hours=2)
    halting_time = roundTime(datetime.now(pgh), 24*60*60)
    halting_time = pgh.localize(datetime(2017,4,17,0,0))

    slot_end = slot_start + timechunk

    current_day = slot_start.date()

    while slot_start < datetime.now(pytz.utc) and slot_start < halting_time:
        # * Get all parking events that start between slot_start and slot_end
        purchases = get_parking_events(slot_start,slot_end,True)

        print("{} | {} purchases".format(datetime.strftime(slot_start.astimezone(pgh),"%Y-%m-%d %H:%M:%S ET"), len(purchases)))

        slot_start += timechunk
        slot_end = slot_start + timechunk


if __name__ == '__main__':
  main()
