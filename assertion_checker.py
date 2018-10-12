# This script runs through some date range and verifies one or more 
# assertions about the structure of the parking data (e.g., the value of a 
# given field is always one of three strings or one timestamp for a 
# given transactions is always after some other timestamp).
import sys, os, re, math, json
from collections import OrderedDict, Counter, defaultdict
from util import to_dict, value_or_blank, unique_values, zone_name, is_a_lot, \
lot_code, is_virtual, get_terminals, is_timezoneless, write_or_append_to_csv, \
pull_from_url, remove_field, round_to_cent, corrected_zone_name, lot_list, \
pure_zones_list, numbered_reporting_zones_list

from fetch_terminals import pull_terminals

import dataset
import time
from pprint import pprint
from datetime import datetime, timedelta
import pytz
from dateutil import parser

from process_data import last_date_cache, all_day_ps_cache, \
beginning_of_day, get_batch_parking, get_parking_events, \
get_batch_parking_for_day, hybrid_parking_segment_start_of, is_mobile_payment

def cast_string_to_dt(s):
    try:
        dt = parser.parse(s)
    except:
        raise ValueError("Unable to cast {} to a datetime".format(s))
    return dt


def time_difference(p,ref_field='@PurchaseDateUtc',dt_fmt='%Y-%m-%dT%H:%M:%S'):
    #p['start_date_utc'] = datetime.strptime(p['@StartDateUtc'],'%Y-%m-%dT%H:%M:%S')
    p['start_date_utc'] = parser.parse(p['@StartDateUtc'])
    try:
        p['ref_field'] = datetime.strptime(p[ref_field],dt_fmt)
    except:
        p['ref_field'] = cast_string_to_dt(p[ref_field])

    delta = (p['ref_field'] - p['start_date_utc']).total_seconds()
    return delta

#[ ] Find largest difference between StartDate and PurchaseDate

def main(*args, **kwargs):
    # This function accepts slot_start and halting_time datetimes as
    # arguments to set the time range and push_to_CKAN and output_to_csv
    # to control those output channels.

    output_to_csv = kwargs.get('output_to_csv',False)

    zone_kind = 'new' # 'old' maps to enforcement zones
    # (specifically corrected_zone_name). 'new' maps to numbered reporting
    # zones.
    if zone_kind == 'old':
        zonelist = lot_list + pure_zones_list
    else:
        zonelist = numbered_reporting_zones_list

    pgh = pytz.timezone('US/Eastern')
    use_cache = kwargs.get('use_cache', False)
    #use_cache = True
    terminals = get_terminals(use_cache)
    t_ids = [t['@Id'] for t in terminals]
    t_guids = [t['@Guid'] for t in terminals]


    timechunk = timedelta(days=1)

    slot_start = pgh.localize(datetime(2012,7,23,0,0)) # The earliest available data.
    #slot_start = pgh.localize(datetime(2012,9,1,0,0)) 
    #slot_start = pgh.localize(datetime(2013,1,1,0,0)) 
#    slot_start = pgh.localize(datetime(2016,1,1,0,0)) 
    slot_start = kwargs.get('slot_start',slot_start)

########
    halting_time = slot_start + timedelta(hours=24)

    halting_time = beginning_of_day(datetime.now(pgh) - timedelta(days=7))
    #halting_time = pgh.localize(datetime(2017,6,1,0,0)) 
    #halting_time = pgh.localize(datetime(2015,1,1,0,0)) 
    halting_time = kwargs.get('halting_time',halting_time)

    sampling_zones, parent_zones, _, _, _ = pull_terminals(use_cache,return_extra_zones=True)

    slot_start = beginning_of_day(slot_start)
    slot_end = beginning_of_day(slot_start + timedelta(hours = 26)) 
    # The tricky thing here is that one can't just increment by 
    # 24 hours and expect to split the processing into days because 
    # on days when Daylight Savings Time turns on and off there can be 
    # 23 or 25 hours in a day.
    current_day = slot_start.date()
    month_mark = slot_start.month

    print("The start of the first slot is {}".format(slot_start))
    assertion_1 = lambda p: (p['@PurchaseTypeName'] in [None, 'TopUp', 'Normal']) if ('@PurchaseTypeName' in p) else True
    assertion_2 = lambda p: (p['@PaymentServiceType'] in ['None', 'PrePay Code']) if ('@PaymentServiceType' in p) else False
    assertion_3 = lambda p: ((p['@PaymentServiceType'] == 'PrePay Code') != (p['PurchasePayUnit']['@PayUnitName']=='Mobile Payment')) 
        # PrePay Code <==> Mobile Payment
    assertion_4 = lambda p: (p['@PayIntervalEndLocal'] == p['@EndDateLocal'])
    assertion_5 = lambda p: (beginning_of_day(cast_string_to_dt(p['@DateCreatedUtc'])) - beginning_of_day(cast_string_to_dt(p['@StartDateUtc']))).days in [-1,0,1]
    # Assertion 5 can not be checked with caching_mode == 'db_caching' (since that 
    # would be incorporating the assumption in the test).
    # But get_batch_parking also makes its own assumptions, so 
    # get_batch_parking_for_day must be used to avoid timestamp filtering:
    deactivate_filter = True

    assertion_7 = lambda p: p['@Units'] == '0' or p['@PurchaseTypeName'] == 'Manual' or int(round((cast_string_to_dt(p['@EndDateUtc']) - cast_string_to_dt(p['@StartDateUtc'])).total_seconds()/60)) == int(p['@Units'])
    first_seen = {}

    start_purchase_max = start_created_max = -10000000
    start_purchase_min = start_created_min = 10000000

    caching_mode = 'utc_json'
    if caching_mode == 'db_caching':
        db_filename = kwargs.get('db_filename','transactions_cache.db') # This can be
        db = dataset.connect('sqlite:///'+db_filename)

    hours = defaultdict(int)

    while slot_start <= datetime.now(pytz.utc) and slot_start < halting_time:
        # Get all parking events that start between slot_start and slot_end
        if slot_end > datetime.now(pytz.utc): # Clarify the true time 
            slot_end = datetime.now(pytz.utc) # bounds of slots tha trun up 
            # against the limit of the current time.

        # The first Boolean in the function call below suppresses caching 
        # (if False). The second suppresses some messages to the console, 
        # so that assertion results don't get lost in the noise.

        if deactivate_filter:
            purchases = get_batch_parking_for_day(slot_start,pytz.utc,cache=False,mute=False)
        else:
            if caching_mode in ['db_caching', 'utc_json']:
                purchases = get_parking_events(db,slot_start,slot_end,True,True,caching_mode) 
            else: # caching_mode == 'local_json'?
                purchases = get_batch_parking(slot_start,slot_end,True,True,pgh) #,time_field = '@PurchaseDateLocal',dt_format='%Y-%m-%dT%H:%M:%S')


        #print("{} | {} purchases".format(datetime.strftime(slot_start.astimezone(pgh),"%Y-%m-%d %H:%M:%S ET"), len(purchases)))


        # One pitfall here is that if the cached JSON file was saved 
        # without a particular field, the assertion will not be tested.
        #pprint(purchases[0])
        #print(purchases[0]['@PaymentServiceType'])
        ref_field = '@DateCreatedUtc' #'@PurchaseDateUtc'
        for k,p in enumerate(sorted(purchases, key = lambda x: x['@DateCreatedUtc'])):


            #if not assertion_7(p):
            #    print("   EndDate - StartDate != Units:")
            #    pprint(p)

            # Just adding calculation of hybrid parking segment start here to
            # see when (if ever) it gets violated.
            #try:
            #    hybrid_parking_segment_start_of(p)
            #except:
            #    print("\n\nThe following purchase triggered an exception... Let's see if we can work out why.")
            #    pprint(p)
            #    raise ValueError("Z")
            #    pprint(p)

            if False: # Check time differences?
                if '@StartDateUtc' in p and ref_field in p:
                    delta = time_difference(p,ref_field)#,'%Y-%m-%dT%H:%M:%S.%f')
                    if delta > start_created_max:
                        start_created_max = delta
                        print("\nNow max = {} (StartDateUtc = {})".format(delta, p['@StartDateUtc']))
                        pprint(p)
                    if delta < start_created_min:
                        start_created_min = delta
                        print("\nNow min = {} (StartDateUtc = {})".format(delta, p['@StartDateUtc']))
                        pprint(p)

                    delta_in_hours = int(math.floor(delta/3600))
                    hours[delta_in_hours] += 1
                    if delta_in_hours*delta_in_hours > 2000*2000:
                        print("Here's one of the most anomalously large time differences:")
                        pprint(p)

                #if not assertion_5(p):
                #    print("Assertion 5 has been violated. Some events have a big time gap between StartDate and DateCreated, like this one:")
                #    pprint(p)

                # Assertion 5 gets violated a lot!

            field = '@PurchaseTypeName'
            if field in p:
                if field not in first_seen:
                    first_seen[field] = slot_start
                if not assertion_1(p):
                    print("Assertion 1 has been violated with a @PurchaseTypeName value of {}".format(p['@PurchaseTypeName']))
                    pprint(p)
            field = '@PaymentServiceType'
            if field in p:
                if field not in first_seen:
                    first_seen[field] = slot_start
                if not assertion_2(p):
                    print("Assertion 2 has been violated with a @PaymentServiceType value of {}".format(p[field]))
                    pprint(p)
                field2 = 'PurchasePayUnit'
                if field2 in p and '@PayUnitName' in p[field2]:
                    if not assertion_3(p):
                        print("Assertion 3 has been violated.")
                        pprint(p)
            # Assertion 4 gets violated a number of times per year, probably due to incorrect 
            # @PayIntervalEnd values.
            #if '@PayIntervalEndLocal' in p and '@EndDateLocal' in p:
            #    if not assertion_4(p):
            #        print("Assertion 4 has been violated.")
            #        pprint(p)



        slot_start = beginning_of_day(slot_start + timedelta(hours = 26))
        slot_end = beginning_of_day(slot_start + timedelta(hours = 26)) 
        # The tricky thing here is that one can't just increment 
        # by 24 hours and expect to split the processing into days
        # because on days when Daylight Savings Time turns on and off 
        # there can be 23 or 25 hours in a day.
        #print("The start and end of the next slot are {} and {}".format(slot_start,slot_end))

        # Print a period to the console for every month of data that 
        # has been processed to let the user know that something is happening.
        if month_mark != slot_start.month:
            month_mark = slot_start.month
            print(".", end="", flush=True)

    print("first_seen = {}".format(first_seen))

    print("start_created_min = {}, start_created_max = {}".format(start_created_min,start_created_max))
    print("Distribution of {} - StartTimeUTC (in hours): ".format(ref_field))
    pprint(hours)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        pgh = pytz.timezone('US/Eastern')
        slot_start_string = sys.argv[1]
        try:
            slot_start = pgh.localize(datetime.strptime(slot_start_string,'%Y-%m-%d'))
        except:
            slot_start = pgh.localize(datetime.strptime(slot_start_string,'%Y-%m-%dT%H:%M:%S'))
        main(slot_start=slot_start)
    else:
        main()
