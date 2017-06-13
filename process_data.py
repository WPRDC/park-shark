import xmltodict
import os
import re

import json
from collections import OrderedDict, Counter, defaultdict
from util import to_dict, value_or_blank, unique_values, zone_name, is_a_lot, \
lot_code, is_virtual, get_terminals, is_timezoneless, write_or_append_to_csv, \
pull_from_url, remove_field, round_to_cent, corrected_zone_name, lot_list, \
pure_zones_list, numbered_reporting_zones_list, ad_hoc_groups, \
add_element_to_set_string, add_if_new, group_by_code, numbered_zone, censor, \
only_these_fields, cast_fields
from fetch_terminals import pull_terminals
import requests
import zipfile
try:
    from StringIO import StringIO as BytesIO # For Python 2
except ImportError:
    from io import BytesIO # For Python 3
from copy import copy

import time
import pprint
from datetime import datetime, timedelta
import pytz

import dataset, sqlalchemy


from credentials_file import CALE_API_user, CALE_API_password
from local_parameters import path
from prime_ckan.remote_parameters import server, resource_id, ad_hoc_resource_id

# These functions should eventually be pulled from a repository othern than
# utility_belt that can import transmogrifier:
#from prime_ckan.push_to_CKAN_resource import push_data_to_ckan, open_a_channel
#from prime_ckan.pipe_to_CKAN_resource import pipe_data_to_ckan
#from prime_ckan.gadgets import get_resource_parameter, get_package_name_from_resource_id

#from prime_ckan.pipe_to_CKAN_resource import pipe_data_to_ckan

import sys
try:
    sys.path.insert(0, '~/WPRDC') # A path that we need to import code from
    from utility_belt.push_to_CKAN_resource import push_data_to_ckan, open_a_channel
    from utility_belt.gadgets import get_resource_parameter, get_package_name_from_resource_id
except:
    try:
        sys.path.insert(0, '/Users/daw165/bin/')# Office computer location
        from utility_belt.push_to_CKAN_resource import push_data_to_ckan, open_a_channel
        from utility_belt.gadgets import get_resource_parameter, get_package_name_from_resource_id
    except:
        from prime_ckan.push_to_CKAN_resource import push_data_to_ckan, open_a_channel
        #try:
        #    from prime_ckan.pipe_to_CKAN_resource import pipe_data_to_ckan
        #except:
        #    print("Unable to import pipe_data_to_ckan")
        from prime_ckan.gadgets import get_resource_parameter, get_package_name_from_resource_id


DEFAULT_TIMECHUNK = timedelta(minutes=10)

last_date_cache = None
all_day_ps_cache = []
dts_cache = []

last_utc_date_cache = None
utc_ps_cache = []
utc_dts_cache = []

temp_zone_info = {'344 - 18th & Carson Lot': {'Latitude': 40.428484093957401,
                 'Longitude': -79.98027965426445,
                 'MeterCount': 2,
                 'Type': 'Lot'},
 '345 - 20th & Sidney Lot': {'Latitude': 40.429380412222464,
                 'Longitude': -79.980572015047073,
                 'MeterCount': 2,
                 'Type': 'Lot'},
 '343 - 19th & Carson Lot': {'Latitude': 40.428526970691195,
                 'Longitude': -79.978395402431488,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '345 - 20th & Sidney Lot': {'Latitude': 40.429216054112679,
                 'Longitude': -79.977073073387146,
                 'MeterCount': 2,
                 'Type': 'Lot'},
 '338 - 42nd & Butler Lot': {'Latitude': 40.47053200000002,
                 'Longitude': -79.960346247850453,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '337 - 52nd & Butler Lot': {'Latitude': 40.481067498214522,
                 'Longitude': -79.953901635581985,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '311 - Ansley Beatty Lot': {'Latitude': 40.463049472104458,
                 'Longitude': -79.926414303372439,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '355 - Asteroid Warrington Lot': {'Latitude': 40.421746663239325,
                 'Longitude': -79.993341658895474,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '425 - Bakery Sq': {'Latitude': 40.4560281126722,
                'Longitude': -79.916535012428085,
                'MeterCount': 4,
                'Type': 'On street'},
 '321 - Beacon Bartlett Lot': {'Latitude': 40.435453694403037,
                 'Longitude': -79.923617310019822,
                 'MeterCount': 3,
                 'Type': 'Lot'},
 '363 - Beechview Lot': {'Latitude': 40.411083915458534,
                 'Longitude': -80.024386919130848,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '418 - Beechview': {'Latitude': 40.409913479391079,
                'Longitude': -80.024733782184967,
                'MeterCount': 8,
                'Type': 'On street'},
 '406 - Bloomfield (On-street)': {'Latitude': 40.461946760727805,
                 'Longitude': -79.946826139799441,
                 'MeterCount': 70,
                 'Type': 'On street'},
 '361 - Brookline Lot': {'Latitude': 40.392674122243058,
                 'Longitude': -80.018725208992691,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '419 - Brookline': {'Latitude': 40.393688357340416,
                'Longitude': -80.019989138111754,
                'MeterCount': 21,
                'Type': 'On street'},
 '351 - Brownsville & Sandkey Lot': {'Latitude': 40.384849483758344,
                 'Longitude': -79.977419455740346,
                 'MeterCount': 2,
                 'Type': 'Lot'},
 '416 - Carrick': {'Latitude': 40.386373443728381,
              'Longitude': -79.97945490478287,
              'MeterCount': 9,
              'Type': 'On street'},
 '329 - Centre Craig': {'Latitude': 40.45168996155256,
              'Longitude': -79.95195418596267,
              'MeterCount': 1,
              'Type': 'Lot'},
 '323 - Douglas Phillips Lot': {'Latitude': 40.432617056862256,
                 'Longitude': -79.922537281579963,
                 'MeterCount': 2,
                 'Type': 'Lot'},
 '401 - Downtown 1': {'Latitude': 40.441775562513982,
                'Longitude': -79.998573266419925,
                'MeterCount': 41,
                'Type': 'On street'},
 '402 - Downtown 2': {'Latitude': 40.438541198850679,
                'Longitude': -80.001387482255666,
                'MeterCount': 58,
                'Type': 'On street'},
 '342 - East Carson Lot': {'Latitude': 40.42911498849881,
                 'Longitude': -79.98570442199707,
                 'MeterCount': 2,
                 'Type': 'Lot'},
 '412 - East Liberty': {'Latitude': 40.460954767837613,
              'Longitude': -79.926159897229695,
              'MeterCount': 51,
              'Type': 'On street'},
 '371 - East Ohio Street Lot': {'Latitude': 40.454243200345864,
                 'Longitude': -79.999740015542329,
                 'MeterCount': 3,
                 'Type': 'Lot'},
 '307 - Eva Beatty Lot': {'Latitude': 40.461651797420089,
                 'Longitude': -79.927785198164941,
                 'MeterCount': 2,
                 'Type': 'Lot'},
 '324 - Forbes Murray Lot': {'Latitude': 40.438609122362699,
                 'Longitude': -79.922507232308064,
                 'MeterCount': 3,
                 'Type': 'Lot'},
 '322 - Forbes Shady Lot': {'Latitude': 40.438602290037359,
                 'Longitude': -79.920121894069666,
                 'MeterCount': 3,
                 'Type': 'Lot'},
 '335 - Friendship Cedarville Lot': {'Latitude': 40.462314291429955,
                 'Longitude': -79.948193852761278,
                 'MeterCount': 2,
                 'Type': 'Lot'},
 '331 - Homewood Zenith Lot': {'Latitude': 40.455562043993496,
                 'Longitude': -79.89687910306202,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '328 - Ivy Bellefonte Lot': {'Latitude': 40.45181388509701,
                 'Longitude': -79.933232609325415,
                 'MeterCount': 5,
                 'Type': 'Lot'},
 '325 - JCC/Forbes Lot': {'Latitude': 40.437756155476606,
            'Longitude': -79.923901042327884,
            'MeterCount': 2,
            'Type': 'Off street'},
 '405 - Lawrenceville': {'Latitude': 40.467721251303139,
                'Longitude': -79.963118098839757,
                'MeterCount': 29,
                'Type': 'On street'},
 '369 - Main/Alexander Lot': {'Latitude': 40.440717969032434,
                 'Longitude': -80.03386820671949,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '414 - Mellon Park': {'Latitude': 40.45172469595348,
                'Longitude': -79.919594841104498,
                'MeterCount': 4,
                'Type': 'On street'},
 '420 - Mt. Washington': {'Latitude': 40.432932025800348,
              'Longitude': -80.010913107390707,
              'MeterCount': 18,
              'Type': 'On street'},
 '422 - Northshore': {'Latitude': 40.447064541266613,
                 'Longitude': -80.008874122734966,
                 'MeterCount': 30,
                 'Type': 'On street'},
 '421 - NorthSide': {'Latitude': 40.454215096885378,
                'Longitude': -80.008679951361657,
                'MeterCount': 81,
                'Type': 'On street'},
 '407 - Oakland 1': {'Latitude': 40.440712434300536,
               'Longitude': -79.962027559420548,
               'MeterCount': 48,
               'Type': 'On street'},
 '408 - Oakland 2': {'Latitude': 40.443878246794903,
               'Longitude': -79.956351936149389,
               'MeterCount': 41,
               'Type': 'On street'},
 '409 - Oakland 3': {'Latitude': 40.447221532200416,
               'Longitude': -79.951424734414488,
               'MeterCount': 63,
               'Type': 'On street'},
 '410 - Oakland 4': {'Latitude': 40.441311089931347,
               'Longitude': -79.94689005613327,
               'MeterCount': 45,
               'Type': 'On street'},
 '375 - Oberservatory Hill Lot': {'Latitude': 40.490002153374341,
                 'Longitude': -80.018556118011475,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '314 - Penn Circle NW Lot': {'Latitude': 40.463423581089359,
                 'Longitude': -79.926107418017466,
                 'MeterCount': 2,
                 'Type': 'Lot'},
 '411 - Shadyside': {'Latitude': 40.455189648283827,
                 'Longitude': -79.935153703219399,
                 'MeterCount': 52,
                 'Type': 'On street'},
 '301 - Sheridan Harvard Lot': {'Latitude': 40.462616226637564,
                 'Longitude': -79.923065044145574,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '302 - Sheridan Kirkwood Lot': {'Latitude': 40.46169199390453,
                 'Longitude': -79.922711968915323,
                 'MeterCount': 3,
                 'Type': 'Lot'},
 '357 - Shiloh Street Lot': {'Latitude': 40.429924701959528,
               'Longitude': -80.007599227402991,
               'MeterCount': 3,
               'Type': 'Lot'},
 '415 - SS & SSW': {'Latitude': 40.428051479201962,
                'Longitude': -79.975047048707509,
                'MeterCount': 99,
                'Type': 'On street'},
 '413 - Squirrel Hill': {'Latitude': 40.433581368049765,
                'Longitude': -79.92309870425791,
                'MeterCount': 55,
                'Type': 'On street'},
 '404 - Strip Disctrict': {'Latitude': 40.45040837184569,
                'Longitude': -79.985526114383774,
                'MeterCount': 58,
                'Type': 'On street'},
 '304 - Tamello Beatty Lot': {'Latitude': 40.46097078534487,
                 'Longitude': -79.927121205522525,
                 'MeterCount': 2,
                 'Type': 'Lot'},
 '334 - Taylor Street Lot': {'Latitude': 40.463318543844693,
               'Longitude': -79.950406186508189,
               'MeterCount': 1,
               'Type': 'Lot'},
 '403 - Uptown': {'Latitude': 40.439793439383763,
               'Longitude': -79.984900553021831,
               'MeterCount': 69,
               'Type': 'On street'},
 '354 - Walter/Warrington Lot': {'Latitude': 40.42172215989536,
                 'Longitude': -79.995026086156827,
                 'MeterCount': 1,
                 'Type': 'Lot'},
 '423 - West End': {'Latitude': 40.441325754999475,
               'Longitude': -80.033656060668363,
               'MeterCount': 6,
               'Type': 'On street'}}

def roundTime(dt=None, roundTo=60):
   """Round a datetime object to any time laps[e] in seconds
   dt : datetime.datetime object, default now.
   roundTo : Closest number of seconds to round to, default 1 minute.
   Author: Thierry Husson 2012 - Use it as you want but don't blame me.
   """
   if dt == None : dt = datetime.now()
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = (seconds+roundTo/2) // roundTo * roundTo
   return dt + timedelta(0,rounding-seconds,-dt.microsecond)

def beginning_of_day(dt=None):
    # Takes a datetime and returns the first datetime before
    # that that corresponds to LOCAL midnight (00:00).
    if dt == None : dt = datetime.now()
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)

def terminal_of(p,t_guids,terminals):
    t = terminals[t_guids.index(p['@TerminalGuid'])]
    return t

def p_hash(p,t):
    # Use the combination of the original purchase date for the session
    # and the parking zone that it happened in as a unique identifier
    # to link transactions that are extensions of an original parking
    # purchase back to that first purchase.
    return "{}|{}".format(p['@PurchaseDateLocal'],numbered_zone(t['@Id']))

def is_original(p,t,p_history,previous_history):
    # This function does an initial check to see if a particular
    # transaction might be an extension/"TopUp" of a previous
    # purchase.


    # @PurchaseTypeName == Normal might be a usable way of 
    # identifying original purhcases (though I'm not sure
    # that all TopUps will be identified as such).


    # [ ] Maybe check apparent rate against official rate for the terminal.
    # (Unfortunately, that could only be done for live data.
    # Older purchases may have different rates that can only
    # be inferred from looking at other purchases from that day.)

    #if p['@TerminalGuid'] in t_guids:
        # ValueError: u'401511-WOODST0402' is not in list
    #    t = terminals[t_guids.index(p['@TerminalGuid'])]
    #else:
    #    t = None
    p_key = p_hash(p,t)
    return ((p_key not in p_history) and (p_key not in previous_history))

def find_predecessors(p,t,t_guids,terminals,p_history,previous_history):
    if previous_history != {}:
        predecessors = p_history[p_hash(p,t)] + previous_history[p_hash(p,t)]
    else:
        predecessors = p_history[p_hash(p,t)]
    try: # Sort predecessors for purchase p by EndDateLocal.
        sps = sorted(predecessors, key=lambda x: x['@EndDateLocal'])
    except:
        print("len(predecessors) = {}".format(len(predecessors)))
        for pred in predecessors:
            if '@EndDateLocal' not in pred:
                print("Missing '@EndDateLocal':")
                pprint.pprint(to_dict(pred))
        raise ValueError("Found a transaction that is missing @EndDateLocal")

    # Use some sanity checks to filter out transactions that are not actually
    # related to the purchase p (that is, they're for other cars).
    if len(sps) > 0:
        for sp in sps: # Check that payments are all either physical or virtual.
            if is_virtual(t) != is_virtual(terminal_of(sp,t_guids,terminals)):
                sps.remove(sp)
    if len(sps) > 0:
        latest = p
        # Eliminate earlier purchases that already have more paid-for
        # minutes (represented by the field @Units) than the purchase p.
        for sp in sps[1::-1]:
            if int(sp['@Units']) >= int(latest['@Units']):
                sps.remove(sp)
            else:
                latest = sp
    return sps

def add_to_dict(p,p_dict,terminals,t_guids):
    t = terminals[t_guids.index(p['@TerminalGuid'])]
    p_dict[p_hash(p,t)].append(p)
    return p_dict

def reframe(p,terminals,t_guids,p_history,previous_history,turbo_mode):
    # Take a dictionary and generate a new dictionary from it that samples
    # the appropriate keys and renames and transforms as desired.
    t_A = time.time()
    row = {}
    #row['GUID'] = p['@PurchaseGuid'] # To enable this field,
    # get_batch_parking_for_day needs to be tweaked and
    # JSON caches need to be regenerated.
    try:
        row['TerminalGUID'] = p['@TerminalGuid'] # This is useful
    # for connecting purchases with terminals when the ID changes
    # but the GUID does not change.
    except:
        print("p['@TerminalGuid'] = {}".format(p))
    row['TerminalID'] = p['@TerminalID']
    if p['@TerminalGuid'] in t_guids:
        t = terminals[t_guids.index(p['@TerminalGuid'])]
        row['Latitude'] = value_or_blank('Latitude',t)
        row['Longitude'] = value_or_blank('Longitude',t)
        # Maybe these should be value_or_none instead.
        row['List_of_ad_hoc_groups'] = ad_hoc_groups(t)

    row['Amount'] = float(p['@Amount'])
    t = terminals[t_guids.index(p['@TerminalGuid'])]

    row['Duration'] = int(p['@Units']) # in minutes
# THIS ASSUMES THAT DURATIONS WILL ALWAYS BE IN MINUTES.
# IT WOULD BE NICE FOR THEM TO BE SO TO SIMPLIFY THE COUNTS
# DATA STRUCTURE

    t_B = time.time()
    if not turbo_mode and not is_original(p,t,p_history,previous_history): # This is an initial test, but
    # some of the hits that it can return may still be rejected
    # by the find_predecessors function.
        t_C = time.time()
    # [ ] One option would be to change the p_history dict to
    # correctly separate purchases into sessions.
    # [ ] Another option would be just to eliminate the is_original
    # function and replace it entirely with find_predecessors.
        predecessors = find_predecessors(p,t,t_guids,terminals,p_history,previous_history)
        #print("--------------------")
        #pprint.pprint(p)
        #print("---")
        #pprint.pprint(predecessors)
        #if len(predecessors) > 0:
        #    print("last predecessor = {} ".format(predecessors[-1]))
        print("durations = {}".format([int(x['@Units']) for x in predecessors + [p]]))
        #        print("We need to subtract the durations of the previous payments.")
        # Often only one value is printed to the console for a given purchase:
        # For example:
        #           durations = [45]
        # This means that is_original(p,t,p_history) was False and some other
        # purchase in the history had the same PurchaseDateLocal and numbered
        # zone, but one of the filters in find_predecessors eliminated this
        # one.

        if len(predecessors) > 0:
            # Subtract off the cumulative minutes purchased by the most recent
            # predecssor, so that the Duration field represents just the Duration
            # of this transaction.
            row['Duration'] -= int(predecessors[-1]['@Units'])


     
    #row['Extension'] = a Boolean that indicates whether this purchase is
    # an extension of a previous purchase.
    # Options for keeping statistics on such extensions:
    # 1) Delete all older records as they come up.
    # 2) Reduce the Duration of the extension
#    row['StartDateUTC'] = datetime.strptime(p['@StartDateUtc'],'%Y-%m-%dT%H:%M:%S')
    #'2016-09-19T13:47:16',
#    row['StartDateLocal'] = datetime.strptime(p['@StartDateLocal'],'%Y-%m-%dT%H:%M:%S')
#    row['PurchaseDateLocal'] = datetime.strptime(p['@PurchaseDateLocal'],'%Y-%m-%dT%H:%M:%S')

#    row['PayIntervalStartUTC'] = datetime.strptime(p['@PayIntervalStartUtc'],'%Y-%m-%dT%H:%M:%S')

#    row['EndDateUTC'] = datetime.strptime(p['@EndDateUtc'],'%Y-%m-%dT%H:%M:%S')

#    if '@EndDateUtc' in p:
#        end_time = datetime.strptime(p['@EndDateUtc'],'%Y-%m-%dT%H:%M:%S')
        # Some of the JSON files don't have this field because
        # I eliminated it from the more recent saved-file format.
        # Thus, maybe this whole 'Done' field is no longer needed
        # since it doesn't seem to be used anywhere.

    # Other EndDate parameters will likely be needed here for detecting
    # payment extensions.


#        utc_end_time = (pytz.utc).localize(end_time)
#        row['Done'] = utc_end_time  < datetime.now(pytz.utc)
    t_D = time.time()
    #print("t_B - t_A = {:1.2e}, t_C-t_B = {:1.2e}, t_D-t_C = {:1.2e}".format(t_B-t_A,t_C-t_B,t_D-t_C))

    return row

     # Return list of OrderedDicts where each OrderedDict
     # represents a row for a parking zone (or lot) and time slot.
     # ZONE     START_DATETIME  END_DATETIME  TRANSACTIONS
     #    CAR-HOURS   MONEY  LOT/VIRTUAL/OTHER INFORMATION


def find_biggest_value(d_of_ds,field='Transactions'):
    return sorted(d_of_ds,key=lambda x:d_of_ds[x][field])[-1]

def update_occupancies(inferred_occupancy,stats_by_zone,slot_start,timechunk):
    delta_minutes = timechunk.total_seconds()/60.0
    for zone in stats_by_zone:
        durations = json.loads(stats_by_zone[zone]['Durations'])
#        if len(durations) > 0:
#            print "\ndurations for zone {} = ".format(zone)
#            pprint.pprint(durations)
        for d_i in durations:
            bins = int(round(float(d_i)/delta_minutes))
            # Rounding like this means that for a timechunk of 10 minutes,
            # 1-4 minute parking sessions will not add to inferred
            # occupancy, while 5-14 minute sessions will add ten minutes
            # of apparent occupancy. This will work perfectly if the
            # timechunk is one minute (i.e., no occupancy will be lost
            # due to rounding errors).
            for k in range(0,bins):
                inferred_occupancy[slot_start+k*timechunk][zone] += durations[d_i]
#        if len(durations) > 0:
#            print "inferred_occpancy for zone {} =".format(zone)
#            for t in sorted(inferred_occupancy.keys()):
#                print t, to_dict(inferred_occupancy[t])
    return inferred_occupancy

def initialize_zone_stats(start_time,end_time,aggregate_by,tz=pytz.timezone('US/Eastern')):
    stats = {}
    start_time_pgh = start_time.astimezone(tz)
    stats['Start'] = datetime.strftime(start_time_pgh,"%Y-%m-%d %H:%M:%S")
    # [ ] Is this the correct start time?
    end_time_pgh = end_time.astimezone(tz)
    stats['End'] = datetime.strftime(end_time_pgh,"%Y-%m-%d %H:%M:%S")
    start_time_utc = start_time.astimezone(pytz.utc)
    stats['UTC Start'] = datetime.strftime(start_time_utc,"%Y-%m-%d %H:%M:%S")
    stats['Transactions'] = 0
    stats['Car-minutes'] = 0
    stats['Payments'] = 0.0
    stats['Durations'] = [] # The Durations field represents the durations of the purchases
    # made during this time slot. Just as Transactions indicates how many times people
    # put money into parking meters (or virtual meters via smartphone apps) and
    # Payments tells you how much money was paid, Durations tells you the breakdown of
    # parking minutes purchased. The sum of all the durations represented in the
    # Durations dictionary should equal the value in the Car-minutes field.
    if aggregate_by == 'ad hoc zone':
        stats['Parent Zone'] = None
    return stats

def distill_stats(rps,terminals,t_guids,t_ids, start_time,end_time, zone_kind='old', aggregate_by='zone', parent_zones=[], tz=pytz.timezone('US/Eastern')):
    # Originally this function just aggregated information
    # between start_time and end_time to the zone level.

    # Then it was modified to support ad hoc zones,
    # allowing the function to be called separately just to
    # get ad-hoc-zone-level aggregation.

    # THEN it was modified to also allow aggregation by
    # meter ID instead of only by zone.
    stats_by = {}
    for k,rp in enumerate(rps):
        t_guid = rp['TerminalGUID']
        t_id = rp['TerminalID']
        zone = None
        zones = []
        if aggregate_by == 'zone':
            if zone_kind == 'new':
                zone = numbered_zone(t_id)
            elif t_guid in t_guids:
                t = terminals[t_guids.index(t_guid)]
                if zone_kind == 'old':
                    zone = corrected_zone_name(t) # Changed
                    # from zone_name(t) to avoid getting
                    # transactions in "Z - Inactive/Removed Terminals".
            else:
                print("OH NO!!!!!!!!!!!\n THE TERMINAL FOR THIS PURCHASE CAN NOT BE FOUND\n BASED ON ITS GUID!!!!!!!!!!!!!!!")
                if zone_kind == 'old':
                    zone = corrected_zone_name(None,t_ids,rp['TerminalID'])

            if zone is not None:
                zones = [zone]
        elif aggregate_by == 'ad hoc zone':
            if 'List_of_ad_hoc_groups' in rp and rp['List_of_ad_hoc_groups'] != []:
                zones = rp['List_of_ad_hoc_groups']
# The problem with this is that a given purchase is associated with a terminal which may have MULTIPLE ad hoc zones. Therefore, each ad hoc zone must have its own parent zone(s).
        elif aggregate_by == 'meter':
            zones = [t_guid] # [ ] Should this be GUID or just ID?


            # The above could really stand to be refactored
        if zones != []:
            for zone in censor(zones):
                if zone not in stats_by:
                    stats_by[zone] = initialize_zone_stats(start_time,end_time,aggregate_by,tz=pytz.timezone('US/Eastern'))
                if aggregate_by == 'ad hoc zone':
                    if 'Parent Zone' in stats_by[zone]:
                        stats_by[zone]['Parent Zone'] = '|'.join(parent_zones[zone])
                elif aggregate_by == 'meter':
                    stats_by[zone]['Meter ID'] += rp['TerminalID']
                stats_by[zone]['Transactions'] += 1
                stats_by[zone]['Car-minutes'] += rp['Duration']
                stats_by[zone]['Payments'] += rp['Amount']
                stats_by[zone]['Durations'].append(rp['Duration'])

    for zone in stats_by.keys():
        counted = Counter(stats_by[zone]['Durations'])
        stats_by[zone]['Durations'] = json.dumps(counted, sort_keys=True)
        stats_by[zone]['Payments'] = float(round_to_cent(stats_by[zone]['Payments']))

    return stats_by

def build_url(base_url,slot_start,slot_end):
    # This function takes the bounding datetimes, checks that
    # they have time zones, and builds the appropriate URL,
    # converting the datetimes to UTC (which is what the CALE
    # API expects).

    # This function is called by both get_recent_parking_events
    # and get_batch_parking_for_day.

    if is_timezoneless(slot_start) or is_timezoneless(slot_end):
        raise ValueError("Whoa, whoa, whoa! One of those times is unzoned!")
    # Since a slot_end that is too far in the future results 
    # in a 400 (reason = "Bad Request"), limit how far in 
    # the future slot_end may be
    arbitrary_limit = datetime.now(pytz.utc) + timedelta(hours = 1)
    if slot_end.astimezone(pytz.utc) > arbitrary_limit:
        slot_end = arbitrary_limit

    date_format = '%Y-%m-%d'
    time_format = '%H%M%S'
    url_parts = [slot_start.astimezone(pytz.utc).strftime(date_format),
        slot_start.astimezone(pytz.utc).strftime(time_format),
        slot_end.astimezone(pytz.utc).strftime(date_format),
        slot_end.astimezone(pytz.utc).strftime(time_format)]

    url = base_url + '/'.join(url_parts)
    return url

def convert_doc_to_purchases(doc,slot_start,date_format):
    if 'Purchases' not in doc:
        print("Failed to retrieve records for UTC time {}".format(slot_start.astimezone(pytz.utc).strftime(date_format)))
        # Possibly an exception should be thrown here.
        return None
    if doc['Purchases']['@Records'] == '0':
        return []
    ps = doc['Purchases']['Purchase']
    if type(ps) == list:
        #print "Found a list!"
        return ps
    if type(ps) == type(OrderedDict()):
        #print "It's just one OrderedDict. Let's convert it to a list!"
        return [ps]
    print("Found something else of type {}".format(type(ps)))
    return []

def cull_fields(ps):
    # Remove a bunch of unneeded fields.
    purchases = remove_field(ps,'@Code')
    purchases = remove_field(purchases,'@ArticleID')
    purchases = remove_field(purchases,'@ArticleName')
    purchases = remove_field(purchases,'@CurrencyCode')
    purchases = remove_field(purchases,'@VAT')
    # Other fields that could conceivably be removed:
    # @ExternalID, @PurchaseStateName, some fields in PurchasePayUnit, maybe others

    # Filtering out a lot more fields to try to slim down the amount of data:
    #purchases = remove_field(purchases,'@PurchaseGuid')
    #purchases = remove_field(purchases,'@TerminalGuid')
    #purchases = remove_field(purchases,'@PurchaseDateUtc')#
    #purchases = remove_field(purchases,'@PayIntervalStartLocal')#
    #purchases = remove_field(purchases,'@PayIntervalStartUtc')#
    #purchases = remove_field(purchases,'@PayIntervalEndLocal')#
    #purchases = remove_field(purchases,'@PayIntervalEndUtc')#
    #purchases = remove_field(purchases,'@EndDateLocal')
    #purchases = remove_field(purchases,'@EndDateUtc')#
    #purchases = remove_field(purchases,'@PaymentServiceType')
    purchases = remove_field(purchases,'@TicketNumber')
    purchases = remove_field(purchases,'@TariffPackageID')
    purchases = remove_field(purchases,'@ExternalID')#
    purchases = remove_field(purchases,'@PurchaseStateName')
    purchases = remove_field(purchases,'@PurchaseTriggerTypeName')
    #purchases = remove_field(purchases,'@PurchaseTypeName')#
    purchases = remove_field(purchases,'@MaskedPAN','PurchasePayUnit')
    purchases = remove_field(purchases,'@BankAuthorizationReference','PurchasePayUnit')
    purchases = remove_field(purchases,'@CardFeeAmount','PurchasePayUnit')
    purchases = remove_field(purchases,'@PayUnitID','PurchasePayUnit')
    purchases = remove_field(purchases,'@TransactionReference','PurchasePayUnit')
    purchases = remove_field(purchases,'@CardIssuer','PurchasePayUnit')

    return purchases

def get_day_from_json_or_api(slot_start,tz,filename,date_format,cache=True,mute=False):
    # Caches parking once it's been downloaded and checks
    # cache before redownloading.

    # Note that no matter what time of day is associated with slot_start,
    # this function will get all of the transactions for that entire day.
    # Filtering the results down to the desired time range is handled 
    # elsewhere (in the calling function (get_batch_parking)).


    # Caching by date ties this approach to a particular time zone. This
    # is why transactions are dropped if we send this function a UTC
    # slot_start (I think).

    if not os.path.isfile(filename) or os.stat(filename).st_size == 0:
        if not mute:
            print("Sigh! {} not found, so I'm pulling the data from the API...".format(filename))

        slot_start = beginning_of_day(slot_start.astimezone(tz))
        slot_end = slot_start + timedelta(days = 1)

        base_url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/BatchDataExport/4/BatchDataExport.svc/purchase/ticket/'
        url = build_url(base_url,slot_start,slot_end)

        if not mute:
            print("Here's the URL: {}".format(url))
        r = requests.get(url, auth=(CALE_API_user, CALE_API_password))

        # Convert Cale's XML into a Python dictionary
        doc = xmltodict.parse(r.text,encoding = r.encoding)


        url2 = doc['BatchDataExportResponse']['Url']
        r2 = requests.get(url2, auth=(CALE_API_user, CALE_API_password))
        doc = xmltodict.parse(r2.text,encoding = r2.encoding)

        while not r2.ok or doc['BatchDataExportFileResponse']['ExportStatus'] != 'Processed':
            time.sleep(10)
            r2 = requests.get(url2, auth=(CALE_API_user, CALE_API_password))
            doc = xmltodict.parse(r2.text,encoding = r2.encoding)

        url3 = doc['BatchDataExportFileResponse']['Url']

        # When the ZIP file is ready:
        r3 = requests.get(url3, stream=True, auth=(CALE_API_user, CALE_API_password))
        while not r3.ok:
            time.sleep(5)
            r3 = requests.get(url3, stream=True, auth=(CALE_API_user, CALE_API_password))

        z = zipfile.ZipFile(BytesIO(r3.content))

        # Extract contents of a one-file zip file to memory:
        xml = z.read(z.namelist()[0])
        doc = xmltodict.parse(xml,encoding = 'utf-8')

        ps = convert_doc_to_purchases(doc['BatchExportRoot'],slot_start,date_format)

        purchases = cull_fields(ps)

        if cache:
            try: # Python 3 file opening
                with open(filename, "w") as f:
                    json.dump(purchases,f,indent=2)
            except: # Python 2 file opening
                with open(filename, "wb") as f:
                    json.dump(purchases,f,indent=2)
    else: # Load locally cached version
        try: # Python 3 file opening
            with open(filename, "r", encoding="utf-8") as f:
                ps = json.load(f)
        except: # Python 2 file opening
            with open(filename,'rb') as f:
                ps = json.load(f)

    return ps

def get_batch_parking_for_day(slot_start,tz,cache=True,mute=False):
    # Caches parking once it's been downloaded and checks
    # cache before redownloading.

    # Note that no matter what time of day is associated with slot_start,
    # this function will get all of the transactions for that entire day.
    # Filtering the results down to the desired time range is handled 
    # elsewhere (in the calling function (get_batch_parking)).


    # Caching by date ties this approach to a particular time zone. This
    # is why transactions are dropped if we send this function a UTC
    # slot_start (I think) and try to use the Eastern Time Zone JSON 
    # files. I am now trying to fix this by specifying the timezone 
    # and distinguishing between JSON-file folders.

    date_format = '%Y-%m-%d'

    dashless = slot_start.strftime('%y%m%d')
    if tz == pytz.utc:
        filename = path + "utc_json/"+dashless+".json"
    else:
        filename = path + "json/"+dashless+".json"
    
    ps = get_day_from_json_or_api(slot_start,tz,filename,date_format,cache,mute)

    return ps

def get_batch_parking(slot_start,slot_end,cache,mute=False,tz=pytz.timezone('US/Eastern'),time_field = '@PurchaseDateLocal',dt_format='%Y-%m-%dT%H:%M:%S'):
    # This function handles situation where slot_start and slot_end are on different days
    # by calling get_batch_parking_for_day in a loop.

    # The parameter "time_field" determines which of the timestamps is used for calculating
    # the datetime values used to filter purchases down to those between slot_start
    # and start_end.


    # Note that the time zone tz and the time_field must be consistent for this to work properly.
    # Here is a little sanity check:
    
    if (re.search('Utc',time_field) is not None) != (tz == pytz.utc): 
        # This does an XOR between these values.
        raise RuntimeError("It looks like time_field may not be consistent with the provided time zone")

    global last_date_cache, all_day_ps_cache, dts_cache
    if last_date_cache != slot_start.date():
        if not mute:
            print("last_date_cache ({}) doesn't match slot_start.date() ({})".format(last_date_cache, slot_start.date()))

        ps_all = []
        dt_start_i = slot_start
        while dt_start_i < slot_end:
            ps_for_whole_day = get_batch_parking_for_day(dt_start_i,tz,cache,mute)
            ps_all += ps_for_whole_day
            dt_start_i += timedelta(days = 1)
            if not mute:
                print("Now there are {} transactions in ps_all.".format(len(ps_all)))

        all_day_ps_cache = ps_all # Note that if slot_start and slot_end are not on the same day,
        # all_day_ps_cache will hold transactions for more than just the date of slot_start, but 
        # since filtering is done further down in this function, this should not represent a 
        # problem. There should be no situations where more than two days of transactions will
        # wind up in this cache at any one time.
        dts_cache = [tz.localize(datetime.strptime(p[time_field],dt_format)) for p in ps_all]
        time.sleep(3)
    else:
        ps_all = all_day_ps_cache
    #ps = [p for p in ps_all if slot_start <= tz.localize(datetime.strptime(p[time_field],'%Y-%m-%dT%H:%M:%S')) < slot_end] # This takes like 3 seconds to
    # execute each time for busy days since the time calculations
    # are on the scale of tens of microseconds.
    # So let's generate the datetimes once (above), and do
    # it this way:
    ps = [p for p,dt in zip(ps_all,dts_cache) if slot_start <= dt < slot_end]
    # Now instead of 3 seconds it takes like 0.03 seconds.
    last_date_cache = slot_start.date()
    return ps
########################
def epoch_time(dt):
    """Convert a datetime to the UNIX epoch time."""
   # Be sure to do all this in UTC.  

    if is_timezoneless(dt):
        raise ValueError("Whoa, whoa, whoa! That time is unzoned!")

    if dt.tzinfo != pytz.utc:
        dt = copy(dt).astimezone(pytz.utc)

    try:
        return dt.timestamp() # This only works in Python 3.3 and up.
    except:
        return (dt-(pytz.utc).localize(datetime(1970,1,1,0,0,0))).total_seconds()

# database functions #
def create_db(db_filename):
    db = dataset.connect('sqlite:///'+db_filename)
    db.create_table('cached_utc_dates', primary_id='date', primary_type='String')
    db.create_table('cached_purchases', primary_id='@PurchaseGuid', primary_type='String')
    cached_ps = db['cached_purchases']
    cached_ps.create_index(['unix_time']) # Creating this index should massively speed up queries.
    return db

def create_or_connect_to_db(db_filename):
    # It may be possible to do some of this more succinctly by using 
    # get_table(table_name, primary_id = 'id', primary_type = 'Integer)
    # to create the table if it doesn't already exist.
    print("Checking for path+db_filename = {}".format(path+db_filename))
    if os.path.isfile(path+db_filename):
        try:
            db = dataset.connect('sqlite:///'+db_filename)
            print("Database file found with tables {}.".format(db.tables))
            # Verify that both tables are present.
            cached_ds = db.load_table('cached_utc_dates')
            sorted_ds = cached_ds.find(order_by=['date'])
            cds = list(cached_ds.all())
    
            d_strings = sorted([list(d.values())[0] for d in cds])
            if len(cds) > 0:
                print("cached_utc_dates loaded. It contains {} dates. The first is {}, and the last is {}.".format(len(cds), d_strings[0], d_strings[-1]))
            _ = db.load_table('cached_purchases')
            print("Both tables found.")
        except sqlalchemy.exc.NoSuchTableError as e:
            print("Unable to load at least one of the tables.")
            os.remove(db_filename)
            db = create_db(db_filename)
            print("Deleted file and created new database.")
            cds = db.load_table('cached_utc_dates')
            print("cached_utc_dates loaded again, as another test.")
    else:
        print("Database file not found. Creating new one.")
        db = create_db(db_filename)
    return db

def get_tables_from_db(db):
    """Convenience function to get the two cache tables from the cache database."""
    cached_dates = db['cached_utc_dates']
    cached_ps = db['cached_purchases']
    return cached_dates,cached_ps

def in_db(cached_dates,date_i):
    # Return a Boolean indicating whether the date is in cached_dates 
    # which specifies whether all events with StartDateUTC values 
    # equal to date_i have been cached in the database.
    date_format = '%Y-%m-%d'
    date_string = date_i.strftime(date_format)
    return (cached_dates.find_one(date=date_string) is not None)
# end database functions #

########################

def get_utc_ps_for_day_from_json(slot_start,cache=True,mute=False):
    # (This is designed to be the "from_somewhere" part of the function
    # formerly known as get_ps_from_somewhere.)
    ###
    # Caches parking once it's been downloaded (in individual JSON files) and checks
    # cache before redownloading.

    # Note that no matter what time of day is associated with slot_start,
    # this function will get all of the transactions for that entire day.
    # Filtering the results down to the desired time range is handled 
    # elsewhere (in the calling function (get_batch_parking)).
    ###############
    # This function gets parking purchases, either from a 
    # JSON cache (if the date appears to have been cached)
    # or else from the API (and then caches the whole thing 
    # if cache = True), by using get_batch_parking_for_day.

    # Note that no matter what time of day is associated with slot_start,
    # this function will get all of the transactions for that entire day.

    # Filtering the results down to the desired time range is now handled 
    # in this functions (though the function only marks a date as cached
    # when it has added all events from the day of slot_start (based on
    # StartDateUtc)).
    
    # This approach tries to address the problem of the often 
    # gigantic discrepancy between the DateCreatedUtc timestamp
    # and the StartDateUtc timestamp.

    ref_field = '@StartDateUtc' # This should not be changed.

    #for each date in day before, day of, and day after (to get all transactions)
        # [Three full days is probably slightly overkill since the most extreme 
        # negative case of DateCreatedUtc - StartDateUtc observed so far has 
        # been -12 hours.]

    # To avoid issues with missing or extra hours during Daylight Savings Time,
    # all slot times should be in UTC.

    # The cached_dates format is also UTC dates. (This change was made to 
    # route around issues encountered when using localized versions of 
    # StartDateUtc and converting to dates.)

    pgh = pytz.timezone('US/Eastern') # This time zone needs to be hard-coded since
    # get_batch_parking_for_day still only works when using the local time zone
    # (for some reason).
    #for offset in range(-1,2):
    ps_all = []
    dts_all = []
    for offset in range(0,2):
        query_start = (beginning_of_day(slot_start) + (offset)*timedelta(days = 1)).astimezone(pgh)

        ps = []
        dts = []
        t_start_fetch = time.time()
        ps_for_whole_day = get_batch_parking_for_day(query_start,pytz.utc,cache,mute)

        # Filter down to the events in the slot, adding on two date/time fields #
        datetimes = [(pytz.utc).localize(datetime.strptime(p[ref_field],'%Y-%m-%dT%H:%M:%S')) for p in ps_for_whole_day]
        #ps = [p for p,dt in zip(purchases,dts) if beginning_of_day(slot_start) <= dt < beginning_of_day(slot_start) + timedelta(days=1)]
        
        start_of_day = beginning_of_day(slot_start)
        start_of_next_day = beginning_of_day(slot_start) + timedelta(days=1)
        for purchase_i,datetime_i in zip(ps_for_whole_day,datetimes):
            if start_of_day <= datetime_i < start_of_next_day:
                ps.append(purchase_i)
                dts.append(datetime_i)
            if purchase_i['@PurchaseGuid'] == 'EE37C59D-F9AD-97E8-D296-1C0A5A683A67':
                print("FOUND IT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print("start_of_day <= datetime_i < start_of_next_day = {}".format(start_of_day <= datetime_i < start_of_next_day))
                pprint.pprint(purchase_i)

        t_end_fetch = time.time()
        if len(ps) > 0:
            print("  Time required to pull day {} (either from the API or from a JSON file): {} s  |  len(ps)/len(purchases) = {}".format(offset,t_end_fetch-t_start_fetch,len(ps)/len(ps_for_whole_day)))
        ps_all += ps
        dts_all += datetimes

    return ps_all

# ~~~~~~~~~~~~~~~~

# Attempting to split get_ps_from_somewhere into two functions to enable day-by-day
# caching, to reduce database hits, like in the previous approach.

def get_ps_for_day(db,slot_start,cache=True,mute=False):
    # (This is designed to be the "from_somewhere" part of the function
    # formerly known as get_ps_from_somewhere.)
    ###
    # Caches parking once it's been downloaded (in a database) and checks
    # cache before redownloading.

    # Note that no matter what time of day is associated with slot_start,
    # this function will get all of the transactions for that entire day.
    # Filtering the results down to the desired time range is handled 
    # elsewhere (in the calling function (get_batch_parking)).
    ###############
    # This function gets parking purchases, either from a 
    # cache database (if the date appears to have been cached)
    # or else from the API (and then caches the whole thing 
    # if cache = True).

    # Note that no matter what time of day is associated with slot_start,
    # this function will get all of the transactions for that entire day.

    # Filtering the results down to the desired time range is now handled 
    # in this functions (though the function only marks a date as cached
    # when it has added all events from the day of slot_start (based on
    # StartDateUtc)).
    
    # This approach tries to address the problem of the often 
    # gigantic discrepancy between the DateCreatedUtc timestamp
    # and the StartDateUtc timestamp.

    # This new database-centric, StartDateUtc-based approach has
    # been verified to give the same results as the old approach
    # for 2012-07-23.

    cached_dates,cached_ps = get_tables_from_db(db)

    ref_field = '@StartDateUtc' # This should not be changed.
    date_format = '%Y-%m-%d'

    tz = pytz.utc
    ps_all = []
    dts_all = []
    #for each date in day before, day of, and day after (to get all transactions)
        # [Three full days is probably slightly overkill since the most extreme 
        # negative case of DateCreatedUtc - StartDateUtc observed so far has 
        # been -12 hours.]

    # To avoid issues with missing or extra hours during Daylight Savings Time,
    # all slot times should be in UTC.

    # The cached_dates format is also UTC dates. (This change was made to 
    # route around issues encountered when using localized versions of 
    # StartDateUtc and converting to dates.)

    slot_start_date_string = slot_start.astimezone(tz).strftime(date_format) # This is the date string 
    # for the start of the overall desired time range.

    if cached_dates.find_one(date=slot_start_date_string) is None:
        print("cached_dates.find_one(date=slot_start_date_string)".format(cached_dates.find_one(date=slot_start_date_string)))
        # A date is added to cached_dates when the date and the two surrounding it have
        # been queried and all events with a StartDateUTC value in the day corresponding
        # to slot_start (as well as slot_start_date_string) have been added to the database.

        # That way, if the date is in cached_dates, the query can be done on the 
        # database without hitting the API.

        if not mute:
            print("Sigh! {} not found in cached_dates, so I'm pulling the data from the API...".format(slot_start_date_string))
        # Pull the data from the API.
        
        #for offset in range(-1,2):
        for offset in range(0,2):
            query_start = beginning_of_day(slot_start) + (offset)*timedelta(days = 1)
            query_end = query_start + timedelta(days = 1)

            #query_date_string = query_start.astimezone(tz).strftime(date_format) # This is the date
            # in the local time zone. # This seems to not be used at all.

            base_url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/BatchDataExport/4/BatchDataExport.svc/purchase/ticket/'
            url = build_url(base_url,query_start,query_end)

            t_start_dl = time.time()
            if not mute:
                print("Here's the URL for offset = {}: {}".format(offset,url))
            r = requests.get(url, auth=(CALE_API_user, CALE_API_password))

            # Convert Cale's XML into a Python dictionary
            doc = xmltodict.parse(r.text,encoding = r.encoding)

            url2 = doc['BatchDataExportResponse']['Url']
            r2 = requests.get(url2, auth=(CALE_API_user, CALE_API_password))
            doc = xmltodict.parse(r2.text,encoding = r2.encoding)

            while not r2.ok or doc['BatchDataExportFileResponse']['ExportStatus'] != 'Processed':
                time.sleep(10)
                r2 = requests.get(url2, auth=(CALE_API_user, CALE_API_password))
                doc = xmltodict.parse(r2.text,encoding = r2.encoding)

            url3 = doc['BatchDataExportFileResponse']['Url']

            # When the ZIP file is ready:
            r3 = requests.get(url3, stream=True, auth=(CALE_API_user, CALE_API_password))
            while not r3.ok:
                time.sleep(5)
                r3 = requests.get(url3, stream=True, auth=(CALE_API_user, CALE_API_password))
            
            z = zipfile.ZipFile(BytesIO(r3.content)) # This unzipping time can be like half a 
            # second and is sufficient buffer time between the previous API call and the next one.

            # Extract contents of a one-file zip file to memory:
            xml = z.read(z.namelist()[0])
            doc = xmltodict.parse(xml,encoding = 'utf-8')

            ps = convert_doc_to_purchases(doc['BatchExportRoot'],query_start,date_format)

            t_end_dl = time.time()


            # Remove fields #
            purchases = cull_fields(ps)

            # Filter down to the events in the slot, adding on two date/time fields #
            datetimes = [(pytz.utc).localize(datetime.strptime(p[ref_field],'%Y-%m-%dT%H:%M:%S')) for p in purchases]
            #ps = [p for p,dt in zip(purchases,dts) if beginning_of_day(slot_start) <= dt < beginning_of_day(slot_start) + timedelta(days=1)]
            ps = []
            dts = []
            
            start_of_day = beginning_of_day(slot_start)
            start_of_next_day = beginning_of_day(slot_start) + timedelta(days=1)
            for purchase_i,datetime_i in zip(purchases,datetimes):
                if start_of_day <= datetime_i < start_of_next_day:
                    purchase_i['StartDateUTC_date'] = (datetime_i).astimezone(tz).strftime(date_format) # This SHOULD be equal to slot_start.date().........
                    # but verify this.
                    # [ ] Assuming that these two things are equal would speed up processing a little.
                    if purchase_i['StartDateUTC_date'] != slot_start_date_string:
                        print("slot_start = {} = {}".format(slot_start, slot_start.astimezone(pytz.utc)))
                        print("slot_start_date_string (This is the local date) = {}".format(slot_start_date_string))
                        print("beginning_of_day(slot_start) = {}".format(start_of_day))
                        print("datetime_i = {}".format(datetime_i))
                        print("beginning_of_day(slot_start) + timedelta(days=1) = {}".format(start_of_next_day))
                        pprint.pprint(purchase_i)
                        raise ValueError("purchase_i['StartDateUTC_date'] != slot_start_date_string, {} != {}".format(purchase_i['StartDateUTC_date'], slot_start_date_string))
                    ps.append(purchase_i)
                    dts.append(datetime_i)

            for p,dt in zip(ps,dts):
                p['unix_time'] = epoch_time(dt)
                # This is a hack to provide a float that can be stored in SQLite (which has serious problems with datetime 
                # comparisons) until I can get a Postgres database set up.
            
            # Add these purchases to the overall accumulating set that fit in the slot #
            ps_all += ps #ps_all is all purchases that have StartDateUtc values between the beginning of the day corresponding to slot_start
            # and the beginning of the next day (24 hours later, in UTC).
            dts_all += dts         
            
            if len(purchases) > 0:
                print("  Time required to pull day {} from the API: {} s  |  len(ps)/len(purchases) = {}".format(offset,t_end_dl-t_start_dl,len(ps)/len(purchases)))

        if cache:
            # Store in db and update cached_dates.

            ps_all_fixed = remove_field(ps_all,'PurchasePayUnit') # PurchasePayUnit itself contains a data structure
            # so dataset can't handle sticking it into a databse.
            # It might be better to take the payment information fields and add them as scalar fields.

            # Verify that there are currently no transactions in the database with the target StartDateUTC date string:
            should_be_none = cached_ps.find_one(StartDateUTC_date = slot_start_date_string)

            if should_be_none is not None:
                print("should_be_none = ")
                pprint.pprint(should_be_none)
                raise ValueError("A transaction was found in the database even though it shouldn't have been there according to cached_dates.")

            # ps is a list of dicts.
            t_x = time.time()

            db.begin()
            try:
                for p in ps_all_fixed:
                    db['cached_purchases'].upsert(p, ['@PurchaseGuid'])
                db.commit()
                print("Stored {} transactions in cached_purchases.".format(len(ps_all_fixed)))
            except:
                db.rollback()
                print("Failed to store {} transactions in cached_purchases.".format(len(ps_all_fixed)))
            # The upserting code was rewritten from the version below to the version above 
            # to try to speed up the process, but it's not clear how much it really helped.
            # Both would need to be tested on a list of ~10,000 transactions to see the 
            # true improvement.

            #for p in ps_all_fixed:
            #    cached_ps.upsert(p, ['@PurchaseGuid'])
            t_y = time.time()
            print("     Time required to upsert {} transactions to the database: {} s".format(len(ps_all_fixed),t_y-t_x))

            cached_dates.upsert(dict(date = slot_start_date_string), ['date'])
            print("Stored the date {} in cached_dates.".format(slot_start_date_string))
    else: # Load locally cached data
        start_of_day = beginning_of_day(slot_start)
        start_of_next_day = beginning_of_day(slot_start) + timedelta(days=1)
        start_epoch = epoch_time(start_of_day)
        next_epoch = epoch_time(start_of_next_day)
        time0 = time.time()
        ps_all = list(db.query("SELECT * FROM cached_purchases WHERE unix_time >= {} and unix_time < {}".format(start_epoch,next_epoch)))
        # Manual tests suggest that the unix_time query is at least not returning any results outside the intended date range
        time1 = time.time()
        print("The unix_time query returned {} transactions in {} s.".format(len(ps_all), time1-time0))
        # The unix_time query seems to take about the same amount of time as the StartDateUTC_date query, which is weird
        # since the unix_time field is supposed to be indexed.
        #ps_all2 = list(db.query("SELECT * FROM cached_purchases WHERE StartDateUTC_date = '{}'".format(slot_start_date_string)))
        #time2=time.time()
        #print("The unix_time query returned {} transactions, while the StartDateUtc_date query (seeking {}) returned {} transactions in {} s.".format(len(ps), slot_start_date_string, len(ps2), time2-time1))
    
    return ps_all

def get_ps(db,slot_start,slot_end,cache,mute=False,tz=pytz.utc,time_field = '@StartDateUtc',dt_format='%Y-%m-%dT%H:%M:%S'):
    # (This is designed to be the "get_ps" part of the function
    # formerly known as get_ps_from_somewhere.)
    
    # That is,
    #       get_ps_from_somewhere(db,slot_start,slot_end,cache,mute)
    # should return the same results as
    #       get_ps(db,slot_start,slot_end,cache,mute)
    # which suggests a good test to try.

    ###
    # This function handles the situation where slot_start and slot_end are on different days
    # by calling get_ps_for_day in a loop.

    # The parameter "time_field" determines which of the timestamps is used for calculating
    # the datetime values used to filter purchases down to those between slot_start
    # and start_end.


    # Note that the time zone tz and the time_field must be consistent for this to work properly.
    # Here is a little sanity check:
    
    if (re.search('Utc',time_field) is not None) != (tz == pytz.utc): # This does an XOR 
                                                                      # between these values.
        raise RuntimeError("It looks like the time_field may not be consistent with the provided time zone")

    global last_utc_date_cache, utc_ps_cache, utc_dts_cache
    if last_utc_date_cache != slot_start.date():
        if not mute:
            print("last_utc_date_cache ({}) doesn't match slot_start.date() ({})".format(last_utc_date_cache, slot_start.date()))

        ps_all = []
        dt_start_i = slot_start
        while dt_start_i < slot_end:
            #ps_for_whole_day = get_ps_for_day(db,dt_start_i,cache,mute)
            ps_for_whole_day = get_utc_ps_for_day_from_json(slot_start,cache,mute)
            ps_all += ps_for_whole_day
            dt_start_i += timedelta(days = 1)
            if not mute:
                print("Now there are {} transactions in ps_all".format(len(ps_all)))

        utc_ps_cache = ps_all # Note that if slot_start and slot_end are not on the same day,
        # utc_ps_cache will hold transactions for more than just the date of slot_start, but 
        # since filtering is done further down in this function, this should not represent a 
        # problem. There should be no situations where more than two days of transactions will
        # wind up in this cache at any one time.
        utc_dts_cache = [tz.localize(datetime.strptime(p[time_field],dt_format)) for p in ps_all] # This may break for StartDateUtc!!!!!
    else:
        ps_all = utc_ps_cache
    #ps = [p for p in ps_all if slot_start <= tz.localize(datetime.strptime(p[time_field],'%Y-%m-%dT%H:%M:%S')) < slot_end] # This takes like 3 seconds to
    # execute each time for busy days since the time calculations
    # are on the scale of tens of microseconds.
    # So let's generate the datetimes once (above), and do
    # it this way:
    ps = [p for p,dt in zip(ps_all,utc_dts_cache) if slot_start <= dt < slot_end]
    # Now instead of 3 seconds it takes like 0.03 seconds.

    #pgh = pytz.timezone('US/Eastern')
    #for p in ps:
        #if p['@TerminalID'] == '404451-22NDST0002':
        #    pprint.pprint(p)

        # Search for particular transactions and print them.
        #if re.search('404',p['@TerminalID']) is not None:
        #    if p['@Units'] == '45':
        #        sdu = (pytz.utc).localize(datetime.strptime(p[time_field],dt_format))
        #        sdl = sdu.astimezone(pgh)
        #        if pgh.localize(datetime(2013,10,1,17,30)) <= sdl < pgh.localize(datetime(2013,10,1,17,40)):
        #            pprint.pprint(p)
    last_utc_date_cache = slot_start.date()
    return ps

def get_ps_from_somewhere(db,slot_start,slot_end,cache=True,mute=False):
    # This function gets parking purchases, either from a 
    # cache database (if the date appears to have been cached)
    # or else from the API (and then caches the whole thing 
    # if cache = True).

    # Note that no matter what time of day is associated with slot_start,
    # this function will get all of the transactions for that entire day.

    # Filtering the results down to the desired time range is now handled 
    # in this functions (though the function only marks a date as cached
    # when it has added all events from the day of slot_start (based on
    # StartDateUtc)).
    
    # This approach tries to address the problem of the often 
    # gigantic discrepancy between the DateCreatedUtc timestamp
    # and the StartDateUtc timestamp.

    # This new database-centric, StartDateUtc-based approach has
    # been verified to give the same results as the old approach
    # for 2012-07-23.

    cached_dates,cached_ps = get_tables_from_db(db)

    ref_field = '@StartDateUtc' # This should not be changed.
    date_format = '%Y-%m-%d'

    tz = pytz.utc
    ps_all = []
    dts_all = []
    #for each date in day before, day of, and day after (to get all transactions)
        # [Three full days is probably slightly overkill since the most extreme 
        # negative case of DateCreatedUtc - StartDateUtc observed so far has 
        # been -12 hours.]

    # To avoid issues with missing or extra hours during Daylight Savings Time,
    # all slot times should be in UTC.

    # The cached_dates format is also UTC dates. (This change was made to 
    # route around issues encountered when using localized versions of 
    # StartDateUtc and converting to dates.)

    slot_start_date_string = slot_start.astimezone(tz).strftime(date_format) # This is the date string 
    # for the start of the overall desired time range.

    if cached_dates.find_one(date=slot_start_date_string) is None:
        # A date is added to cached_dates when the date and the two surrounding it have
        # been queried and all events with a StartDateUTC value in the day corresponding
        # to slot_start (as well as slot_start_date_string) have been added to the database.

        # That way, if the date is in cached_dates, the query can be done on the 
        # database without hitting the API.

        if not mute:
            print("Sigh! {} not found in cached_dates, so I'm pulling the data from the API...".format(slot_start_date_string))
       # Pull the data from the API.
        for offset in range(-1,2):
            query_start = beginning_of_day(slot_start) + (offset)*timedelta(days = 1)
            query_end = query_start + timedelta(days = 1)

            #query_date_string = query_start.astimezone(tz).strftime(date_format) # This is the date
            # in the local time zone. # This seems to not be used at all.

            base_url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/BatchDataExport/4/BatchDataExport.svc/purchase/ticket/'
            url = build_url(base_url,query_start,query_end)

            if not mute:
                print("Here's the URL for offset = {}: {}".format(offset,url))
            r = requests.get(url, auth=(CALE_API_user, CALE_API_password))

            # Convert Cale's XML into a Python dictionary
            doc = xmltodict.parse(r.text,encoding = r.encoding)

            url2 = doc['BatchDataExportResponse']['Url']
            r2 = requests.get(url2, auth=(CALE_API_user, CALE_API_password))
            doc = xmltodict.parse(r2.text,encoding = r2.encoding)

            while not r2.ok or doc['BatchDataExportFileResponse']['ExportStatus'] != 'Processed':
                time.sleep(10)
                r2 = requests.get(url2, auth=(CALE_API_user, CALE_API_password))
                doc = xmltodict.parse(r2.text,encoding = r2.encoding)

            url3 = doc['BatchDataExportFileResponse']['Url']

            # When the ZIP file is ready:
            r3 = requests.get(url3, stream=True, auth=(CALE_API_user, CALE_API_password))
            while not r3.ok:
                time.sleep(5)
                r3 = requests.get(url3, stream=True, auth=(CALE_API_user, CALE_API_password))

            z = zipfile.ZipFile(BytesIO(r3.content))

            # Extract contents of a one-file zip file to memory:
            xml = z.read(z.namelist()[0])
            doc = xmltodict.parse(xml,encoding = 'utf-8')

            ps = convert_doc_to_purchases(doc['BatchExportRoot'],query_start,date_format)

            # Remove fields #
            purchases = cull_fields(ps)

            # Filter down to the events in the slot, adding on two date/time fields #
            datetimes = [(pytz.utc).localize(datetime.strptime(p[ref_field],'%Y-%m-%dT%H:%M:%S')) for p in purchases]
            #ps = [p for p,dt in zip(purchases,dts) if beginning_of_day(slot_start) <= dt < beginning_of_day(slot_start) + timedelta(days=1)]
            ps = []
            dts = []
            
            start_of_day = beginning_of_day(slot_start)
            start_of_next_day = beginning_of_day(slot_start) + timedelta(days=1)
            for purchase_i,datetime_i in zip(purchases,datetimes):
                if start_of_day <= datetime_i < start_of_next_day:
                    purchase_i['StartDateUTC_date'] = (datetime_i).astimezone(tz).strftime(date_format) # This SHOULD be equal to slot_start.date().........
                    # but verify this.
                    # [ ] Assuming that these two things are equal would speed up processing a little.
                    if purchase_i['StartDateUTC_date'] != slot_start_date_string:
                        print("slot_start = {} = {}".format(slot_start, slot_start.astimezone(pytz.utc)))
                        print("slot_start_date_string (This is the local date) = {}".format(slot_start_date_string))
                        print("beginning_of_day(slot_start) = {}".format(start_of_day))
                        print("datetime_i = {}".format(datetime_i))
                        print("beginning_of_day(slot_start) + timedelta(days=1) = {}".format(start_of_next_day))
                        pprint.pprint(purchase_i)
                        raise ValueError("purchase_i['StartDateUTC_date'] != slot_start_date_string, {} != {}".format(purchase_i['StartDateUTC_date'], slot_start_date_string))
                    ps.append(purchase_i)
                    dts.append(datetime_i)

            for p,dt in zip(ps,dts):
                p['unix_time'] = epoch_time(dt)
                # This is a hack to provide a float that can be stored in SQLite (which has serious problems with datetime 
                # comparisons) until I can get a Postgres database set up.
            
            # Add these purchases to the overall accumulating set that fit in the slot #
            ps_all += ps #ps_all is all purchases that have StartDateUtc values between the beginning of the day corresponding to slot_start
            # and the beginning of the next day (24 hours later, in UTC).
            dts_all += dts         
        
        if cache:
            #store in the db and update cached_dates 

            ps_all_fixed = remove_field(ps_all,'PurchasePayUnit') # PurchasePayUnit itself contains a data structure
            # so dataset can't handle sticking it into a databse.
            # It might be better to take the payment information fields and add them as scalar fields.

            # Verify that there are currently no transactions in the database with the target StartDateUTC date string:
            should_be_none = cached_ps.find_one(StartDateUTC_date = slot_start_date_string)

            if should_be_none is not None:
                print("should_be_none = ")
                pprint.pprint(should_be_none)
                raise ValueError("A transaction was found in the database even though it shouldn't have been there according to cached_dates.")

            # ps is a list of dicts.
            t_x = time.time()

            db.begin()
            try:
                for p in ps_all_fixed:
                    db['cached_purchases'].upsert(p, ['@PurchaseGuid'])
                db.commit()
            except:
                db.rollback()
            # The upserting code was rewritten from the version below to the version above 
            # to try to speed up the process, but it's not clear how much it really helped.
            # Both would need to be tested on a list of ~10,000 transactions to see the 
            # true improvement.

            #for p in ps_all_fixed:
            #    cached_ps.upsert(p, ['@PurchaseGuid'])
            t_y = time.time()
            print("     Time required to upsert {} transactions to the database: {} s".format(len(ps_all_fixed),t_y-t_x))

            cached_dates.upsert(dict(date = slot_start_date_string), ['date'])

        # Now that the data for one full day has been stored in the cache database,
        # obtain the originally desired transactions.


        requested_ps = [p for p,dt in zip(ps_all,dts_all) if slot_start <= dt < slot_end]

        for_comparison = list(db.query("SELECT * FROM cached_purchases WHERE unix_time >= {} and unix_time < {}".format(epoch_time(slot_start),epoch_time(slot_end))))
        print("len(requested_ps) = {}, while len(for_comparison) = {}".format(len(requested_ps),len(for_comparison)))

        # [ ] Do the comparison of requested_ps with for_comparison.

    else: # Load locally cached version
        t_b = time.time()
        requested_ps = list(db.query("SELECT * FROM cached_purchases WHERE unix_time >= {} and unix_time < {}".format(epoch_time(slot_start),epoch_time(slot_end))))
        t_c = time.time()
        print("Got {} transactions from the database in {} s.".format(len(requested_ps),t_c-t_b))
    return requested_ps

def get_events_from_db(db,slot_start,slot_end,cache,mute=False,tz=pytz.timezone('US/Eastern'),time_field = '@PurchaseDateLocal',dt_format='%Y-%m-%dT%H:%M:%S'):
    # This function gets all transactions between slot_start and slot_end, using time_field
    # as the reference field, by relying on get_ps_from_somewhere to do most of the heavy
    # lifting. The main thing this function adds is adding margins and then filtering down
    # to handle different reference time fields.

    # The parameter "time_field" determines which of the timestamps is used for calculating
    # the datetime values used to filter purchases down to those between slot_start
    # and start_end.

    # This function has been written but has so far not been used.
    
    # Note that the time zone tz and the time_field must be consistent for this to work properly.
    # Here is a little sanity check:
    if (re.search('Utc',time_field) is not None) != (tz == pytz.utc): 
        # This does an XOR between these values.
        print("time_field = {}, but tz = {}".format(time_field,tz))
        raise RuntimeError("It looks like time_field may not be consistent with the provided time zone")

    cached_dates,cached_ps = get_tables_from_db(db)

    # if any date in the desired slot does not fall sufficiently within the available pre-downloaded
    # dates (according to cached_dates), 
        # call get_ps_from_somewhere to get them from the API.
  
    margin = timedelta(hours = 24)
    day = (slot_start - margin).date() 
    while day <= (slot_end + margin).date():
        if not in_db(cached_dates,slot_start):
            dt_start = datetime.combine(day, datetime.min.time())
            dt_end = datetime.combine(day + timedelta(days=1), datetime.min.time())
            get_ps_from_somewhere(db,dt_start,dt_end,cache,mute)

    # call get_ps_from_somewhere with an appropriate margin added on and 
    # then filter down to the desired events.
    unfiltered_ps = get_ps_from_somewhere(db,slot_start-margin,slot_end+margin,cache,mute) # These are actually
        # kind of filtered. The hope is that the margins eliminate any filtering effects.
    dts = [tz.localize(datetime.strptime(p[time_field],dt_format)) for p in unfiltered_ps]
    ps = [p for p,dt in zip(ps_all,dts) if slot_start <= dt < slot_end]
    return ps

def get_recent_parking_events(slot_start,slot_end,mute=False,tz=pytz.utc,time_field = '@StartDateUtc',dt_format='%Y-%m-%dT%H:%M:%S'):
    # To ensure that all events are obtained, add a huge margin around each slot to collect the outlier 
    # events that have reference time fields with values very different from when the events were
    # filed in the CALE database.

    # This approach results in a query for ten minutes of data scanning almost two days worth of data,
    # but this seems necessary because of how the CALE API works.

    # Alternatives: Do one big query for all the data needed during one run of this script:
    #                       1) Cache everything from the main()-level slot_start - 1 day to 
    #                       halting_time + 1 day.
    #                       2) Use that live_cache repeatedly.

    # slot_start and slot_end must have time zones so that they
    # can be correctly converted into UTC times for interfacing
    # with the /Cah LAY/ API.

    # The time-zone argument tz is used to specify the time zone of time_field.
    # Thus, if @StartDateUtc is used as the reference time field for deciding whether a transaction
    # is in the slot or not, the time zone tz should be UTC.
    if (re.search('Utc',time_field) is not None) != (tz == pytz.utc): 
        # This does an XOR between these values.
        raise RuntimeError("It looks like time_field may not be consistent with the provided time zone")

    if time_field == '@DateCreatedUtc':
        margin = timedelta(minutes = 0)
    else:
        margin = timedelta(hours = 24)
    date_format = '%Y-%m-%d'
    base_url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/4/LiveDataExportService.svc/purchases/'
    url = build_url(base_url,slot_start - margin,slot_end + margin)
    if not mute:
        print("Here's the URL: {}".format(url))

    r = pull_from_url(url)
    doc = xmltodict.parse(r.text,encoding = 'utf-8')
    ps_all = convert_doc_to_purchases(doc,slot_start,date_format)
    dts = [tz.localize(datetime.strptime(p[time_field],dt_format)) for p in ps_all]
    ps = [p for p,dt in zip(ps_all,dts) if slot_start <= dt < slot_end]
    time.sleep(1)
    return ps

def naive_get_recent_parking_events(slot_start,slot_end,mute=False,tz=pytz.timezone('US/Eastern'),time_field = '@PurchaseDateLocal',dt_format='%Y-%m-%dT%H:%M:%S'):
    # [This version has now been labelled "naive" since it does not account 
    # for the fact that the default reference time field (@DateCreatedUtc)
    # does not really enable selection of parking sessions by their 
    # true temporal bounds.]

    # slot_start and slot_end must have time zones so that they
    # can be correctly converted into UTC times for interfacing
    # with the /Cah LAY/ API.
    date_format = '%Y-%m-%d'
    base_url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/4/LiveDataExportService.svc/purchases/'
    url = build_url(base_url,slot_start,slot_end)
    if not mute:
        print("Here's the URL: {}".format(url))

    r = pull_from_url(url)
    doc = xmltodict.parse(r.text,encoding = 'utf-8')
    ps = convert_doc_to_purchases(doc,slot_start,date_format)
    time.sleep(1)
    return ps

def get_parking_events(db,slot_start,slot_end,cache=False,mute=False,db_caching=True):
    pgh = pytz.timezone('US/Eastern')
    #if datetime.now(pgh) - slot_end <= timedelta(hours = 24):
        # This is too large of a margin, to be on the safe side.
        # I have not yet found the exact edge.
    if datetime.now(pgh) - slot_end <= timedelta(days = 5):
        #return get_recent_parking_events(slot_start,slot_end,mute,pytz.utc,time_field = '@DateCreatedUtc',dt_format='%Y-%m-%dT%H:%M:%S.%f')
        return get_recent_parking_events(slot_start,slot_end,mute,pytz.utc,time_field = '@StartDateUtc',dt_format='%Y-%m-%dT%H:%M:%S')
    else:
        if db_caching:
            return get_ps(db,slot_start,slot_end,cache,mute)
            #return get_ps_from_somewhere(db,slot_start,slot_end,cache,mute)
        #return get_events_from_db(slot_start,slot_end,cache,mute,pytz.utc,time_field = '@StartDateUtc') # With time_field = '@StartDateUtc',
        # this function should return the same thing as get_ps_from_somewhere.
        else:
            return get_batch_parking(slot_start,slot_end,cache,mute,pytz.utc,time_field = '@StartDateUtc')
        #return get_batch_parking(slot_start,slot_end,cache,mute,pytz.utc,time_field = '@PurchaseDateUtc')
        #return get_batch_parking(slot_start,slot_end,cache,mute,pgh,time_field = '@PurchaseDateLocal')
        #return get_batch_parking(slot_start,slot_end,cache,pytz.utc,time_field = '@DateCreatedUtc',dt_format='%Y-%m-%dT%H:%M:%S.%f')

def package_for_output(stats_rows,zonelist,inferred_occupancy, temp_zone_info,tz,slot_start,slot_end,aggregate_by,augment):
    list_of_dicts = []
    augmented = []
    zlist = sorted(list(set(sorted(stats_rows.keys())+zonelist)))

    for zone in zlist:
        if zone in stats_rows.keys():
            d = stats_rows[zone]
        else:
            d = initialize_zone_stats(slot_start,slot_end,aggregate_by,tz)
        d['Zone'] = zone
        if zone in stats_rows.keys():
            list_of_dicts.append(d)
        if inferred_occupancy is not None:
            d['Inferred occupancy'] = inferred_occupancy[slot_start][zone]
    #            if d['Inferred occupancy'] > 0:
    #                print "Inferred occupancy:", slot_start,zone,d['Inferred occupancy']
        if zone in temp_zone_info:
            extra = temp_zone_info[zone]
            d['Latitude'] = extra['Latitude']
            d['Longitude'] = extra['Longitude']
            d['Meter count'] = extra['MeterCount']
            d['Zone type'] = extra['Type']
            #augmented.append(d)
            if augment:
                if d['Inferred occupancy'] > 0 or zone in stats_rows.keys():
                    augmented.append(d)

#        elif zone in stats_rows.keys(): # Allentown is missing, but since all those terminals
        # are listed as inactive, this branch should never get called
        # unless someone (maybe the ParkMobile user entering a code)
        # makes an error.
#            print("Found a zone not listed in temp_zone_info: {}".format(zone))
    return list_of_dicts, augmented

def main(*args, **kwargs):
    # This function accepts slot_start and halting_time datetimes as
    # arguments to set the time range and push_to_CKAN and output_to_csv
    # to control those output channels.
    t_begin = time.time()

    output_to_csv = kwargs.get('output_to_csv',False)
    push_to_CKAN = kwargs.get('push_to_CKAN',True)
    augment = kwargs.get('augment',False)

    default_filename = 'parking-dataset-1.csv'
    filename = kwargs.get('filename',default_filename)
    overwrite = kwargs.get('overwrite',False)

    turbo_mode = kwargs.get('turbo_mode',False)
    # When turbo_mode is true, skip time-consuming stuff,
    # like correct calculation of durations.
    skip_processing = kwargs.get('skip_processing',False)
    db_caching = kwargs.get('db_caching',True)

    threshold_for_uploading = kwargs.get('threshold_for_uploading',1000) # The
    # minimum length of the list of dicts that triggers uploading to CKAN.


    db_filename = kwargs.get('db_filename','transactions_cache.db') # This can be
    # changed with a passed parameter to substituted a test database 
    # for running controlled tests with small numbers of events.
    db = create_or_connect_to_db(db_filename)

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


    timechunk = kwargs.get('timechunk',DEFAULT_TIMECHUNK)

  #  timechunk = timedelta(seconds=1)
    if skip_processing:
        timechunk = timedelta(hours=24)

    #slot_start = roundTime(datetime.now() - timedelta(hours=24), 60*60)
    # Start 24 hours ago (rounded to the nearest hour).
    # This is a naive (timezoneless) datetime, so let's try it this way:
    #slot_start = roundTime(datetime.now(pytz.utc) - timedelta(hours=24), 60*60)
    # It is recommended that all work be done in UTC time and that the
    # conversion to a local time zone only happen at the end, when
    # presenting something to humans.
    #slot_start = beginning_of_day(datetime.now(pgh) - timedelta(hours=24)) #+ timedelta(hours=2)
    #slot_start = beginning_of_day(datetime.now(pgh) - timedelta(days=7))
    slot_start = pgh.localize(datetime(2012,7,23,0,0)) # The actual earliest available data.
    slot_start = pgh.localize(datetime(2017,4,15,0,0))
    slot_start = kwargs.get('slot_start',slot_start)

########
    halting_time = slot_start + timedelta(hours=24)

    # halting_time = beginning_of_day(datetime.now(pgh))
    halting_time = pgh.localize(datetime(3030,4,13,0,0)) # Set halting time
    # to the far future so that the script runs all the way up to the most
    # recent data (based on the slot_start < now check in the loop below).
    #halting_time = pgh.localize(datetime(2017,3,2,0,0)) # Set halting time
    halting_time = kwargs.get('halting_time',halting_time)


    # Setting slot_start and halting_time to UTC has no effect on 
    # getting_ps_from_somewhere, but totally screws up get_batch_parking
    # (resulting in zero transactions after 20:00 (midnight UTC).
    if db_caching:
        slot_start = slot_start.astimezone(pytz.utc)
        halting_time = halting_time.astimezone(pytz.utc)
    # This is not related to the resetting of ps_dict, since extending 
    # ps_dict by adding on previous_ps_dict did not change the fact that 
    # casting slot_start and halting_time to UTC caused all transactions
    # after 20:00 ET to not appear in the output.

    # Therefore, (until the real reason is uncovered), slot_start and halting_time
    # will only be converted to UTC when using database caching.

    inferred_occupancy = defaultdict(lambda: defaultdict(int)) # Number of cars for each time slot and zone.
    ad_hoc_zones, parent_zones = pull_terminals(use_cache=use_cache,return_extra_zones=True)
    print("ad hoc zones = {}".format(ad_hoc_zones))

    print("parent_zones = ...")
    pprint.pprint(parent_zones)


    virtual_zone_checked = []

    ordered_fields = [{"id": "Zone", "type": "text"}]
    ordered_fields.append({"id": "Parent Zone", "type": "text"})
    ordered_fields.append({"id": "Start", "type": "timestamp"})
    ordered_fields.append({"id": "End", "type": "timestamp"})
    ordered_fields.append({"id": "UTC Start", "type": "timestamp"})
    ordered_fields.append({"id": "Transactions", "type": "int"})
    ordered_fields.append({"id": "Car-minutes", "type": "int"})
    ordered_fields.append({"id": "Payments", "type": "float"})
    ordered_fields.append({"id": "Durations", "type": "json"})

    ad_hoc_ordered_fields = list(ordered_fields)
    ordered_fields.remove({"id": "Parent Zone", "type": "text"})

    if push_to_CKAN: # Explicitly list the resources in the console.
        dp, settings, site, API_key = open_a_channel(server)
        package_name = get_package_name_from_resource_id(site,resource_id,API_key)
        print("package = {}, site = {}, server = {}".format(package_name, site, server))
        r_name = get_resource_parameter(site,resource_id,'name',API_key)
        a_h_name = get_resource_parameter(site,ad_hoc_resource_id,'name',API_key)
        print("resource_id = {} ({}),  ad_hoc_resource_id = {} ({})".format(resource_id, r_name, ad_hoc_resource_id, a_h_name))

    cumulated_dicts = []
    cumulated_ad_hoc_dicts = []
    ps_dict = defaultdict(list)
    previous_ps_dict = defaultdict(list)

    # The current approach to calculating durations tracks the recent transaction history
    # and subtracts the "Units" value (the number of cumulative minutes purchased)
    # from the previous transaction to get the incremental duration of the most
    # recent purchase.

    # (Collapsing all those transactions to a single session would be the opposite
    # process: Whenver an older predecessor transaction is found, it is folded into
    # one big session object to represent the whole multi-part parking purchase.)

    # Since this approach relies on having a purchase history, for complete consistency
    # (no matter for which datetime the script starts processing the same durations
    # and session-clustering should be achieved), it would be best to run the script
    # over some number of hours (warm_up_period) during which the purchase history
    # is built up but the data is not output to anything (save the console).

    # HOWEVER, the above reasoning is inconsistent with how the purchase history is
    # currently being kept (clearing it at midnight every day). The edge case I
    # was concerned about was the parking purchase that happens at 12:05am that
    # extends a previous purchase.

    # Using a separate seeding-mode stage considerably speeds up the warming-up
    # period (from maybe 10 minutes to closer to one or two).
    seeding_mode = True
    if seeding_mode:
        warm_up_period = timedelta(hours=12)
        print("slot_start - warm_up_period = {}".format(slot_start - warm_up_period))
        purchases = get_parking_events(db,slot_start - warm_up_period,slot_start,True,False,db_caching)
        for p in sorted(purchases, key = lambda x: x['@DateCreatedUtc']):
            reframe(p,terminals,t_guids,ps_dict,{},turbo_mode)
            ps_dict = add_to_dict(p,copy(ps_dict),terminals,t_guids) # ps_dict is intended to
            # be a way to look up recent transactions that might be part of the same 
            # session as a particular transaction. Here it is being seeded.

    slot_end = slot_start + timechunk
    current_day = slot_start.date()

    dkeys = ['Zone', 'Start', 'End', 'UTC Start', 'Transactions', 'Car-minutes', 'Payments', 'Durations']
    # These are dictionary keys (for writing a bunch of dictionaries to a CSV file), NOT database keys.
    augmented_dkeys = ['Zone', 'Start', 'End', 'UTC Start', 'Transactions', 'Car-minutes', 'Payments', 'Durations', 'Latitude', 'Longitude', 'Meter count', 'Zone type', 'Inferred occupancy']
    ad_hoc_dkeys = ['Zone', 'Parent Zone', 'Start', 'End', 'UTC Start', 'Transactions', 'Car-minutes', 'Payments', 'Durations']
    # I just added 'UTC Start' to ad_hoc_dkeys on April 25, 2017.

    # [ ] Check that primary keys are in fields for writing to CKAN. Maybe check that dkeys are valid fields.

    while slot_start <= datetime.now(pytz.utc) and slot_start < halting_time:

        t0 = time.time()

        # Get all parking events that start between slot_start and slot_end
        if slot_end > datetime.now(pytz.utc): # Clarify the true time bounds of slots that
            slot_end = datetime.now(pytz.utc) # run up against the limit of the current time.

        purchases = get_parking_events(db,slot_start,slot_end,True,False,db_caching)
        t1 = time.time()

        print("{} | {} purchases".format(datetime.strftime(slot_start.astimezone(pgh),"%Y-%m-%d %H:%M:%S ET"), len(purchases)))

        if skip_processing:
            print("Sleeping...")
            time.sleep(3)
        else:
            reframed_ps = []

            for p in sorted(purchases, key = lambda x: x['@DateCreatedUtc']):
                reframed_ps.append(reframe(p,terminals,t_guids,ps_dict,previous_ps_dict,turbo_mode))

                if slot_start.date() == current_day: # Keep a running history of all
                    ps_dict = add_to_dict(p,copy(ps_dict),terminals,t_guids) # purchases for a given day.
                else:
                    print("Moving ps_dict to previous_ps_dict at {}. (Should ps_dict be generated before the reframing loop for better efficiency? Would that screw up durations computations?)".format(slot_start))
                    current_day = slot_start.date()
                    previous_ps_dict = ps_dict
                    ps_dict = defaultdict(list) # And restart that history when a new day is encountered.
            # Temporary for loop to check for unconsidered virtual zone codes.
            #for rp in reframed_ps:
            #    if rp['TerminalID'][:3] == "PBP":
            #        code = rp['TerminalID'][3:]
            #        if code not in virtual_zone_checked:
            #            print("\nVerifying group code {} for a purchase at terminal {}".format(code,rp['TerminalGUID']))
            #            code_group = group_by_code(code)
            #            print("Found group {}".format(code_group))
            #            virtual_zone_checked.append(code)


            t2 = time.time()

            # Condense to key statistics (including duration counts).
            stats_rows = distill_stats(reframed_ps,terminals,t_guids,t_ids,slot_start,slot_end, zone_kind, 'zone', [], tz=pgh)
            # stats_rows is actually a dictionary, keyed by zone.
            
            ad_hoc_stats_rows = distill_stats(reframed_ps,terminals,t_guids,t_ids,slot_start, slot_end, zone_kind, 'ad hoc zone', parent_zones, tz=pgh)

            t3 = time.time()
            if not turbo_mode and augment:
                inferred_occupancy = update_occupancies(inferred_occupancy,stats_rows,slot_start,timechunk)
            # We may eventually need to compute ad_hoc_inferred_occupancy.
            t4 = time.time()

            if len(stats_rows) == 0:
                print
            else:
                print("({})".format(find_biggest_value(stats_rows,'Transactions')))
            #pprint.pprint(stats_rows)
            # Return list of OrderedDicts where each OrderedDict
            # represents a row for a parking zone (or lot) and time slot.

            list_of_dicts, augmented = package_for_output(stats_rows,zonelist,inferred_occupancy,temp_zone_info,pgh,slot_start,slot_end,'zone',augment)
            
            if output_to_csv and len(list_of_dicts) > 0: # Write to files as
            # often as necessary, since the associated delay is not as great as
            # for pushing data to CKAN.
                write_or_append_to_csv(filename,list_of_dicts,dkeys,overwrite)
                if not turbo_mode and augment:
                    write_or_append_to_csv('augmented-purchases-1.csv',augmented,augmented_dkeys,overwrite)

            cumulated_dicts += list_of_dicts
            if push_to_CKAN and len(cumulated_dicts) >= threshold_for_uploading:
                print("len(cumulated_dicts) = {}".format(len(cumulated_dicts)))
                # server and resource_id parameters are imported from remote_parameters.py
                filtered_list_of_dicts = only_these_fields(cumulated_dicts,dkeys)
                filtered_list_of_dicts = cast_fields(filtered_list_of_dicts,ordered_fields) # This is all a hack until a proper marshmallow-based pipeline can be called.

                #success = pipe_data_to_ckan(server, resource_id, cumulated_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
                success = push_data_to_ckan(server, resource_id, filtered_list_of_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
                print("success = {}".format(success))
                if success:
                    cumulated_dicts = []

            ad_hoc_list_of_dicts, _ = package_for_output(ad_hoc_stats_rows,ad_hoc_zones,None,{},pgh,slot_start,slot_end,'ad hoc zone',augment)
            # Between the passed use_ad_hoc_zones boolean and other parameters, more
            # information is being passed than necessary to distinguish between
            # ad hoc zones and regular zones.

            if output_to_csv and len(ad_hoc_list_of_dicts) > 0:
                write_or_append_to_csv('ad-hoc-parking-dataset-1.csv',ad_hoc_list_of_dicts,ad_hoc_dkeys,overwrite)
                #print("Wrote some ad hoc data to a CSV file")

            cumulated_ad_hoc_dicts += ad_hoc_list_of_dicts
            if push_to_CKAN and len(cumulated_ad_hoc_dicts) >= threshold_for_uploading:
                filtered_list_of_dicts = only_these_fields(cumulated_ad_hoc_dicts,ad_hoc_dkeys)
                filtered_list_of_dicts = cast_fields(filtered_list_of_dicts,ad_hoc_ordered_fields)
                success_a = push_data_to_ckan(server, ad_hoc_resource_id, filtered_list_of_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
                if success_a:
                    cumulated_ad_hoc_dicts = []

            del inferred_occupancy[slot_start]

        slot_start += timechunk
        slot_end = slot_start + timechunk
        t8 = time.time()
        if not skip_processing:
            if len(reframed_ps) > 0:
                print("t8-t0 = {:1.2e} s. t1-t0 = {:1.2e} s. t2-t1 = {:1.2e} s. t3-t2 = {:1.2e} s.  (t3-t2)/len(rps) = {:1.2e} s".format(t8-t0, t1-t0, t2-t1, t3-t2, (t3-t2)/len(reframed_ps)))
            else:
                print("t8-t0 = {:1.2e} s. t1-t0 = {:1.2e} s. t2-t1 = {:1.2e} s. t3-t2 = {:1.2e} s.".format(t8-t0, t1-t0, t2-t1, t3-t2))
    print("After the main processing loop, len(ps_dict) = {}, len(cumulated_dicts) = {}, and len(cumulated_ad_hoc_dicts) = {}".format(len(ps_dict), len(cumulated_dicts), len(cumulated_ad_hoc_dicts)))
   
    cached_dates,_ = get_tables_from_db(db)
    print("Currently cached dates (These are UTC dates): {}".format(list(cached_dates.all())))

    t_end = time.time()
    print("Run time = {}".format(t_end-t_begin))

    if push_to_CKAN: # Upload the last batch.
        # server and resource_id parameters are imported from remote_parameters.py
        filtered_list_of_dicts = only_these_fields(cumulated_dicts,dkeys)
        filtered_list_of_dicts = cast_fields(filtered_list_of_dicts,ordered_fields)
        success = push_data_to_ckan(server, resource_id, filtered_list_of_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
        #success = pipe_data_to_ckan(server, resource_id, filtered_list_of_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
        if success:
            cumulated_dicts = []
            print("Pushed the last batch of transactions to {}".format(resource_id))
        filtered_list_of_dicts = only_these_fields(cumulated_ad_hoc_dicts,ad_hoc_dkeys)
        filtered_list_of_dicts = cast_fields(filtered_list_of_dicts,ad_hoc_ordered_fields)
        success_a = push_data_to_ckan(server, ad_hoc_resource_id, filtered_list_of_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
        if success_a:
            cumulated_ad_hoc_dicts = []
            print("Pushed the last batch of ad hoc transactions to {}".format(ad_hoc_resource_id))

        return success and success_a # This will be true if the last two pushes of data to CKAN are true (and even if all previous pushes
        # failed, the data should be sitting around in cumulated lists, and these last two success Booleans will tell you whether
        # the whole process succeeded).

    return None # The success Boolean should be defined when push_to_CKAN is false.

if __name__ == '__main__':
    main()
