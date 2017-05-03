import xmltodict
import os
import re

import json
from collections import OrderedDict, Counter, defaultdict
from util import to_dict, value_or_blank, unique_values, zone_name, is_a_lot, lot_code, is_virtual, get_terminals, is_timezoneless, write_or_append_to_csv, pull_from_url, remove_field, round_to_cent, corrected_zone_name, lot_list, pure_zones_list, numbered_reporting_zones_list, special_groups, add_element_to_set_string, add_if_new, group_by_code, numbered_zone, censor, only_these_fields, cast_fields
from fetch_terminals import pull_terminals_return_special_zones_and_parent_zones
import requests
import zipfile
try:
    import StringIO # For Python 2
except ImportError:
    from io import StringIO # For Python 3
from copy import copy

import time
import pprint
from datetime import datetime, timedelta
import pytz

from credentials_file import CALE_API_user, CALE_API_password
from local_parameters import path
from remote_parameters import server, resource_id, ad_hoc_resource_id

# These functions should eventually be pulled from utility_belt.
from prime_ckan.push_to_CKAN_resource import push_data_to_ckan, open_a_channel
#from prime_ckan.pipe_to_CKAN_resource import pipe_data_to_ckan
from prime_ckan.util import get_resource_parameter, get_package_name_from_resource_id

DEFAULT_TIMECHUNK = timedelta(minutes=10)

last_date_cache = None
all_day_ps_cache = []
dts = []

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

def terminal_of(p,t_guids,terminals):
    t = terminals[t_guids.index(p['@TerminalGuid'])]
    return t

def p_hash(p,t):
    # Use the combination of the original purchase date for the session
    # and the parking zone that it happened in as a unique identifier
    # to link transactions that are extensions of an original parking
    # purchase back to that first purchase.
    return "{}|{}".format(p['@PurchaseDateLocal'],numbered_zone(t['@Id']))

def is_original(p,t,p_history):
    # Check apparent rate against official rate for the terminal.

    # Unfortunately, this could only be done for live data.
    # Older purchases may have different rates that can only
    # be inferred from looking at other purchases from that day.
    #if p['@TerminalGuid'] in t_guids:
        # ValueError: u'401511-WOODST0402' is not in list
    #    t = terminals[t_guids.index(p['@TerminalGuid'])]
    #else:
    #    t = None
    p_key = p_hash(p,t)
    return not (p_key in p_history)

def find_predecessors(p,t,t_guids,terminals,p_history):
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

def reframe(p,terminals,t_guids,p_history,turbo_mode):
    # Take a dictionary and generate a new dictionary from it that samples
    # the appropriate keys and renames and transforms as desired.
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
        row['List_of_Special_Groups'] = special_groups(t)

    row['Amount'] = float(p['@Amount'])
    t = terminals[t_guids.index(p['@TerminalGuid'])]

    row['Duration'] = int(p['@Units']) # in minutes
# THIS ASSUMES THAT DURATIONS WILL ALWAYS BE IN MINUTES.
# IT WOULD BE NICE FOR THEM TO BE SO TO SIMPLIFY THE COUNTS
# DATA STRUCTURE

    if not turbo_mode and not is_original(p,t,p_history): # This is an initial test, but
    # some of the hits that it can return may still be rejected
    # by the find_predecessors function.

    # [ ] One option would be to change the p_history dict to
    # correctly separate purchases into sessions.
    # [ ] Another option would be just to eliminate the is_original
    # function and replace it entirely with find_predecessors.
        predecessors = find_predecessors(p,t,t_guids,terminals,p_history)
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
    row['StartDateLocal'] = datetime.strptime(p['@StartDateLocal'],'%Y-%m-%dT%H:%M:%S')
    row['PurchaseDateLocal'] = datetime.strptime(p['@PurchaseDateLocal'],'%Y-%m-%dT%H:%M:%S')

#    row['PayIntervalStartUTC'] = datetime.strptime(p['@PayIntervalStartUtc'],'%Y-%m-%dT%H:%M:%S')

#    row['EndDateUTC'] = datetime.strptime(p['@EndDateUtc'],'%Y-%m-%dT%H:%M:%S')
    if '@EndDateUtc' in p:
        end_time = datetime.strptime(p['@EndDateUtc'],'%Y-%m-%dT%H:%M:%S')
        # Some of the JSON files don't have this field because
        # I eliminated it from the more recent saved-file format.
        # Thus, maybe this whole 'Done' field is no longer needed
        # since it doesn't seem to be used anywhere.

    # Other EndDate parameters will likely be needed here for detecting
    # payment extensions.


        utc_end_time = (pytz.utc).localize(end_time)
        row['Done'] = utc_end_time  < datetime.now(pytz.utc)

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
    if aggregate_by == 'special zone':
        stats['Parent Zone'] = None
    return stats

def distill_stats(rps,terminals,t_guids,t_ids, start_time,end_time, zone_kind='old', aggregate_by='zone', parent_zones=[], tz=pytz.timezone('US/Eastern')):
    # Originally this function just aggregated information
    # between start_time and end_time to the zone level.

    # Then it was modified to support special zones,
    # allowing the function to be called separately just to
    # get special-zone-level aggregation.

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
        elif aggregate_by == 'special zone':
            if 'List_of_Special_Groups' in rp and rp['List_of_Special_Groups'] != []:
                zones = rp['List_of_Special_Groups']
# The problem with this is that a given purchase is associated with a terminal which may have MULTIPLE special zones. Therefore, each special zone must have its own parent zone(s).
        elif aggregate_by == 'meter':
            zones = [t_guid] # [ ] Should this be GUID or just ID?


            # The above could really stand to be refactored
        if zones != []:
            for zone in censor(zones):
                if zone not in stats_by:
                    stats_by[zone] = initialize_zone_stats(start_time,end_time,aggregate_by,tz=pytz.timezone('US/Eastern'))
                if aggregate_by == 'special zone':
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

def get_batch_parking_for_day(slot_start,cache=True):
    # Caches parking once it's been downloaded and checks
    # cache before redownloading.

    date_format = '%Y-%m-%d'

    dashless = slot_start.strftime('%y%m%d')
    xml_filename = path + "xml/"+dashless+".xml"
    filename = path + "json/"+dashless+".json"

    if not os.path.isfile(filename) or os.stat(filename).st_size == 0:
        print("Sigh! {}.json not found, so I'm pulling the data from the API...".format(dashless))

        slot_start = roundTime(slot_start, 24*60*60)
        slot_end = slot_start + timedelta(days = 1)

        base_url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/BatchDataExport/4/BatchDataExport.svc/purchase/ticket/'
        url = build_url(base_url,slot_start,slot_end)

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

        z = zipfile.ZipFile(StringIO.StringIO(r3.content))

        # Extract contents of a one-file zip file to memory:
        xml = z.read(z.namelist()[0])
        doc = xmltodict.parse(xml,encoding = 'utf-8')

        ps = convert_doc_to_purchases(doc['BatchExportRoot'],slot_start,date_format)

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
        purchases = remove_field(purchases,'@PurchaseDateUtc')
        purchases = remove_field(purchases,'@PayIntervalStartLocal')
        purchases = remove_field(purchases,'@PayIntervalStartUtc')
        purchases = remove_field(purchases,'@PayIntervalEndLocal')
        purchases = remove_field(purchases,'@PayIntervalEndUtc')
        #purchases = remove_field(purchases,'@EndDateLocal')
        purchases = remove_field(purchases,'@EndDateUtc')
        purchases = remove_field(purchases,'@PaymentServiceType')
        purchases = remove_field(purchases,'@TicketNumber')
        purchases = remove_field(purchases,'@TariffPackageID')
        purchases = remove_field(purchases,'@ExternalID')
        purchases = remove_field(purchases,'@PurchaseStateName')
        purchases = remove_field(purchases,'@PurchaseTriggerTypeName')
        purchases = remove_field(purchases,'@PurchaseTypeName')
        purchases = remove_field(purchases,'@MaskedPAN','PurchasePayUnit')
        purchases = remove_field(purchases,'@BankAuthorizationReference','PurchasePayUnit')
        purchases = remove_field(purchases,'@CardFeeAmount','PurchasePayUnit')
        purchases = remove_field(purchases,'@PayUnitID','PurchasePayUnit')
        purchases = remove_field(purchases,'@TransactionReference','PurchasePayUnit')
        purchases = remove_field(purchases,'@CardIssuer','PurchasePayUnit')

        if cache:
            with open(filename, "wb") as f:
                json.dump(purchases,f,indent=2)
    else: # Load locally cached version
        with open(filename,'rb') as f:
            ps = json.load(f)
    return ps

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

def get_recent_parking_events(slot_start,slot_end):
    # slot_start and slot_end must have time zones so that they
    # can be correctly converted into UTC times for interfacing
    # with the /Cah LAY/ API.
    date_format = '%Y-%m-%d'
    base_url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/4/LiveDataExportService.svc/purchases/'
    url = build_url(base_url,slot_start,slot_end)

    r = pull_from_url(url)
    doc = xmltodict.parse(r.text,encoding = 'utf-8')
    ps = convert_doc_to_purchases(doc,slot_start,date_format)
    time.sleep(5)
    return ps

def get_parking_events(slot_start,slot_end,cache=False):
    pgh = pytz.timezone('US/Eastern')
    #if datetime.now(pgh) - slot_end <= timedelta(hours = 24):
        # This is too large of a margin, to be on the safe side.
        # I have not yet found the exact edge.
    if datetime.now(pgh) - slot_end <= timedelta(days = 5):
        return get_recent_parking_events(slot_start,slot_end)
    else:
        return get_batch_parking(slot_start,slot_end,cache,pgh)

def package_for_output(stats_rows,zonelist,inferred_occupancy, temp_zone_info,tz,slot_start,slot_end,aggregate_by):
    list_of_dicts = []
    augmented = []
    zlist = list(set(sorted(stats_rows.keys())+zonelist))
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
            if d['Inferred occupancy'] > 0 or zone in stats_rows.keys():
                augmented.append(d)

        elif zone in stats_rows.keys(): # Allentown is missing, but since all those terminals
        # are listed as inactive, this branch should never get called
        # unless someone (maybe the ParkMobile user entering a code)
        # makes an error.
            print("Found a zone not listed in temp_zone_info: {}".format(zone))
    return list_of_dicts, augmented

def main(*args, **kwargs):
    # This function accepts slot_start and halting_time datetimes as
    # arguments to set the time range and push_to_CKAN and output_to_csv
    # to control those output channels.

    output_to_csv = kwargs.get('output_to_csv',False)
    push_to_CKAN = kwargs.get('push_to_CKAN',True)

    turbo_mode = False # When turbo_mode is true, skip time-consuming stuff,
    # like correct calculation of durations.
    skip_processing = False

    threshold_for_uploading = kwargs.get('threshold_for_uploading',1000) # The
    # minimum length of the list of dicts that triggers uploading to CKAN.

    zone_kind = 'new' # 'old' maps to enforcement zones
    # (specifically corrected_zone_name). 'new' maps to numbered reporting
    # zones.
    if zone_kind == 'old':
        zonelist = lot_list + pure_zones_list
    else:
        zonelist = numbered_reporting_zones_list

    pgh = pytz.timezone('US/Eastern')
    use_cache = False
    #use_cache = True
    terminals = get_terminals(use_cache)
    t_ids = [t['@Id'] for t in terminals]
    t_guids = [t['@Guid'] for t in terminals]


    timechunk = DEFAULT_TIMECHUNK
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
    #slot_start = roundTime(datetime.now(pgh) - timedelta(hours=24), 24*60*60) #+ timedelta(hours=2)
    #slot_start = roundTime(datetime.now(pgh) - timedelta(days=7), 24*60*60)
    slot_start = pgh.localize(datetime(2012,8,1,0,0)) # Possibly the earliest available data.
    slot_start = pgh.localize(datetime(2012,9,1,0,0)) # Avoid 2012-08-01 transaction that breaks duration calculations for now.
    slot_start = pgh.localize(datetime(2017,4,15,0,0))
    slot_start = kwargs.get('slot_start',slot_start)

########
    halting_time = slot_start + timedelta(hours=24)

    # halting_time = roundTime(datetime.now(pgh), 24*60*60)
    halting_time = pgh.localize(datetime(3030,4,13,0,0)) # Set halting time
    # to the far future so that the script runs all the way up to the most
    # recent data (based on the slot_start < now check in the loop below).
    #halting_time = pgh.localize(datetime(2017,3,2,0,0)) # Set halting time
    halting_time = kwargs.get('halting_time',halting_time)

    inferred_occupancy = defaultdict(lambda: defaultdict(int)) # Number of cars for each time slot and zone.

    special_zones, parent_zones = pull_terminals_return_special_zones_and_parent_zones(use_cache)
    print("special zones = {}".format(special_zones))

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
        package_name, _ = get_package_name_from_resource_id(site,resource_id,API_key)
        print("package = {}, site = {}, server = {}".format(package_name, site, server))
        r_name, _ = get_resource_parameter(site,resource_id,'name',API_key)
        a_h_name, _ = get_resource_parameter(site,ad_hoc_resource_id,'name',API_key)
        print("resource_id = {} ({}),  ad_hoc_resource_id = {} ({})".format(resource_id, r_name, ad_hoc_resource_id, a_h_name))

    cumulated_dicts = []
    cumulated_ad_hoc_dicts = []
    ps_dict = defaultdict(list)

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
        purchases = get_parking_events(slot_start-warm_up_period,slot_start,True)
        for p in sorted(purchases, key = lambda x: x['@DateCreatedUtc']):
            reframe(p,terminals,t_guids,ps_dict,turbo_mode)
            ps_dict = add_to_dict(p,copy(ps_dict),terminals,t_guids) # purchases for a given day.

    slot_end = slot_start + timechunk
    current_day = slot_start.date()
    
    while slot_start <= datetime.now(pytz.utc) and slot_start < halting_time:
        # Get all parking events that start between slot_start and slot_end
        purchases = get_parking_events(slot_start,slot_end,True)

        print("{} | {} purchases".format(datetime.strftime(slot_start.astimezone(pgh),"%Y-%m-%d %H:%M:%S ET"), len(purchases)))

        if skip_processing:
            print("Sleeping...")
            time.sleep(3)
        else:
            reframed_ps = []

            for p in sorted(purchases, key = lambda x: x['@DateCreatedUtc']):
                reframed_ps.append(reframe(p,terminals,t_guids,ps_dict,turbo_mode))

                if slot_start.date() == current_day: # Keep a running history of all
                    ps_dict = add_to_dict(p,copy(ps_dict),terminals,t_guids) # purchases for a given day.
                else:
                    current_day = slot_start.date()
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


            # Condense to key statistics (including duration counts).
            stats_rows = distill_stats(reframed_ps,terminals,t_guids,t_ids,slot_start,slot_end, zone_kind, 'zone', [], tz=pgh)
            # stats_rows is actually a dictionary, keyed by zone.

            special_stats_rows = distill_stats(reframed_ps,terminals,t_guids,t_ids,slot_start, slot_end, zone_kind, 'special zone', parent_zones, tz=pgh)

            if not turbo_mode:
                inferred_occupancy = update_occupancies(inferred_occupancy,stats_rows,slot_start,timechunk)
            # We may eventually need to compute special_inferred_occupancy.

            if len(stats_rows) == 0:
                print
            else:
                print("({})".format(find_biggest_value(stats_rows,'Transactions')))
            #pprint.pprint(stats_rows)
            # Return list of OrderedDicts where each OrderedDict
            # represents a row for a parking zone (or lot) and time slot.

    #        keys = (list_of_dicts.values()[0]).keys()
            keys = ['Zone', 'Start', 'End', 'UTC Start', 'Transactions', 'Car-minutes', 'Payments', 'Durations']

            list_of_dicts, augmented = package_for_output(stats_rows,zonelist,inferred_occupancy,temp_zone_info,pgh,slot_start,slot_end,'zone')

            augmented_keys = ['Zone', 'Start', 'End', 'UTC Start', 'Transactions', 'Car-minutes', 'Payments', 'Durations', 'Latitude', 'Longitude', 'Meter count', 'Zone type', 'Inferred occupancy']
            if output_to_csv:
                write_or_append_to_csv('parking-dataset-1.csv',list_of_dicts,keys)
                if not turbo_mode:
                    write_or_append_to_csv('augmented-purchases-1.csv',augmented,augmented_keys)

            cumulated_dicts += list_of_dicts
            if push_to_CKAN and len(cumulated_dicts) >= threshold_for_uploading:
                print("len(cumulated_dicts) = {}".format(len(cumulated_dicts)))
                # server and resource_id parameters are imported from remote_parameters.py
                filtered_list_of_dicts = only_these_fields(cumulated_dicts,keys)
                filtered_list_of_dicts = cast_fields(filtered_list_of_dicts,ordered_fields) # This is all a hack until a proper marshmallow-based pipeline can be called.

                #success = pipe_data_to_ckan(server, resource_id, cumulated_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
                success = push_data_to_ckan(server, resource_id, filtered_list_of_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
                print("success = {}".format(success))
                if success:
                    cumulated_dicts = []

            special_keys = ['Zone', 'Parent Zone', 'Start', 'End', 'UTC Start', 'Transactions', 'Car-minutes', 'Payments', 'Durations']
            # [ ] I just added 'UTC Start' to special_keys on April 25, 2017.

            special_list_of_dicts, _ = package_for_output(special_stats_rows,special_zones,None,{},pgh,slot_start,slot_end,'special zone')
            # Between the passed use_special_zones boolean and other parameters, more
            # information is being passed than necessary to distinguish between
            # special zones and regular zones.
            if output_to_csv:
                write_or_append_to_csv('special-parking-dataset-1.csv',special_list_of_dicts,special_keys)
            cumulated_ad_hoc_dicts += special_list_of_dicts
            if push_to_CKAN and len(cumulated_ad_hoc_dicts) >= threshold_for_uploading:
                filtered_list_of_dicts = only_these_fields(cumulated_ad_hoc_dicts,special_keys)
                filtered_list_of_dicts = cast_fields(filtered_list_of_dicts,ad_hoc_ordered_fields)
                success_a = push_data_to_ckan(server, ad_hoc_resource_id, filtered_list_of_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
                if success_a:
                    cumulated_ad_hoc_dicts = []

            del inferred_occupancy[slot_start]

        slot_start += timechunk
        slot_end = slot_start + timechunk

    print("len(ps_dict) = {}".format(len(ps_dict)))

    if push_to_CKAN: # Upload the last batch.
        # server and resource_id parameters are imported from remote_parameters.py
        filtered_list_of_dicts = only_these_fields(cumulated_dicts,keys)
        filtered_list_of_dicts = cast_fields(filtered_list_of_dicts,ordered_fields)
        success = push_data_to_ckan(server, resource_id, filtered_list_of_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
        if success:
            cumulated_dicts = []
        filtered_list_of_dicts = only_these_fields(cumulated_ad_hoc_dicts,special_keys)
        filtered_list_of_dicts = cast_fields(filtered_list_of_dicts,ad_hoc_ordered_fields)
        success_a = push_data_to_ckan(server, ad_hoc_resource_id, filtered_list_of_dicts, upload_in_chunks=True, chunk_size=5000, keys=None)
        if success_a:
            cumulated_ad_hoc_dicts = []

        return success and success_a # This will be true if the last two pushes of data to CKAN are true (and even if all previous pushes
        # failed, the data should be sitting around in cumulated lists, and these last two success Booleans will tell you whether
        # the whole process succeeded).

    return None # The success Boolean should be defined when push_to_CKAN is false.

if __name__ == '__main__':
    main()
