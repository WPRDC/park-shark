import os, re, csv, json, xmltodict
import random
from collections import OrderedDict, Counter, defaultdict
from util.util import to_dict, value_or_blank, unique_values, zone_name, is_a_lot, \
lot_code, is_virtual, get_terminals, is_timezoneless, write_or_append_to_csv, \
pull_from_url, remove_field, round_to_cent, corrected_zone_name, lot_list, \
other_zones_list, numbered_reporting_zones_list, sampling_groups, \
add_element_to_set_string, add_if_new, group_by_code, numbered_zone, censor, \
build_keys
from fetch_terminals import pull_terminals
import requests
import zipfile
from io import BytesIO # Works only under Python 3
from copy import copy

import time, pytz
from pprint import pprint
from datetime import datetime, timedelta
from dateutil import parser

#from util.db_util import create_or_connect_to_db, get_tables_from_db, get_ps_for_day as db_get_ps_for_day
from util.sqlite_util import get_events_from_sqlite, bulk_upsert_to_sqlite, bulk_upsert_to_sqlite_local, time_to_field, mark_date_as_cached, is_date_cached, mark_utc_date_as_cached, is_utc_date_cached
from notify import send_to_slack

import config # To define a file-crossing global like global_terminal_ids_without_groups.

#from util.carto_util import update_map
from parameters.credentials_file import CALE_API_user, CALE_API_password
from parameters.local_parameters import path, SETTINGS_FILE
from parameters.remote_parameters import BASE_URL
from pipe.pipe_to_CKAN_resource import send_data_to_pipeline, get_connection_parameters, TransactionsSchema, SplitTransactionsSchema, SamplingTransactionsSchema, SplitSamplingTransactionsSchema, OccupancySchema
from pipe.gadgets import get_resource_data

from nonchalance import add_hashes

DEFAULT_TIMECHUNK = timedelta(minutes=10)

last_date_cache = None
all_day_ps_cache = []
dts_cache = []

last_utc_date_cache = None
utc_ps_cache = []
utc_dts_cache = []

global_warnings = defaultdict(int)

temp_zone_info = {'344 - 18th & Carson Lot': {'Latitude': 40.428484093957401,
                 'Longitude': -79.98027965426445},
 '345 - 20th & Sidney Lot': {'Latitude': 40.429380412222464,
                 'Longitude': -79.980572015047073},
 '343 - 19th & Carson Lot': {'Latitude': 40.428526970691195,
                 'Longitude': -79.978395402431488},
 '345 - 20th & Sidney Lot': {'Latitude': 40.429216054112679,
                 'Longitude': -79.977073073387146},
 '338 - 42nd & Butler Lot': {'Latitude': 40.47053200000002,
                 'Longitude': -79.960346247850453},
 '337 - 52nd & Butler Lot': {'Latitude': 40.481067498214522,
                 'Longitude': -79.953901635581985},
 '311 - Ansley Beatty Lot': {'Latitude': 40.463049472104458,
                 'Longitude': -79.926414303372439},
 '355 - Asteroid Warrington Lot': {'Latitude': 40.421746663239325,
                 'Longitude': -79.993341658895474},
 '425 - Bakery Sq': {'Latitude': 40.4560281126722,
                'Longitude': -79.916535012428085},
 '321 - Beacon Bartlett Lot': {'Latitude': 40.435453694403037,
                 'Longitude': -79.923617310019822},
 '363 - Beechview Lot': {'Latitude': 40.411083915458534,
                 'Longitude': -80.024386919130848},
 '418 - Beechview': {'Latitude': 40.409913479391079,
                'Longitude': -80.024733782184967},
 '406 - Bloomfield (On-street)': {'Latitude': 40.461946760727805,
                 'Longitude': -79.946826139799441},
 '361 - Brookline Lot': {'Latitude': 40.392674122243058,
                 'Longitude': -80.018725208992691},
 '419 - Brookline': {'Latitude': 40.393688357340416,
                'Longitude': -80.019989138111754},
 '351 - Brownsville & Sandkey Lot': {'Latitude': 40.384849483758344,
                 'Longitude': -79.977419455740346},
 '416 - Carrick': {'Latitude': 40.386373443728381,
              'Longitude': -79.97945490478287},
 '329 - Centre Craig': {'Latitude': 40.45168996155256,
              'Longitude': -79.95195418596267},
 '323 - Douglas Phillips Lot': {'Latitude': 40.432617056862256,
                 'Longitude': -79.922537281579963},
 '401 - Downtown 1': {'Latitude': 40.441775562513982,
                'Longitude': -79.998573266419925},
 '402 - Downtown 2': {'Latitude': 40.438541198850679,
                'Longitude': -80.001387482255666},
 '342 - East Carson Lot': {'Latitude': 40.42911498849881,
                 'Longitude': -79.98570442199707},
 '412 - East Liberty': {'Latitude': 40.460954767837613,
              'Longitude': -79.926159897229695},
 '371 - East Ohio Street Lot': {'Latitude': 40.454243200345864,
                 'Longitude': -79.999740015542329},
 '307 - Eva Beatty Lot': {'Latitude': 40.461651797420089,
                 'Longitude': -79.927785198164941},
 '324 - Forbes Murray Lot': {'Latitude': 40.438609122362699,
                 'Longitude': -79.922507232308064},
 '322 - Forbes Shady Lot': {'Latitude': 40.438602290037359,
                 'Longitude': -79.920121894069666},
 '335 - Friendship Cedarville Lot': {'Latitude': 40.462314291429955,
                 'Longitude': -79.948193852761278},
 '331 - Homewood Zenith Lot': {'Latitude': 40.455562043993496,
                 'Longitude': -79.89687910306202},
 '328 - Ivy Bellefonte Lot': {'Latitude': 40.45181388509701,
                 'Longitude': -79.933232609325415},
 '325 - JCC/Forbes Lot': {'Latitude': 40.437756155476606,
            'Longitude': -79.923901042327884},
 '405 - Lawrenceville': {'Latitude': 40.467721251303139,
                'Longitude': -79.963118098839757},
 '369 - Main/Alexander Lot': {'Latitude': 40.440717969032434,
                 'Longitude': -80.03386820671949},
 '414 - Mellon Park': {'Latitude': 40.45172469595348,
                'Longitude': -79.919594841104498},
 '420 - Mt. Washington': {'Latitude': 40.432932025800348,
              'Longitude': -80.010913107390707},
 '422 - Northshore': {'Latitude': 40.447064541266613,
                 'Longitude': -80.008874122734966},
 '421 - NorthSide': {'Latitude': 40.454215096885378,
                'Longitude': -80.008679951361657},
 '407 - Oakland 1': {'Latitude': 40.440712434300536,
               'Longitude': -79.962027559420548},
 '408 - Oakland 2': {'Latitude': 40.443878246794903,
               'Longitude': -79.956351936149389},
 '409 - Oakland 3': {'Latitude': 40.447221532200416,
               'Longitude': -79.951424734414488},
 '410 - Oakland 4': {'Latitude': 40.441311089931347,
               'Longitude': -79.94689005613327},
 '375 - Oberservatory Hill Lot': {'Latitude': 40.490002153374341,
                 'Longitude': -80.018556118011475},
 '314 - Penn Circle NW Lot': {'Latitude': 40.463423581089359,
                 'Longitude': -79.926107418017466},
 '411 - Shadyside': {'Latitude': 40.455189648283827,
                 'Longitude': -79.935153703219399},
 '301 - Sheridan Harvard Lot': {'Latitude': 40.462616226637564,
                 'Longitude': -79.923065044145574},
 '302 - Sheridan Kirkwood Lot': {'Latitude': 40.46169199390453,
                 'Longitude': -79.922711968915323},
 '357 - Shiloh Street Lot': {'Latitude': 40.429924701959528,
               'Longitude': -80.007599227402991},
 '415 - SS & SSW': {'Latitude': 40.428051479201962,
                'Longitude': -79.975047048707509},
 '413 - Squirrel Hill': {'Latitude': 40.433581368049765,
                'Longitude': -79.92309870425791},
 '404 - Strip Disctrict': {'Latitude': 40.45040837184569,
                'Longitude': -79.985526114383774},
 '304 - Tamello Beatty Lot': {'Latitude': 40.46097078534487,
                 'Longitude': -79.927121205522525},
 '334 - Taylor Street Lot': {'Latitude': 40.463318543844693,
               'Longitude': -79.950406186508189},
 '403 - Uptown': {'Latitude': 40.439793439383763,
               'Longitude': -79.984900553021831},
 '354 - Walter/Warrington Lot': {'Latitude': 40.42172215989536,
                 'Longitude': -79.995026086156827},
 '423 - West End': {'Latitude': 40.441325754999475,
               'Longitude': -80.033656060668363}}
# 341 - 18th & Sidney is missing from this list.

def get_zone_info(server):
    """Gather useful parameters about each zone (or lot) into a zone_info dictionary."""
    from parameters.remote_parameters import spot_counts_resource_id, lease_counts_resource_id
    zone_info_cache_file = 'zone_info.csv'
    try:
        settings, site, package_id, API_key = get_connection_parameters(server, SETTINGS_FILE)
        records = get_resource_data(site,spot_counts_resource_id,API_key=API_key,count=10000)
        lease_rows = get_resource_data(site,lease_counts_resource_id,API_key=API_key,count=10000)
    except:
        print("Unable to download the zone/lot/lease information. Falling back to the cached file.")
        with open(zone_info_cache_file) as zic:
            list_of_ds = csv.DictReader(zic)
            zone_info = {}
            for d in list_of_ds:
                zone = d['zone']
                zone_info[zone] = d
                del(zone_info[zone]['zone'])
        return zone_info

    zone_info = {}
    leases = {}
    for l in lease_rows:
        leases[l['zone']] = l['active_leases']
    for r in records:
        zone = r['zone']
        zone_info[zone] = {'spaces': r['spaces'], 'type': 'On street' if r['type'] == 'on-street' else 'Lot'}
        if zone in leases.keys():
            try:
                zone_info[zone]['leases'] = int(leases[zone])
            except:
                pass
        try:
            zone_info[zone]['spaces'] = int(zone_info[zone]['spaces'])
            # Subtract lease counts from the number of available spots in each parking lot in get_zone_info to provide a more accurate estimate of percent_occupied.
            if 'leases' in zone_info[zone]:
                zone_info[zone]['spaces'] -= zone_info[zone]['leases']
        except:
            pass


        if zone in temp_zone_info.keys():
            zone_info[zone]['latitude'] = temp_zone_info[zone]['Latitude']
            zone_info[zone]['longitude'] = temp_zone_info[zone]['Longitude']

    # Convert to a list-of-dicts structure for caching to a file:
    list_of_ds = [{'zone': k, **v} for k,v in zone_info.items()]
    keys = sorted(list_of_ds[0].keys())
    # Now cache the resulting zone_info in case it can't be retrieved.
    write_or_append_to_csv(zone_info_cache_file,list_of_ds,keys,actually_overwrite=True)
    return zone_info

def round_time(dt=None, round_to=60, method="half up"):
    """Round a datetime object to any time laps[e] in seconds
    dt : datetime.datetime object, default now.
    roundTo : Closest number of seconds to round to, default 1 minute.
    Author: Thierry Husson 2012 - Use it as you want but don't blame me.
    Modified by drw 2018 with help from https://stackoverflow.com/a/32547090
    """
    if dt == None : dt = datetime.now()
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    if method == 'half up': # Round to the nearest value
        # breaking ties by round 0.5 up to 1.
        rounding = (seconds+round_to/2) // round_to * round_to
    elif method == 'down':
        rounding = seconds // round_to * round_to
    else:
        raise ValueError("round_time doesn't know how to round {}".format(method))
    return dt + timedelta(0,rounding-seconds,-dt.microsecond)

def is_very_beginning_of_the_month(dt):
   return dt.day == 1 and dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0

def beginning_of_day(dt=None):
    """Takes a datetime and returns the first datetime before
    that that corresponds to LOCAL midnight (00:00).

    This function is time-zone agnostic."""
    # Using this function may have been part of the DST-related problems. Hence the
    # creation of localized_beginning_of_day below.
    if dt == None : dt = datetime.now()
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)

def localized_beginning_of_day(local_tz,dt=None):
    """Takes a datetime and returns the first datetime before
    that that corresponds to LOCAL midnight (00:00).

    This function is time-zone agnostic."""
    # Note that this function was created as one attempt to fix the DST-related
    # problems, but now that more times have been switched to using UTC time,
    # it seems to be unneeded.
    if dt == None : dt = datetime.now()
    return (local_tz.normalize(dt)).replace(hour=0, minute=0, second=0, microsecond=0)

def terminal_of(p,t_guids,terminals):
    t = terminals[t_guids.index(p['@TerminalGuid'])]
    return t

def add_duration(p,raw_only=False):
    if raw_only:
        p['Duration'] = None # in minutes
    else:
        p['Duration'] = int(p['@Units']) # in minutes
    return p

def fix_one_duration(p,session,raw_only=False):
    add_duration(p)
    try: # Sort transactions by EndDateLocal.
        ps = sorted(session, key=lambda x: x['@EndDateUtc'])[::-1]
        #pprint(ps)
    except:
        print("len(session) = {}".format(len(session)))
        for e in session:
            if '@EndDateUtc' not in e:
                print("Missing '@EndDateUtc':")
                pprint(to_dict(e))
                raise ValueError("Found a transaction that is missing @EndDateUtc.")
        raise ValueError("Unable to sort session transactions.")

    # Find p in the sorted session list.
    k = 0
    while ps[k] != p:
        k += 1
    assert ps[k] == p

    if len(ps) > 1 and k+1 != len(ps):
        p['Duration'] -= int(ps[k+1]['@Units'])

        if p['Duration'] < 0:
            pprint(ps)
            pprint(p)
            raise ValueError('Negative duration encountered.')
    elif 'Duration' not in p:
        p['Duration'] = int(p['@Units'])

    # Now that each purchase has an associated duration, calculate the true start of the corresponding
    # parking segment (when the car has parked, not when it has paid).
    p['parking_segment_start_utc'] = parking_segment_start_of(p)
    # This is a costly operation, so really calculating Durations and finding the true pay interval bounds should be
    # done when the data is first pulled and stored in the local cache.
    p['segment_number'] = len(ps)-k-1
    #print("Durations: {}, @Units: {}".format([e['Duration'] if 'Duration' in e else None for e in ps], [int(e['@Units']) for e in ps]))

def fix_durations(session,raw_only=False):
    """This function accepts a pre-grouped set of purchases that comprise
    a single car's parking session. This is typically one to five transactions.
    These purchases can be assigned new fields (like 'Duration') which are
    computed by examining other purchases to infer the real parking time
    associated with each transaction."""
    try: # Sort transactions by EndDateUtc.
        ps = sorted(session, key=lambda x: x['@EndDateUtc'])[::-1]
    except:
        print("len(session) = {}".format(len(session)))
        for e in session:
            if '@EndDateUtc' not in e:
                print("Missing '@EndDateUtc':")
                pprint(to_dict(e))
                raise ValueError("Found a transaction that is missing @EndDateUtc.")
        raise ValueError("Unable to sort session transactions.")

    for p in ps:
        if 'Duration' not in p:
            add_duration(p,raw_only)
        else:
            print("p with pre-existing Duration field found in fix_durations.")

    for k,p in enumerate(ps):
        # Subtract the durations of the previous payments.
        # If the minutes purchased are 10, 30, 5,
        # the Units fields will have values 10, 40, and 45, when
        # sorted in chronological order.

        # Reversing this process, we start with the most recent
        # transaction (45) and subtract the previous (40) to
        # get the minutes purchased (5).

        if k+1 != len(ps):
            # Subtract off the cumulative minutes purchased by the most recent
            # predecessor, so that the Duration field represents just the Duration
            # of this transaction. (Duration is the incremental number of minutes
            # purchased, while the '@Units' field is the CUMULATIVE number of
            # minutes.)
            p['Duration'] -= int(ps[k+1]['@Units'])

            # Now that each purchase has an associated duration, calculate the true start of the corresponding
            # parking segment (when the car has parked, not when it has paid).
            if p['Duration'] < 0:
                pprint(session)
                pprint(p)
                raise ValueError('Negative duration encountered.')

        p['parking_segment_start_utc'] = parking_segment_start_of(p)
        p['segment_number'] = len(ps)-k-1

def hash_reframe(p,terminals,t_guids,hash_history,previous_history,uncharted_n_zones,uncharted_e_zones,turbo_mode,raw_only,transactions_only,extend=True):
    """Take a dictionary and generate a new dictionary from it that samples
    the appropriate keys and renames and transforms as desired.

    In contrast with reframe, which used ps_dict with its uncertain linking between transactions,
    hash_reframe is hashing unique identifiers to take the guesswork out of linking transactions
    into sessions (at the cost of preventing past transactions from being linked)."""
    row = {}
    #row['GUID'] = p['@PurchaseGuid'] # To enable this field,
    # get_batch_parking_for_day needs to be tweaked and
    # JSON caches need to be regenerated.
    try:
        row['TerminalGUID'] = p['@TerminalGuid'] # This is useful
    # for connecting purchases with terminals when the ID changes
    # but the GUID does not change.
    except:
        print("p['@TerminalGuid'] is missing from {}".format(p))
    row['TerminalID'] = p['@TerminalID']
    if p['@TerminalGuid'] in t_guids:
        t = terminals[t_guids.index(p['@TerminalGuid'])]
        if extend:
            row['Latitude'] = value_or_blank('Latitude',t)
            row['Longitude'] = value_or_blank('Longitude',t)
            # Maybe these should be value_or_none instead.
            row['List_of_sampling_groups'] = sampling_groups(t,uncharted_n_zones,uncharted_e_zones)

    row['Amount'] = float(p['@Amount'])
    if not transactions_only:
        if 'Duration' in p:
            row['Duration'] = p['Duration']
        else:
            row['Duration'] = None
    row['Is Mobile Payment'] = is_mobile_payment(p)
    return row

def find_biggest_value(d_of_ds,field='transactions'):
    return sorted(d_of_ds,key=lambda x:d_of_ds[x][field])[-1]

def update_occupancies(inferred_occupancy,stats_by_zone,slot_start,timechunk):
    """This function uses the parking durations inferred by trying to piece
    together sessions from individual transactions to synthesize an
    estimated count of parked cars for each zone and time chunk,
    starting at slot_start and going forward.

    No correction factors have been applied yet."""
    delta_minutes = timechunk.total_seconds()/60.0
    for zone in stats_by_zone:
        #durations = json.loads(stats_by_zone[zone]['Durations']) # No longer necessary
        # since this field is going to be a list of integers until package_for_output
        # is called.
        durations = stats_by_zone[zone]['Durations']
#        if len(durations) > 0:
#            print "\ndurations for zone {} = ".format(zone)
#            pprint(durations)
        for d_i in durations:
            bins = int(round(float(d_i)/delta_minutes))
            # Rounding like this means that for a timechunk of 10 minutes,
            # 1-4 minute parking sessions will not add to inferred
            # occupancy, while 5-14 minute sessions will add ten minutes
            # of apparent occupancy. This will work perfectly if the
            # timechunk is one minute (i.e., no occupancy will be lost
            # due to rounding errors).


            # [ ] What if instead of rounding, we use fractional cars?
            # A car that is parked for 3 minutes out of a 10-minute
            # slot is 0.3 cars. But for this to really work well, we'd
            # need to know not just the durations for each slot, but
            # the start time for each duration. Using rounded occupancies
            # is good for generating occupancy estimates from
            # aggregated statitistics.

            # [ ] Compare this method to an exact transaction-by-transaction
            # calculation of occupancy.
            for k in range(0,bins):
                inferred_occupancy[slot_start+k*timechunk][zone] += 1
                # inferred_occupancy is measured in cars (or used parking spaces),
                # though a more useful metric would be percent_occupied.
#        if len(durations) > 0:
#            print "inferred_occupancy for zone {} =".format(zone)
#            for t in sorted(inferred_occupancy.keys()):
#                print t, to_dict(inferred_occupancy[t])
    return inferred_occupancy

def format_a_key(meter_id,year,month,hour):
#    return "{}|{}/{} {:02d}".format(meter_guid,year,month,hour)
    return "{}/{} {:02d}|{}".format(year,month,hour,meter_id)

def initialize_zone_stats(start_time,end_time,space_aggregate_by,time_aggregate_by,split_by_mode,tz=pytz.timezone('US/Eastern'), transactions_only=True):
    stats = {}

    # This is where it would be nice to maybe do some different formatting based on the
    # time_aggregation parameter (since now a bin is not defined just by start and end
    # but also by year-month. The other possibility would be to do it when the month
    # is archived (from the loop in main()).
    start_time_local = start_time.astimezone(tz)
    stats['start'] = datetime.strftime(start_time_local,"%Y-%m-%d %H:%M:%S")
    # [ ] Is this the correct start time?
    end_time_local = end_time.astimezone(tz)
    stats['end'] = datetime.strftime(end_time_local,"%Y-%m-%d %H:%M:%S")
    start_time_utc = start_time.astimezone(pytz.utc)
    stats['utc_start'] = datetime.strftime(start_time_utc,"%Y-%m-%d %H:%M:%S")
    if not split_by_mode:
        stats['transactions'] = 0
        stats['Payments'] = 0.0
    else:
        stats['meter_transactions'] = 0
        stats['Meter Payments'] = 0.0
        stats['mobile_transactions'] = 0
        stats['Mobile Payments'] = 0.0

    if not transactions_only:
        stats['car_minutes'] = 0
        stats['Durations'] = [] # The Durations field represents the durations of the purchases
        # made during this time slot. Just as transactions indicates how many times people
        # put money into parking meters (or virtual meters via smartphone apps) and
        # Payments tells you how much money was paid, Durations tells you the breakdown of
        # parking minutes purchased. The sum of all the durations represented in the
        # Durations list should equal the value in the car_minutes field. This field has been
        # changed to a list data structure until package_for_output, at which point it is
        # reformatted into a dictionary.
    if space_aggregate_by == 'sampling zone':
        stats['parent_zone'] = None
    if time_aggregate_by == 'month':
        stats['Year'] = start_time.astimezone(tz).strftime("%Y")
        stats['Month'] = start_time.astimezone(tz).strftime("%m")
        stats['Hour'] = start_time.astimezone(tz).strftime("%-H")
        stats['UTC Hour'] = start_time.astimezone(pytz.utc).strftime("%-H")
    return stats

def distill_stats(rps,terminals,t_guids,t_ids,group_lookup_addendum,start_time,end_time, stats_by={},zone_kind='old', space_aggregate_by='zone', time_aggregate_by=None, split_by_mode=False, parent_zones=[], tz=pytz.timezone('US/Eastern'), transactions_only=True):
    # Originally this function just aggregated information
    # between start_time and end_time to the zone level.

    # Then it was modified to support sampling zones,
    # allowing the function to be called separately just to
    # get sampling-zone-level aggregation.

    # THEN it was modified to also allow aggregation by
    # meter ID instead of only by zone.

    # If 'Duration' does not have a non-None value in any of the rps,
    # distill_stats will not add Durations and car-minutes fields.
    global global_warnings

    for k,rp in enumerate(rps):
        t_guid = rp['TerminalGUID']
        t_id = rp['TerminalID']
        zone = None
        space_aggregation_keys = []
        aggregation_keys = []

        if space_aggregate_by == 'zone':
            if t_guid in t_guids:
                t = terminals[t_guids.index(t_guid)]
            else:
                t = None

            if zone_kind == 'new':
                zone, _, _ = numbered_zone(t_id, t, group_lookup_addendum)
            elif t is not None:
                if zone_kind == 'old':
                    zone = corrected_zone_name(t) # Changed
                    # from zone_name(t) to avoid getting
                    # transactions in "Z - Inactive/Removed Terminals".
            else:
                print("OH NO!!!!!!!!!!!\n THE TERMINAL FOR THIS PURCHASE CAN NOT BE FOUND\n BASED ON ITS GUID!!!!!!!!!!!!!!!")
                if zone_kind == 'old':
                    zone = corrected_zone_name(None,t_ids,rp['TerminalID'])

            if zone is not None: # zone can be None if a numbered zone cannot be identified, meaning that
                # space_aggregation_keys will be empty, and the transaction will not be have a bin to
                # be published in.
                space_aggregation_keys = [zone]
        elif space_aggregate_by == 'sampling zone':
            if 'List_of_sampling_groups' in rp and rp['List_of_sampling_groups'] != []:
                space_aggregation_keys = rp['List_of_sampling_groups']
# The problem with this is that a given purchase is associated with a terminal which may have MULTIPLE sampling zones. Therefore, each sampling zone must have its own parent zone(s).
        elif space_aggregate_by == 'meter':
            space_aggregation_keys = [t_id] # Should this be GUID or just ID? ... Let's
                # make it GUID (as it will not change), but store meter ID as
                # an additional field
                #       I've decided to switch to ID for nicer sorting, and because
                # maybe it actually makes a little more sense to highlight the changes
                # associated with an ID change. (Sometimes this is fixing a typo or
                # a small change but it might be a larger change. In any event,
                # the user would have both ID and GUID in this meter-month-hour
                # aggregation mode.

        if space_aggregation_keys != []:
            space_aggregation_keys = censor(space_aggregation_keys,space_aggregate_by) # The censor function filters out
            # forbidden zones, both for regular zones and sampling zones and requires that sampling zones be
            # pre-approved (that is, in the designated_minizones list).
            if time_aggregate_by is None:
                aggregation_keys = space_aggregation_keys
            elif time_aggregate_by == 'month':
                time_key = start_time.astimezone(tz).strftime("%Y/%m %H") #+ "-"+ start_time.astimezone(pytz.utc).strftime("%H")
                # [X] Actually, do we really need to distinguish between the two 1-2am slots on the one day of the year when
                # the clocks shift back? Maybe we can just combine those together.
                year = start_time.astimezone(tz).strftime("%Y")
                month = start_time.astimezone(tz).strftime("%m")
                hour = int(start_time.astimezone(tz).strftime("%H"))
                #aggregation_keys = [s_a_k + "|" + time_key for s_a_k in space_aggregation_keys]
                aggregation_keys = [format_a_key(s_a_k,year,month,hour) for s_a_k in space_aggregation_keys]
                #time_key = start_time.astimezone(tz).strftime("%Y/%m %H") + "-"+ start_time.astimezone(pytz.utc).strftime("%H")
                #aggregation_keys = [(s_a_k, time_key) for s_a_k in space_aggregation_keys if not_censored(s_a_k)]
            # The above could really stand to be refactored.
            if aggregation_keys != []:
                for a_key in aggregation_keys:
                    zone = a_key.split('|')[0]
                    if a_key not in stats_by:
                        stats_by[a_key] = initialize_zone_stats(start_time,end_time,space_aggregate_by,time_aggregate_by,split_by_mode, tz=pytz.timezone('US/Eastern'), transactions_only=transactions_only)

                    stats_by[a_key]['zone'] = zone

                    if space_aggregate_by == 'sampling zone':
                        if 'parent_zone' in stats_by[a_key]:
                            #for zone in space_aggregation_keys:
                            # There are now cases where getting the zone from space_aggregation_keys
                            # for space_aggregate_by == 'sampling zone' results in multiple zones
                            # since the value comes from rp['List_of_sampling_groups']. Basically,
                            # a terminal group can be assigned to an arbitary number of Terminal
                            # Groups, and we are getting the ones that are not sampling zones,
                            # so one terminal can be both in 'CMU Study' and 'Marathon/CMU', for
                            # instance.
                            #
                            # It seems like the correct thing to do in this case is add the
                            # transactions to both sampling zones.
                            # This should actually happen naturally if the space part of the
                            # aggregation key could be pulled off and used as the zone in
                            # each case, which is what I've done.
                            # This output seems to be the same as before space-time aggregation
                            # was added.
                            if zone in parent_zones:
                                stats_by[a_key]['parent_zone'] = '|'.join(parent_zones[zone])
                            else:
                                msg = "sampling zone = {} is not listed in parent_zones, though this may just be because process_data.py is working off of a cached file.".format(zone)
                                print(msg)
                                global_warnings[msg] += 1
                                stats_by[a_key]['parent_zone'] = ''

                    elif space_aggregate_by == 'meter':
                        stats_by[a_key]['Meter GUID'] = t_guid
                        stats_by[a_key]['Meter ID'] = t_id
                        nz, _, _ = numbered_zone(t_id,None,group_lookup_addendum)
                        stats_by[a_key]['zone'] = nz

                    if not split_by_mode:
                        stats_by[a_key]['transactions'] += 1
                        stats_by[a_key]['Payments'] += rp['Amount']
                    else: # Split payments into mobile and meter payments
                        if rp['Is Mobile Payment']:
                            stats_by[a_key]['mobile_transactions'] += 1
                            stats_by[a_key]['Mobile Payments'] += rp['Amount']
                        else:
                            stats_by[a_key]['meter_transactions'] += 1
                            stats_by[a_key]['Meter Payments'] += rp['Amount']

                    if not transactions_only and 'Duration' in rp and rp['Duration'] is not None:
                        stats_by[a_key]['car_minutes'] += rp['Duration']
                        stats_by[a_key]['Durations'].append(rp['Duration'])

    return stats_by

def build_url(base_url,slot_start,slot_end):
    """This function takes the bounding datetimes, checks that
    they have time zones, and builds the appropriate URL,
    converting the datetimes to UTC (which is what the CALE
    API expects).

    This function is called by get_batch_parking_for_day
    (and was also used by get_recent_parking_events)."""

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
    """Remove a bunch of unneeded fields."""
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
    #purchases = remove_field(purchases,'@TicketNumber') # Commented out 2019-01-28
    #purchases = remove_field(purchases,'@TariffPackageID') # Commented out 2019-01-28
    #purchases = remove_field(purchases,'@ExternalID') # Commented out 2019-01-28
    #purchases = remove_field(purchases,'@PurchaseStateName')
    purchases = remove_field(purchases,'@PurchaseTriggerTypeName')
    #purchases = remove_field(purchases,'@PurchaseTypeName')#
    purchases = remove_field(purchases,'@MaskedPAN','PurchasePayUnit')
    purchases = remove_field(purchases,'@BankAuthorizationReference','PurchasePayUnit')
    purchases = remove_field(purchases,'@CardFeeAmount','PurchasePayUnit')
    purchases = remove_field(purchases,'@PayUnitID','PurchasePayUnit')
    #purchases = remove_field(purchases,'@TransactionReference','PurchasePayUnit')
    purchases = remove_field(purchases,'@CardIssuer','PurchasePayUnit')

    return purchases

def get_doc_from_url(url):
    r = requests.get(url, auth=(CALE_API_user, CALE_API_password))

    if r.status_code == 403: # 403 = Forbidden, meaing that the CALE API
        # has decided to shut down for a while (maybe for four hours
        # after the last query of historical data).
        raise RuntimeError("The CALE API is returning a 403 Forbidden error, making it difficult to accomplish anything.")

    # Convert Cale's XML into a Python dictionary
    doc = xmltodict.parse(r.text,encoding = r.encoding)

    try:
        # This try-catch clause is only protecting one of the three cases where
        # the function is reading into doc without being sure that the fields
        # are there (occasionally in practice they are not because of unknown
        # stuff on the API end).

        # The next time such an exception is thrown, it might make sense to
        # look at what has been printed from doc and maybe put the call
        # to get_doc_from_url into a try-catch clause.
        url2 = doc['BatchDataExportResponse']['Url']
    except:
        pprint(doc)
        print("Unable to get the first URL by using the command url2 = doc['BatchDataExportResponse']['Url'].")
        print("Waiting 10 seconds and restarting.")
        time.sleep(10)
        return None, False

    r2 = requests.get(url2, auth=(CALE_API_user, CALE_API_password))
    if r2.status_code == 403:
        raise RuntimeError("The CALE API is returning a 403 Forbidden error, making it difficult to accomplish anything.")

    doc = xmltodict.parse(r2.text,encoding = r2.encoding)

    delays = 0
    while not r2.ok or doc['BatchDataExportFileResponse']['ExportStatus'] != 'Processed':
        time.sleep(10)
        r2 = requests.get(url2, auth=(CALE_API_user, CALE_API_password))
        doc = xmltodict.parse(r2.text,encoding = r2.encoding)
        delays += 1
        if delays % 5 == 0:
            print("|", end="", flush=True)
        else:
            print(".", end="", flush=True)
        if delays > 30:
            return None, False

    url3 = doc['BatchDataExportFileResponse']['Url']

    # When the ZIP file is ready:
    delays = 0
    r3 = requests.get(url3, stream=True, auth=(CALE_API_user, CALE_API_password))
    if r3.status_code == 403:
        raise RuntimeError("The CALE API is returning a 403 Forbidden error, making it difficult to accomplish anything.")
    while not r3.ok and delays < 20:
        time.sleep(5)
        r3 = requests.get(url3, stream=True, auth=(CALE_API_user, CALE_API_password))
        delays += 1
        if delays % 5 == 0:
            print("|", end="", flush=True)
        else:
            print(",", end="", flush=True)
        if delays > 30:
            return None, False

    z = zipfile.ZipFile(BytesIO(r3.content))
    time.sleep(0.5)

    # Extract contents of a one-file zip file to memory:
    xml = z.read(z.namelist()[0])
    doc = xmltodict.parse(xml,encoding = 'utf-8')
    return doc, True

def get_day_from_json_or_api(slot_start,tz,cache=True,mute=False,utc_json_folder='utc_json'):
    """Caches parking once it's been downloaded and checks
    cache before redownloading.

    Note that no matter what time of day is associated with slot_start,
    this function will get all of the transactions for that entire day.
    Filtering the results down to the desired time range is handled
    elsewhere (in the calling function (e.g., get_utc_ps_for_day_from_json)).

    Caching by date ties this approach to a particular time zone. This
    is why transactions are dropped if we send this function a UTC
    slot_start (I think).

    This function seems to give the same result whether slot_start is
    localized for UTC or Eastern, so long as tz is pytz.utc."""

    date_format = '%Y-%m-%d'
    slot_start = slot_start.astimezone(tz) # slot_start needs to already
    # have a time zone associated with it. This line forces slot_start to be
    # in timezone tz, even if it wasn't before.

    dashless = slot_start.strftime('%y%m%d')
    if tz == pytz.utc:
        filename = path + utc_json_folder + "/" + dashless + ".json"
    else:
        filename = path + "json/"+dashless+".json"

    too_soon = slot_start.date() >= datetime.now(tz).date()
    # If the day that is being requested is today, definitely don't cache it.

    recent = datetime.now(tz) - slot_start <= timedelta(days = 5) # This
    # definition of recent is a little different since a) it uses slot_start
    # rather than slot_end (which is fine here, as we know that slot_start
    # and slot_end are separated by one day) and b) it uses the time zone tz
    # (though that should be fine since slot_start has already been converted
    # to time zone tz).

    if not os.path.isfile(filename) or os.stat(filename).st_size == 0:
        if not mute:
            print("Sigh! {} not found, so I'm pulling the data from the API...".format(filename))

        slot_start = beginning_of_day(slot_start)
        slot_end = slot_start + timedelta(days = 1)

        if recent:
            base_url = f'{BASE_URL}LiveDataExport/4/LiveDataExportService.svc/purchases/'
        else:
            base_url = f'{BASE_URL}BatchDataExport/4/BatchDataExport.svc/purchase/ticket/'

        url = build_url(base_url,slot_start,slot_end)

        if not mute:
            print("Here's the URL: {}".format(url))

        if recent:
            # [ ] pull_from_url currently has a different retry-on-failure model
            # than get_doc_from_url (which is wrapped in a while loop so it will
            # either succeed or keep trying forever, while pull_from_url eventually
            # gives up).
            r = pull_from_url(url)
            doc = xmltodict.parse(r.text,encoding = 'utf-8')
            ps = convert_doc_to_purchases(doc,slot_start,date_format)
        else:
            downloaded = False
            while not downloaded:
                doc, downloaded = get_doc_from_url(url)
                print("!", end="", flush=True)

            ps = convert_doc_to_purchases(doc['BatchExportRoot'],slot_start,date_format)

        ps = add_hashes(ps)
        purchases = cull_fields(ps)

        #print("cache = {}, recent = {}, too_soon = {}".format(cache,recent,too_soon))


        if cache and not too_soon:
            # Check if directory exists.
            directory = '/'.join(filename.split('/')[:-1])
            if not os.path.isdir(directory):
                os.mkdir(directory)
            # Caching data from the LiveDataExport endpoint (but not today's data) is an interesting experiment.
            with open(filename, "w") as f:
                json.dump(purchases,f,indent=2)
            if recent:
                print(" !!!!!!!!!!! Cached some data from the LiveDataExport endpoint in {}".format(filename))
    else: # Load locally cached version
        with open(filename, "r", encoding="utf-8") as f:
            ps = json.load(f)

    return ps

def get_batch_parking_for_day(slot_start,tz,cache=True,mute=False,utc_json_folder="utc_json"):
    """Caches parking once it's been downloaded and checks
    cache before redownloading.

    Note that no matter what time of day is associated with slot_start,
    this function will get all of the transactions for that entire day.
    Filtering the results down to the desired time range is handled
    elsewhere (in the calling function (get_batch_parking)).


    Caching by date ties this approach to a particular time zone. This
    is why transactions are dropped if we send this function a UTC
    slot_start (I think) and try to use the Eastern Time Zone JSON
    files. This has been fixed by specifying the timezone
    and distinguishing between JSON-file folders."""

    ps = get_day_from_json_or_api(slot_start,tz,cache,mute,utc_json_folder)

    return ps

def get_batch_parking(slot_start,slot_end,cache,mute=False,utc_json_folder="utc_json",tz=pytz.timezone('US/Eastern'),time_field = '@PurchaseDateLocal',dt_format='%Y-%m-%dT%H:%M:%S'):
    """This function handles the situation where slot_start and slot_end are on different days
    by calling get_batch_parking_for_day in a loop.

    The parameter "time_field" determines which of the timestamps is used for calculating
    the datetime values used to filter purchases down to those between slot_start
    and start_end.


    Note that the time zone tz and the time_field must be consistent for this to work properly."""
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
            ps_for_whole_day = get_batch_parking_for_day(dt_start_i,tz,cache,mute,utc_json_folder,utc_json_folder)
            ps_all += ps_for_whole_day
            dt_start_i += timedelta(days = 1)
            if not mute:
                print("Now there are {} transactions in ps_all.".format(len(ps_all)))

        all_day_ps_cache = ps_all # Note that if slot_start and slot_end are not on the same day,
        # all_day_ps_cache will hold transactions for more than just the date of slot_start, but
        # since filtering is done further down in this function, this should not represent a
        # problem. There should be no situations where more than two days of transactions will
        # wind up in this cache at any one time.
        dts_cache = [tz.localize(parser.parse(p[time_field])) for p in ps_all]
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
def get_payment_type(p):
    # If the 'PurchasePayUnit' field cannot be found, use the terminal ID
    # to detect whether it's a virtual payment.
    if 'PurchasePayUnit' not in p:
        terminal_id = p['@TerminalID']
        if terminal_id[:3] == 'PBP':
            return 'mobile'
        elif re.match("^\d\d\d\d$", terminal_id) is not None or re.match("^\d\d\d\d\d$", terminal_id) is not None:
            # Terminals with IDs like 5593.
            # These usually have PurchasePayUnit fields and everyone I've checked has been mobile.
            return 'mobile'
        elif terminal_id[0] in ['2', '3', '4']:
            return 'meter'
        elif terminal_id[:4] == 'MTFD':
            # Observations: Where there are coordinates for MTFD terminals, they appear
            # to be the locations of those blue boxes.
            # All MTFD transactions appear to be mobile transactions, so let's assume
            # that the payment type is mobile.
            return 'mobile'
        else:
            pprint(p)
            raise ValueError("Unknown terminal type for terminal ID {} from payment {}.".format(terminal_id,p))

    if type(p['PurchasePayUnit']) == list: # It's a list of Coin and Card payments.
        return 'meter'
    pay_unit_name = p['PurchasePayUnit']['@PayUnitName']
    if pay_unit_name == 'Mobile Payment':
        return 'mobile'
    else: # In addition to "Mobile Payment" and "Coin" and "Card", there's also now "Manual", which is ignorable.
        if pay_unit_name == 'Manual':
            return 'manual'
        elif pay_unit_name in ['Coin', 'Card', 'None']:
            return 'meter'
        else:
            raise ValueError("Unknown payment type for @PayUnitName {} from payment {}.".format(pay_unit_name,p))

def is_mobile_payment(p):
    return get_payment_type(p) == 'mobile'

def hybrid_parking_segment_start_of(p):
    # In the CALE API, @Units == @EndDateUtc - @StartDateUtc.
    # The same relation does not hold for @PurchaseDateUtc, which in rare
    # cases can be on a different day from @StartDateUtc.
    # Observed differences have been @PurchaseDateUtc - @StartDateUtc = 86393 seconds
    # (in which case @PurchaseDateUtc was close to the delayed @DateCreatedUtc value)
    # and -16504 seconds (in which case @StartDateUtc and @DateCreatedUtc were
    # pretty consistent, but @PurchaseDateUtc was hours before either).

    # For mobile transactions, PurchaseDateUtc == StartDateUtc.

    time_field = {'mobile': p['@StartDateUtc'],
            'meter': p['@StartDateUtc']}

    if is_mobile_payment(p):
        payment_type = 'mobile'
    else:
        return time_field['meter']

    assert payment_type == 'mobile'

    # Check whether the payment was purchased inside or outside of regular parking hours.
    sdl = p['@StartDateLocal']
    edl = p['@EndDateLocal']
    amount = p['@Amount']
    units = p['@Units']

    if amount == '0': # For mobile transactions only.
        return time_field['mobile']
        # What should we return if Amount == 0? There is no money associated with
        # that transaction, so it's fine to return the apparent start time.

        # [ ] Eventually the actual start time (inferred from synthesized sessions
        # and true durations) should be used.

    if sdl[11:13] in ['07','06','05','04','03','02','01','00']:
        # Nowhere is parking metered after midnight, so any purchases between
        # midnight and 8am (with amount != 0) can be assumed to take effect at 8am.
        if edl[11:13] in ['07','06','05','04','03','02','01','00']:
            # However, reviewing the data found six instances where non-zero mobile payments
            # took place between midnight and 1am. In each instance, the amount was 5 cents.
            # All transactions took place on May 17th, 2016, netted ten minutes, and happened
            # for the same lot (Asteroid Warrington Lot). These seem to be anomalies that
            # slipped through, maybe in the early days of mobile transactions.
            return time_field['mobile']

        #dt_sdu = (pytz.utc).localize(parser.parse(p['@StartDateUtc']))
        pgh = pytz.timezone('US/Eastern')
        dt_sdl = (pgh).localize(parser.parse(sdl))
        dt_8am = dt_sdl.replace(hour=8, minute=0, second=0, microsecond=0)
        #offset = dt_8am - dt_sdl
        dt_utc = dt_8am.astimezone(pytz.utc)

        #print("A time that definitely needs to be coerced: {}. It shall be coerced to {}, which is {} in UTC.".format(sdl,dt_8am,dt_utc))
        #pprint(p)
        back_to_string = dt_utc.strftime("%Y-%m-%dT%H:%M:%S")
        return back_to_string

    elif sdl[11:13] in ['23','22','21','20','19','18']:
        if edl[11:13] in ['00','23','22','21','20','19','18']:
            return time_field['mobile']
            #if amount != '0':
            #    #print("Start = {}, End = {}, Units = {}. This looks OK.".format(sdl,edl,units))
            #    return time_field['mobile']
            #else:
            #    pass
            #    print("Amount = '0!'")
            #    pprint(p)
        else:
            #print("Start = {}, End = {}, Units = {}, Amount = {}. This looks weird.".format(sdl,edl,units,amount))
            #It's not clear if this needs to be coereced.
            #pprint(p)
            #raise ValueError("Evaluate this purchase to decide how to handle it.") # None of these have been found to date.

            # New exception that triggers this branch:
            # Start = 2021-08-11T21:10:00, End = 2021-08-12T08:00:00, Units = 650, Amount = 0.83. This looks weird.
            #    OrderedDict([('@PurchaseGuid', '**********************'),
            #     ('@TerminalGuid', 'DAF2E9A3-AF9B-491B-92F1-A0C4244AD904'),
            #     ('@TerminalID', 'PBP 412043'),
            #     ('@PurchaseDateLocal', '2021-08-11T21:09:58'),
            #     ('@PurchaseDateUtc', '2021-08-12T01:09:58'),
            #     ('@StartDateLocal', '2021-08-11T21:10:00'),
            #     ('@StartDateUtc', '2021-08-12T01:10:00'),
            #     ('@PayIntervalStartLocal', '2021-08-11T21:10:00'),
            #     ('@PayIntervalStartUtc', '2021-08-12T01:10:00'),
            #     ('@PayIntervalEndLocal', '2021-08-12T08:00:00'),
            #     ('@PayIntervalEndUtc', '2021-08-12T12:00:00'),
            #     ('@EndDateLocal', '2021-08-12T08:00:00'),
            #     ('@EndDateUtc', '2021-08-12T12:00:00'),
            #     ('@TicketNumber', '0'),
            #     ('@Units', '650'),
            #     ('@Amount', '0.83'),
            #     ('@TariffPackageID', '315'),
            #     ('@ExternalID', '*******************'),
            #     ('@DateCreatedUtc', '2021-08-12T01:10:32.180'),
            #     ('@PurchaseStateName', 'Completed'),
            #     ('@PaymentServiceType', 'None'),
            #     ('@PurchaseTypeName', 'Normal'),
            #     ('PurchasePayUnit',
            #      OrderedDict([('@PayUnitName', 'Mobile Payment'),
            #                   ('@Amount', '0.83'),
            #                   ('@TransactionReference', '********')])),

            # .83 * 60 = 50 minutes, so the charge is for parking between 21:10 and 22:00. The coercion would have to
            # be to coerce the end time (not the start time).
            return time_field['mobile'] # If end-time coercion were happening/necessary, this is a case
            # where it should be done, but as far as I can tell, nothing like that is happening.

    else:
        return time_field['mobile']

    # At present what the hybrid_parking_segment_start_of function boils down to is returning 8am for non-free
    # parking that starts between midnight and 8am and StartDateUtc for all other cases (except starts
    # between 6pm and midnight that end in some other part of the day (midnight to 6pm), which would throw
    # an exception if one of these transactions were found).

    # DateCreatedUtc can be just too far off from StartDateUtc. For now, use StartDateUtc as the best
    # estimate for the start time of each parking segment, with the intent of replacing this with
    # inferred true parking-segment-start times as soon as possible.

def parking_segment_start_of(p):
    # Test this calculation for various cases (mobile vs. non-mobile,
    # before parking hours, during parking hours, and after parking hours)
    # to be sure that it is sufficiently general.

    # Note that this is only usable when the duration of the transaction
    # can be determined. For historical data, this is easy for mobile
    # transactions and hard for meter transactions.
    return (pytz.utc).localize(parser.parse(p['@EndDateUtc'])) - timedelta(minutes=p['Duration'])

def keep_running(slot_start_time,halting_time):
    return slot_start_time <= datetime.now(pytz.utc) and slot_start_time < halting_time

def add_n_years(p,dt_field,n):
    if dt_field in p:
        dt_value = parser.parse(p[dt_field])
        new_value = dt_value.replace(year = dt_value.year + n)
        p[dt_field] = new_value.strftime("%Y-%m-%dT%H:%M:%S")

def get_utc_ps_for_utc_day_from_json(slot_start,reference_time='purchase_time_utc',cache=True,mute=False,utc_json_folder='utc_json'):
    # A variant of get_utc_ps_for_day_from_json that returns
    # all the transactions for a particular UTC day (from
    # UTC midnight to UTC midnight), eliminating all the
    # Daylight-Savings-Time-induced errors.

    # Solves most of the DateCreatedUtc-StartDateUtc discrepancy by
    # collecting data over two UTC days (from a function that
    # caches API query results as raw JSON files) and then filtering
    # those results down to a single UTC day.

    # Thus, the sequence is
    #       get_day_from_json_or_API: Check if the desired day is
    #       in a JSON file. If not, it fetches the data from the API.
    #       Adding hashes, culling fields, and saving local JSON
    #       files of transactions happens here.
    #
    #       get_utc_ps_for_day_from_json: Takes lots of data (filtered
    #       by DateCreatedUtc) and synthesizes it into a single day
    #       of purchases, filtered instead by StartDateUtc.
    #
    #       cache_in_memory_and_filter: Caches the most recent UTC day
    #       of purchases in memory (or else uses the existing cache)
    #       and then filters the results down to the desired slot.
    #
    #       get_parking_events: Dispatches the correct function based
    #       on recency of the slot and then by caching method.


    # As suggested by the name, this function is designed specifically
    # for the 'utc_json' caching mode.
    ###
    # Note that no matter what time of day is associated with slot_start,
    # this function will get all of the transactions for that entire day.
    # Filtering the results down to the desired time range is handled
    # elsewhere (in the calling function).
    ###############
    # This function gets parking purchases, either from a
    # JSON cache (if the date appears to have been cached)
    # or else from the API (and then caches the whole thing
    # if cache == True), by using get_batch_parking_for_day.

    # Note that no matter what time of day is associated with slot_start,
    # this function will get all of the transactions for that entire day.

    # Filtering the results down to the desired time range is now handled
    # in this functions (though the function only marks a date as cached
    # when it has added all events from the day of slot_start (based on
    # StartDateUtc)).

    # This approach tries to address the problem of the often
    # gigantic discrepancy between the DateCreatedUtc timestamp
    # and the StartDateUtc timestamp.

    #ref_field = '@StartDateUtc' # This should not be changed. # But it looks like I'm changing it.

    #for each date in day before, day of, and day after (to get all transactions)
        # [Three full days is probably slightly overkill since the most extreme
        # negative case of DateCreatedUtc - StartDateUtc observed so far has
        # been -12 hours.]

    # To avoid issues with missing or extra hours during Daylight Savings Time,
    # all slot times should be in UTC.

    # The cached_dates format is also UTC dates. (This change was made to
    # route around issues encountered when using localized versions of
    # StartDateUtc and converting to dates.)

    #pgh = pytz.timezone('US/Eastern') # This time zone no longer needs to be hard-coded
    # since get_batch_parking_for_day has been fixed to work for different time zones
    # (I think). [Actually, we do need to use the local time zone so that beginning_of_day
    # returns the correct time. This time zone is now being passed up to this function
    # using the local_tz variable.]
    slot_start = slot_start.astimezone(pytz.utc)

    # Time-zone-sensitivity problem: This function gives different results for
    # slot_start = 2018-09-23 04:00:00+00:00
    # and
    # slot_start = 2018-09-23 00:00:00-04:00
    # even though these are two different representations of the same time.

    print("############# slot_start = {}, beginning_of_day(slot_start) = {}, slot_start.utcoffset().total_seconds() = {}".format(slot_start,beginning_of_day(slot_start),slot_start.utcoffset().total_seconds()))

    #reference_time = 'hybrid'
    #reference_time = 'purchase_time' # Switching to PurchaseDate as a reference for
    # comparison with CALE Web Office results (even though this timestamp is
    # sometimes problematically different from StartDate).
    print("Using {} reference-time mode.".format(reference_time))

    ps_by_day = defaultdict(list)
    dts_by_day = defaultdict(list)
    ps_all = []
    dts_all = []
    recent = datetime.now(pytz.utc) - slot_start <= timedelta(days = 5)
    ## Begin diagnostics #
    #sought_key = 'ECA8F05C-F220-E948-9FCA-2A51BA006F5B'
    #just_found = False
    ## End diagnostics #
    for offset in range(0,2):
        #query_start = (beginning_of_day(slot_start) + (offset)*timedelta(days = 1)).astimezone(pgh)
        query_start = (beginning_of_day(slot_start) + (offset)*timedelta(days = 1)).astimezone(pytz.utc)

        ps = []
        dts = []
        t_start_fetch = time.time()
        ps_for_whole_day = get_day_from_json_or_api(query_start,pytz.utc,cache,mute,utc_json_folder)

        # Filter down to the events in the slot #
        datetimes = []
        for p in ps_for_whole_day:
            hybrid_start = hybrid_parking_segment_start_of(p)
            p['hybrid_parking_segment_start_utc'] = (pytz.utc).localize(parser.parse(hybrid_start)) # This is being used for estimating occupancy.
            # [ ] Could parking_segment_start_utc be used instead?
            if reference_time == 'hybrid':
                datetimes.append(p['hybrid_parking_segment_start_utc'])
            elif reference_time in ['purchase_time', 'purchase_time_utc']:
                utc_reference_field, local_reference_field = time_to_field(reference_time)
                purchase_dt = (pytz.utc).localize(parser.parse(p['@PurchaseDateUtc']))
                #purchase_dt = (pytz.timezone('US/Eastern')).localize(parser.parse(p['@PurchaseDateLocal']))
                #purchase_dt = purchase_dt.astimezone(pytz.utc) #Using PurchaseDateLocal gives the same results (except
                # there could be problems associated with daylight savings time changes).
                datetimes.append(purchase_dt)

        #ps = [p for p,dt in zip(purchases,dts) if beginning_of_day(slot_start) <= dt < beginning_of_day(slot_start) + timedelta(days=1)]
        start_of_day = beginning_of_day(slot_start)
        start_of_next_day = beginning_of_day(slot_start) + timedelta(days=1)
        for purchase_i,datetime_i in zip(ps_for_whole_day,datetimes):
            ## Begin diagnostics #
            #if purchase_i['@PurchaseGuid'] == sought_key:
            #    print("FOUND IT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            #    pprint(purchase_i)
            #    print("get_payment_type(purchase_i) == {}".format(get_payment_type(purchase_i)))
            #    just_found = True
            #    print("len(ps_by_day) for each day: {}".format([(day,len(ps_by_day[day])) for day in sorted(ps_by_day.keys())]))
            #    print("offset == {}".format(offset))
            ## End diagnostics #

            if start_of_day <= datetime_i < start_of_next_day:
                if get_payment_type(purchase_i) != 'manual': # Filter out payments that are neither meter nor mobile payments.
                    ps.append(purchase_i)
                    dts.append(datetime_i)
            if get_payment_type(purchase_i) != 'manual': # Filter out payments that are neither meter nor mobile payments.
                day = datetime_i.astimezone(pytz.utc).date()
                ps_by_day[day].append(purchase_i)
                dts_by_day[day].append(datetime_i)

            ## Begin diagnostics #
            #if just_found:
            #    just_found = False
            #    print("len(ps_by_day) for each day: {}".format([(day,len(ps_by_day[day])) for day in sorted(ps_by_day.keys())]))

        #if False:
        #    for day in sorted(ps_by_day.keys()):
        #        print("Searching day == {}...".format(day))
        #        for p in ps_by_day[day]:
        #            if p['@PurchaseGuid'] == sought_key:
        #                print("{} found filed under day {}.".format(sought_key,day))
        ## End diagnostics #

            #if purchase_i['@PurchaseGuid'] == '53F693C2-4BF9-4E70-89B5-5B9532461B8C':
            #    print("FOUND IT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            #    print("start_of_day ({}) <= datetime_i ({})< start_of_next_day ({}) = {}".format(start_of_day, datetime_i, start_of_next_day, start_of_day <= datetime_i < start_of_next_day))
            #    pprint(purchase_i)

        t_end_fetch = time.time()
        if len(ps_for_whole_day) > 0:
            print("  Time required to pull day {} ({}), either from the API or from a JSON file: {} s  |  len(ps)/len(purchases) = {}".format(offset,query_start.date(),t_end_fetch-t_start_fetch,len(ps)/len(ps_for_whole_day)))

        store_data_locally = False
        if not recent:
            if reference_time == 'purchase_time_utc':
                if offset == 0:
                    store_data_locally = True
            else:
                if offset in [0,1]: # The reason that we can't just test for offset == 0
            # is because the data is stored in JSON files by UTC time and pulled that way too.
            # Only using offset == 0 misses transactions that are on the next day's UTC JSON file.
                    store_data_locally = True

        if store_data_locally:
            # Currently there's a fair amount of redundancy in here (seemingly).
            for day in sorted(ps_by_day.keys()): # Upsert purchases in batches to reduce write time.
                if day <= (slot_start + timedelta(days=1)).date(): # Here slot_start is a stand-in
                    # for DateCreatedUtc, and this serves as a check against transactions from
                    # the future (which should be impossible) getting added.
                    if reference_time == 'purchase_time_utc':
                        bulk_upsert_to_sqlite(path, ps_by_day[day], dts_by_day[day], day, reference_time)
                        mark_utc_date_as_cached(path,reference_time,day)
                    else:
                        bulk_upsert_to_sqlite_local(path, ps_by_day[day], dts_by_day[day], day, reference_time)
                        mark_date_as_cached(path,reference_time,day,offset) # <<< This is probably the problem.
                    # How do we know when a date has been truly and sufficiently cached????
                    # When the UTC JSON files for offset == 1 and then offset == 0 have
                    # BOTH been loaded and transferred into the correct SQLite database.

                    # The difficulty here is that we're not trusting the entire function get_utc_ps_for_day_from_json
                    # to do its job. Instead, we're making sure that every transaction gets put somewhere (which
                    # is correct since get_utc_ps_for_day_from_json cannot currently get transactions that are
                    # multiple days late).
                    #   Option 1: Just assume that the script will eventually be run for the entire
                    #   set of JSON files and that the SQLite databases will then be complete.
                    #   Option 2: Keep track of offsets used (in database) and incorporate that into
                    #   the check. <== This is an inelegant kluge, but seems simplest at this point.

                    #       A better eventual redesign might be to switch the JSON files back to
                    #       being based on local timestamps or to make the SQLite databases UTC
                    #       also and move the selection of a day's transactions downstream to
                    #       queries that access two SQLite databases.


                else:
                    example = ps_by_day[day][0]
                    dc = example['@DateCreatedUtc']
                    utc_reference_field, local_reference_field = time_to_field(reference_time)
                    example_ref = example[utc_reference_field]
                    example_difference = parser.parse(example_ref) - parser.parse(dc)
                    if True or not mute_alerts: # Shouldn't time-travelling transactions always result in Slack notifications?
                        msg = "Time-travelling transactions transgression: A batch of {} transactions with day == {} when slot_start.date() == {}. Example: @DateCreatedUtc = {}, @PurchaseDateUtc = {}, difference = {}. Full example transaction: {}".format(len(ps_by_day[day]), day, slot_start.date(), dc, example_ref, example_difference,example)
                        global_warnings[msg] += 1
                        print(msg)
                    dt_fields = ['@PurchaseDateLocal', '@EndDateLocal', '@EndDateUtc', '@PayIntervalEndLocal',
                            '@PayIntervalEndUtc', '@PayIntervalStartLocal', '@PayIntervalStartUtc',
                            '@PurchaseDateLocal', '@PurchaseDateUtc', '@StartDateLocal', '@StartDateUtc']
                    # If the time difference is exactly one year, just correct the year.
                    # Example:
                        # '@DateCreatedUtc': '2017-11-20T21:22:58.407'
                        # '@PurchaseDateLocal': '2018-11-19T16:22:26',
                        #{'@Amount': '9',
                        # '@DateCreatedUtc': '2017-11-20T21:22:58.407',
                        # '@EndDateLocal': '2018-11-19T20:52:14',
                        # '@EndDateUtc': '2018-11-20T01:52:14',
                        # '@PayIntervalEndLocal': '2018-11-19T20:52:14',
                        # '@PayIntervalEndUtc': '2018-11-20T01:52:14',
                        # '@PayIntervalStartLocal': '2018-11-19T16:22:14',
                        # '@PayIntervalStartUtc': '2018-11-19T21:22:14',
                        # '@PaymentServiceType': 'PrePay Code',
                        # '@PurchaseDateLocal': '2018-11-19T16:22:26',
                        # '@PurchaseDateUtc': '2018-11-19T21:22:26',
                        # '@PurchaseGuid': 'AB02FF4B-21DD-E139-4680-22F6D0D778D5',
                        # '@StartDateLocal': '2018-11-19T16:22:14',
                        # '@StartDateUtc': '2018-11-19T21:22:14',
                        # '@TerminalGuid': '8C1E8FEB-8E35-48B3-AE37-CB9DF42FB8CD',
                        # '@TerminalID': '328548-IVYBEL0004',
                        # '@Units': '270',
                        # 'PurchasePayUnit': {'@Amount': '9', '@PayUnitName': 'Card'},
                        # 'hybrid_parking_segment_start_utc': datetime.datetime(2018, 11, 19, 21, 22, 14, tzinfo=<UTC>)}
                    if False:
                    #if 362 < example_difference.days < 367:
                        # Fix the years of every transaction.
                        n = -1
                        for p,dt in zip(ps_by_day[day],dts_by_day[day]):
                            for dt_field in dt_fields:
                                add_n_years(p,dt_field,n)
                            dt = dt.replace(dt.year + n)

                        print("These transactions have been fixed by adding {} years to their datetime fields (other than @DateCreatedUtc).".format(n))
                        if reference_time == 'purchase_time_utc':
                            bulk_upsert_to_sqlite(path, ps_by_day[day], dts_by_day[day], day, reference_time)
                        else:
                            bulk_upsert_to_sqlite_local(path, ps_by_day[day], dts_by_day[day], day, reference_time)

                    else:
                        print("We're just not going to file these anywhere for now.")
                        #raise ValueError("Time-travelling transactions transgression")

                    print("Does it make sense to insert transactions under day = {} if slot_start = {}?".format(day,slot_start))
                    print("Here's an example transaction:")
                    pprint(example)
        ps_all += ps
        dts_all += dts


    return ps_all, dts_all

def get_utc_ps_for_day_from_json(slot_start,local_tz=pytz.timezone('US/Eastern'),reference_time='purchase_time_utc',cache=True,mute=False,utc_json_folder='utc_json'):
    # Solves most of the DateCreatedUtc-StartDateUtc discrepancy by
    # collecting data over two UTC days (from a function that
    # caches API query results as raw JSON files) and then filtering
    # those results down to a single UTC day.

    # Thus, the sequence is
    #       get_day_from_json_or_API: Check if the desired day is
    #       in a JSON file. If not, it fetches the data from the API.
    #       Adding hashes, culling fields, and saving local JSON
    #       files of transactions happens here.
    #
    #       get_utc_ps_for_day_from_json: Takes lots of data (filtered
    #       by DateCreatedUtc) and synthesizes it into a single day
    #       of purchases, filtered instead by StartDateUtc.
    #
    #       cache_in_memory_and_filter: Caches the most recent UTC day
    #       of purchases in memory (or else uses the existing cache)
    #       and then filters the results down to the desired slot.
    #
    #       get_parking_events: Dispatches the correct function based
    #       on recency of the slot and then by caching method.


    # As suggested by the name, this function is designed specifically
    # for the 'utc_json' caching mode.
    ###
    # Note that no matter what time of day is associated with slot_start,
    # this function will get all of the transactions for that entire day.
    # Filtering the results down to the desired time range is handled
    # elsewhere (in the calling function).
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

    #ref_field = '@StartDateUtc' # This should not be changed. # But it looks like I'm changing it.

    #for each date in day before, day of, and day after (to get all transactions)
        # [Three full days is probably slightly overkill since the most extreme
        # negative case of DateCreatedUtc - StartDateUtc observed so far has
        # been -12 hours.]

    # To avoid issues with missing or extra hours during Daylight Savings Time,
    # all slot times should be in UTC.

    # The cached_dates format is also UTC dates. (This change was made to
    # route around issues encountered when using localized versions of
    # StartDateUtc and converting to dates.)

    #pgh = pytz.timezone('US/Eastern') # This time zone no longer needs to be hard-coded
    # since get_batch_parking_for_day has been fixed to work for different time zones
    # (I think). [Actually, we do need to use the local time zone so that beginning_of_day
    # returns the correct time. This time zone is now being passed up to this function
    # using the local_tz variable.]
    slot_start = slot_start.astimezone(local_tz)

    # Time-zone-sensitivity problem: This function gives different results for
    # slot_start = 2018-09-23 04:00:00+00:00
    # and
    # slot_start = 2018-09-23 00:00:00-04:00
    # even though these are two different representations of the same time.

    print("############# slot_start = {}, beginning_of_day(slot_start) = {}, slot_start.utcoffset().total_seconds() = {}".format(slot_start,beginning_of_day(slot_start),slot_start.utcoffset().total_seconds()))

    #reference_time = 'hybrid'
    #reference_time = 'purchase_time' # Switching to PurchaseDate as a reference for
    # comparison with CALE Web Office results (even though this timestamp is
    # sometimes problematically different from StartDate).
    print("Using {} reference-time mode.".format(reference_time))

    ps_by_day = defaultdict(list)
    dts_by_day = defaultdict(list)
    ps_all = []
    dts_all = []
    recent = datetime.now(local_tz) - slot_start <= timedelta(days = 5)
    ## Begin diagnostics #
    #sought_key = 'ECA8F05C-F220-E948-9FCA-2A51BA006F5B'
    #just_found = False
    ## End diagnostics #
    for offset in range(0,2):
        #query_start = (beginning_of_day(slot_start) + (offset)*timedelta(days = 1)).astimezone(pgh)
        query_start = (beginning_of_day(slot_start) + (offset)*timedelta(days = 1)).astimezone(pytz.utc)

        ps = []
        dts = []
        t_start_fetch = time.time()
        ps_for_whole_day = get_day_from_json_or_api(query_start,pytz.utc,cache,mute,utc_json_folder)

        # Filter down to the events in the slot #
        datetimes = []
        for p in ps_for_whole_day:
            hybrid_start = hybrid_parking_segment_start_of(p)
            p['hybrid_parking_segment_start_utc'] = (pytz.utc).localize(parser.parse(hybrid_start)) # This is being used for estimating occupancy.
            # [ ] Could parking_segment_start_utc be used instead?
            if reference_time == 'hybrid':
                datetimes.append(p['hybrid_parking_segment_start_utc'])
            elif reference_time in ['purchase_time', 'purchase_time_utc']:
                utc_reference_field, local_reference_field = time_to_field(reference_time)
                purchase_dt = (pytz.utc).localize(parser.parse(p['@PurchaseDateUtc']))
                #purchase_dt = (pytz.timezone('US/Eastern')).localize(parser.parse(p['@PurchaseDateLocal']))
                #purchase_dt = purchase_dt.astimezone(pytz.utc) #Using PurchaseDateLocal gives the same results (except
                # there could be problems associated with daylight savings time changes).
                datetimes.append(purchase_dt)

        #ps = [p for p,dt in zip(purchases,dts) if beginning_of_day(slot_start) <= dt < beginning_of_day(slot_start) + timedelta(days=1)]
        start_of_day = beginning_of_day(slot_start)
        start_of_next_day = beginning_of_day(slot_start) + timedelta(days=1)
        for purchase_i,datetime_i in zip(ps_for_whole_day,datetimes):
            ## Begin diagnostics #
            #if purchase_i['@PurchaseGuid'] == sought_key:
            #    print("FOUND IT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            #    pprint(purchase_i)
            #    print("get_payment_type(purchase_i) == {}".format(get_payment_type(purchase_i)))
            #    just_found = True
            #    print("len(ps_by_day) for each day: {}".format([(day,len(ps_by_day[day])) for day in sorted(ps_by_day.keys())]))
            #    print("offset == {}".format(offset))
            ## End diagnostics #

            if start_of_day <= datetime_i < start_of_next_day:
                if get_payment_type(purchase_i) != 'manual': # Filter out payments that are neither meter nor mobile payments.
                    ps.append(purchase_i)
                    dts.append(datetime_i)
            if get_payment_type(purchase_i) != 'manual': # Filter out payments that are neither meter nor mobile payments.
                day = datetime_i.astimezone(local_tz).date()
                ps_by_day[day].append(purchase_i)
                dts_by_day[day].append(datetime_i)

            ## Begin diagnostics #
            #if just_found:
            #    just_found = False
            #    print("len(ps_by_day) for each day: {}".format([(day,len(ps_by_day[day])) for day in sorted(ps_by_day.keys())]))

        #if False:
        #    for day in sorted(ps_by_day.keys()):
        #        print("Searching day == {}...".format(day))
        #        for p in ps_by_day[day]:
        #            if p['@PurchaseGuid'] == sought_key:
        #                print("{} found filed under day {}.".format(sought_key,day))
        ## End diagnostics #

            #if purchase_i['@PurchaseGuid'] == '53F693C2-4BF9-4E70-89B5-5B9532461B8C':
            #    print("FOUND IT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            #    print("start_of_day ({}) <= datetime_i ({})< start_of_next_day ({}) = {}".format(start_of_day, datetime_i, start_of_next_day, start_of_day <= datetime_i < start_of_next_day))
            #    pprint(purchase_i)

        t_end_fetch = time.time()
        if len(ps_for_whole_day) > 0:
            print("  Time required to pull day {} ({}), either from the API or from a JSON file: {} s  |  len(ps)/len(purchases) = {}".format(offset,query_start.date(),t_end_fetch-t_start_fetch,len(ps)/len(ps_for_whole_day)))

        store_data_locally = False
        if not recent:
            if reference_time == 'purchase_time_utc':
                if offset == 0:
                    store_data_locally = True
            else:
                if offset in [0,1]:
                    store_data_locally = True

        if store_data_locally: # The reason that we can't just test for offset == 0
            # is because the data is stored in JSON files by UTC time and pulled that way too.
            # Only using offset == 0 misses transactions that are on the next day's UTC JSON file.

            # Currently there's a fair amount of redundancy in here (seemingly).

            for day in sorted(ps_by_day.keys()): # Upsert purchases in batches to reduce write time.
                if day <= (slot_start + timedelta(days=1)).date(): # Here slot_start is a stand-in
                    # for DateCreatedUtc, and this serves as a check against transactions from
                    # the future (which should be impossible) getting added.

                    if reference_time == 'purchase_time_utc':
                        #try:
                        #    bulk_upsert_to_sqlite(path, ps_by_day[day], dts_by_day[day], day, reference_time)
                        #except:
                        bulk_upsert_to_sqlite(path, ps_by_day[day], dts_by_day[day], day, reference_time)
                        mark_utc_date_as_cached(path,reference_time,day)
                    else:
                        bulk_upsert_to_sqlite_local(path, ps_by_day[day], dts_by_day[day], day, reference_time)
                        mark_date_as_cached(path,reference_time,day,offset) # <<< This is probably the problem.
                    # How do we know when a date has been truly and sufficiently cached????
                    # When the UTC JSON files for offset == 1 and then offset == 0 have
                    # BOTH been loaded and transferred into the correct SQLite database.

                    # The difficulty here is that we're not trusting the entire function get_utc_ps_for_day_from_json
                    # to do its job. Instead, we're making sure that every transaction gets put somewhere (which
                    # is correct since get_utc_ps_for_day_from_json cannot currently get transactions that are
                    # multiple days late).
                    #   Option 1: Just assume that the script will eventually be run for the entire
                    #   set of JSON files and that the SQLite databases will then be complete.
                    #   Option 2: Keep track of offsets used (in database) and incorporate that into
                    #   the check. <== This is an inelegant kluge, but seems simplest at this point.

                    #       A better eventual redesign might be to switch the JSON files back to
                    #       being based on local timestamps or to make the SQLite databases UTC
                    #       also and move the selection of a day's transactions downstream to
                    #       queries that access two SQLite databases.


                else:
                    mute_alerts = False
                    example = ps_by_day[day][0]
                    dc = example['@DateCreatedUtc']
                    utc_reference_field, local_reference_field = time_to_field(reference_time)
                    example_ref = example[utc_reference_field]
                    example_difference = parser.parse(example_ref) - parser.parse(dc)
                    if True or not mute_alerts: # Always send these alerts, but packge them into global warnings.
                        msg = "Time-travelling transactions transgression: A batch of {} transactions with day == {} when slot_start.date() == {}. Example: @DateCreatedUtc = {}, @PurchaseDateUtc = {}, difference = {}. Full example transaction: {}".format(len(ps_by_day[day]), day, slot_start.date(), dc, example_ref, example_difference,example)
                        global_warnings[msg] += 1
                        print(msg)
                    dt_fields = ['@PurchaseDateLocal', '@EndDateLocal', '@EndDateUtc', '@PayIntervalEndLocal',
                            '@PayIntervalEndUtc', '@PayIntervalStartLocal', '@PayIntervalStartUtc',
                            '@PurchaseDateLocal', '@PurchaseDateUtc', '@StartDateLocal', '@StartDateUtc']
                    # If the time difference is exactly one year, just correct the year.
                    # Example:
                        # '@DateCreatedUtc': '2017-11-20T21:22:58.407'
                        # '@PurchaseDateLocal': '2018-11-19T16:22:26',
                        #{'@Amount': '9',
                        # '@DateCreatedUtc': '2017-11-20T21:22:58.407',
                        # '@EndDateLocal': '2018-11-19T20:52:14',
                        # '@EndDateUtc': '2018-11-20T01:52:14',
                        # '@PayIntervalEndLocal': '2018-11-19T20:52:14',
                        # '@PayIntervalEndUtc': '2018-11-20T01:52:14',
                        # '@PayIntervalStartLocal': '2018-11-19T16:22:14',
                        # '@PayIntervalStartUtc': '2018-11-19T21:22:14',
                        # '@PaymentServiceType': 'PrePay Code',
                        # '@PurchaseDateLocal': '2018-11-19T16:22:26',
                        # '@PurchaseDateUtc': '2018-11-19T21:22:26',
                        # '@PurchaseGuid': 'AB02FF4B-21DD-E139-4680-22F6D0D778D5',
                        # '@StartDateLocal': '2018-11-19T16:22:14',
                        # '@StartDateUtc': '2018-11-19T21:22:14',
                        # '@TerminalGuid': '8C1E8FEB-8E35-48B3-AE37-CB9DF42FB8CD',
                        # '@TerminalID': '328548-IVYBEL0004',
                        # '@Units': '270',
                        # 'PurchasePayUnit': {'@Amount': '9', '@PayUnitName': 'Card'},
                        # 'hybrid_parking_segment_start_utc': datetime.datetime(2018, 11, 19, 21, 22, 14, tzinfo=<UTC>)}
                    if False:
                    #if 362 < example_difference.days < 367:
                        # Fix the years of every transaction.
                        n = -1
                        for p,dt in zip(ps_by_day[day],dts_by_day[day]):
                            for dt_field in dt_fields:
                                add_n_years(p,dt_field,n)
                            dt = dt.replace(dt.year + n)

                        print("These transactions have been fixed by adding {} years to their datetime fields (other than @DateCreatedUtc).".format(n))
                        if reference_time == 'purchase_time_utc':
                            bulk_upsert_to_sqlite(path, ps_by_day[day], dts_by_day[day], day, reference_time)
                        else:
                            bulk_upsert_to_sqlite_local(path, ps_by_day[day], dts_by_day[day], day, reference_time)

                    else:
                        print("We're just not going to file these anywhere for now.")
                        #raise ValueError("Time-travelling transactions transgression")

                    print("Does it make sense to insert transactions under day = {} if slot_start = {}?".format(day,slot_start))
                    print("Here's an example transaction:")
                    pprint(example)
        ps_all += ps
        dts_all += dts


    return ps_all, dts_all

def get_ps_for_utc_day(dt_start_i,reference_time,cache,mute,caching_mode,utc_json_folder='utc_json'):
    """If possible, pull events from the sqlite cache.

    NOTE: If the data is already cached in a SQLite database, this
    returns transactions from UTC midnight to UTC midnight.
    Otherwise it fetches and returns transactions from UTC midnight to UTC midnight."""

    # dt_start_i comes in as a UTC datetime.
    # and is_date_cached wants a UTC date.
    utc_date = dt_start_i.astimezone(pytz.utc).date()

    if caching_mode not in ['none'] and is_utc_date_cached(path,reference_time,utc_date): # First check
        # whether caching_mode == 'none' to avoid searching for non-existent cached_dates.db file.
        ps_for_whole_day, dts_for_whole_day = get_events_from_sqlite(path,utc_date,reference_time)
    else:
        ps_for_whole_day, dts_for_whole_day = get_utc_ps_for_utc_day_from_json(dt_start_i,reference_time,cache,mute,utc_json_folder)
    return ps_for_whole_day, dts_for_whole_day

def get_ps_for_day_local(dt_start_i,local_tz,reference_time,cache,mute,utc_json_folder='utc_json'):
    """If possible, pull events from the sqlite cache.

    NOTE: If the data is already cached in a SQLite database, this
    returns transactions from LOCAL midnight to local midnight.
    Otherwise it returns transactions from UTC midnight to UTC midnight."""

    # dt_start_i comes in as a UTC datetime.
    # But is_date_cached wants a LOCAL DATE.
    local_date = dt_start_i.astimezone(local_tz).date()

    if is_date_cached(path,reference_time,local_date):
        ps_for_whole_day, dts_for_whole_day = get_events_from_sqlite(path,local_date,reference_time)
    else:
        ps_for_whole_day, dts_for_whole_day = get_utc_ps_for_day_from_json(dt_start_i,local_tz,reference_time,cache,mute,utc_json_folder)
    return ps_for_whole_day, dts_for_whole_day

def cache_in_memory_and_filter(db,slot_start,slot_end,local_tz,cache,mute=False,caching_mode='utc_sqlite',utc_json_folder='utc_json'):
    # Basically, this function gets all the parking events between slot_start and start_end (using time_field)
    # to choose the field to filter on, and maintains an in-memory global cache of all parking events for the
    # entire day corresponding to the last date called. Thus, when slot_start moves from January 1st to
    # January 2nd, the old cache of purchases is dumped, and all of the events for the 2nd (in UTC time)
    # are fetched and used for subsequent queries until slot_start advances to January 3rd.

    ###
    # This function handles the situation where slot_start and slot_end are on different days
    # by calling get_ps_for_day in a loop.

    # When reference_time == 'hybrid', the function "hybrid_parking_segment_start_of" determines
    # the timestamp used for calculating the datetime values used to filter purchases
    # down to those between slot_start and start_end. When reference_time == 'purchase_time',
    # @PurchaseDateUtc is used instead. (reference_time is set in this function.)

    #reference_time = 'purchase_time' # This is a local-time reference.
    reference_time = 'purchase_time_utc' # This is a UTC-time reference.

    # Note that the time zone tz and the field produced by hybrid_parking_segment_start_of must be consistent
    # for this to work properly.
    #tz = pytz.utc
    # The old sanity check looked like this:
        #if (re.search('Utc',time_field) is not None) != (tz == pytz.utc): # This does an XOR
                                                                           # between these values.
        #    raise RuntimeError("It looks like the time_field may not be consistent with the provided time zone")
    # At present, it's not so easy to verify that hybrid_parking_segment_start_of() returns a UTC time.

    global last_utc_date_cache, utc_ps_cache, utc_dts_cache
    if last_utc_date_cache != slot_start.date():
        # Given that reference_time now encodes for UTC vs non-UTC, it's possible for the
        # reference_time to directly conflict with the caching mode.
        if reference_time == 'purchase_time_utc':
            assert caching_mode != 'sqlite'
        if reference_time == 'purchase_time':
            assert caching_mode != 'utc_sqlite'
        if not mute:
            print("last_utc_date_cache ({}) doesn't match slot_start.date() ({})".format(last_utc_date_cache, slot_start.date()))

        ps_all = []
        dts_all = []
        dt_start_i = slot_start
        while dt_start_i.date() <= slot_end.date():
            if caching_mode == 'utc_json':
                ps_for_whole_day, dts_for_whole_day = get_utc_ps_for_day_from_json(dt_start_i,local_tz,reference_time,cache,mute,utc_json_folder)
            elif caching_mode in ['utc_sqlite', 'none']:
                ps_for_whole_day, dts_for_whole_day = get_ps_for_utc_day(dt_start_i,reference_time,cache,mute,caching_mode,utc_json_folder)
            elif caching_mode == 'sqlite':
                ps_for_whole_day, dts_for_whole_day = get_ps_for_day_local(dt_start_i,local_tz,reference_time,cache,mute,utc_json_folder)
                # The reason it's OK to use get_ps_for_day_local (probably) is because the filtering down to
                # the required time range is done at the bottom of this function.
            elif caching_mode == 'db_caching':
                ps_for_whole_day = db_get_ps_for_day(db,dt_start_i,cache,mute)
            else:
                raise ValueError("Behavior for caching_mode = {} is undefined".format(caching_mode))
            ps_all += ps_for_whole_day
            dts_all += dts_for_whole_day
            dt_start_i += timedelta(days = 1)
            if not mute:
                print("Now there are {} transactions in ps_all".format(len(ps_all)))

        utc_ps_cache = ps_all # Note that if slot_start and slot_end are not on the same day,
        # utc_ps_cache will hold transactions for more than just the date of slot_start, but
        # since filtering is done further down in this function, this should not represent a
        # problem. There should be no situations where more than two days of transactions will
        # wind up in this cache at any one time.
        #utc_dts_cache = [tz.localize(datetime.strptime(p[time_field],dt_format)) for p in ps_all] # This may break for StartDateUtc!!!!!

        #utc_dts_cache = [tz.localize(parser.parse(hybrid_parking_segment_start_of(p))) for p in ps_all] # This may break for StartDateUtc!!!!!
        utc_dts_cache = dts_all
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
        #    pprint(p)

        # Search for particular transactions and print them.
        #if re.search('404',p['@TerminalID']) is not None:
        #    if p['@Units'] == '45':
        #        sdu = (pytz.utc).localize(datetime.strptime(p[time_field],dt_format))
        #        sdl = sdu.astimezone(pgh)
        #        if pgh.localize(datetime(2013,10,1,17,30)) <= sdl < pgh.localize(datetime(2013,10,1,17,40)):
        #            pprint(p)
    last_utc_date_cache = slot_start.date()
    return ps

def get_parking_events(db,slot_start,slot_end,local_tz,cache=False,mute=False,caching_mode='utc_sqlite',utc_json_folder="utc_json"):
    # slot_start and slot_end must have time zones so that they
    # can be correctly converted into UTC times for interfacing
    # with the /Cah LAY/ API.

    pgh = pytz.timezone('US/Eastern')
    #if datetime.now(pgh) - slot_end <= timedelta(hours = 24):
        # This is too large of a margin, to be on the safe side.
        # I have not yet found the exact edge.
    #recent = datetime.now(pgh) - slot_end <= timedelta(days = 5)
    # This definition of the 'recent' variable doesn't handle well situations where we
    # want to pull 12 hours of data (as in the warming-up/seeding scenario) and that
    # 12-hour block happens to cross over the boundary between recent and non-recent.

    # Actually, those cases will be handled fine, since the real limit of the LiveData
    # export is like 6 hours and 19 or 20 hours, but to be able to handle slots of
    # arbitrary size, I am changing the definition to use slot_start to decide recency:
    recent = datetime.now(pgh) - slot_start <= timedelta(days = 5) # <<<<<<<< Why
    # does this line use a locally hard-coded value of the local timezone,
    # while calls below use the value passed in local_tz?

    if caching_mode in ['none']:
        # Override cache variable, setting it to False in the cache_in_memory_and_filter call.
        return cache_in_memory_and_filter(db,slot_start,slot_end,local_tz,False,mute,caching_mode,utc_json_folder) # Here cache=False.
    elif caching_mode in ['utc_json', 'sqlite', 'utc_sqlite'] or recent:
        #cache = cache and (not recent) # Don't cache (as JSON files) data from the "Live"
        # (recent transactions) API.
        return cache_in_memory_and_filter(db,slot_start,slot_end,local_tz,cache,mute,caching_mode,utc_json_folder)
    else: # Currently utc_json mode is considered the default and the get_batch_parking
        # functions are considered to be deprecated.
        #elif caching_mode == 'local_json': # The original approach
        return get_batch_parking(slot_start,slot_end,cache,mute,utc_json_folder,pytz.utc,time_field = '@StartDateUtc')
        #return get_batch_parking(slot_start,slot_end,cache,mute,utc_json_folder,pytz.utc,time_field = '@PurchaseDateUtc')
        #return get_batch_parking(slot_start,slot_end,cache,mute,utc_json_folder,pytz.utc,time_field = '@DateCreatedUtc',dt_format='%Y-%m-%dT%H:%M:%S.%f')

def package_for_output(stats_rows,zonelist,inferred_occupancy, zone_info,tz,slot_start,slot_end,space_aggregate_by,time_aggregate_by,split_by_mode,transactions_only):
    # This function works for zones and sampling zones. It has now been modified
    # to do basic aggregating by meter, ignoring inferred occupancy and augmentation.

    # In some cases, the output of package_for_output can look very much like list(stats_rows.values())
    # without the sorting by zone name. One thing that package_for_output adds is ensuring that all
    # zones in zonelist are represented in augmented output (so zones with zero occupancy still
    # get rows).


    # Convert Durations (list of integers) and Payments to their final forms.
    # (Durations becomes a JSON dict and Payments becomes rounded to the nearest
    # cent.) [moved from bottom of distill_stats]
    augment = not transactions_only

    for aggregation_key in stats_rows.keys():
        if not transactions_only:
            counted = Counter(stats_rows[aggregation_key]['Durations']) # For a durations of [60], counted looks like this Counter({60: 1})
            stats_rows[aggregation_key]['durations'] = json.dumps(counted, sort_keys=True)
            # However, json.dumps converts the integer keys to strings. JavaScript and JSON do not support integer strings.
            # Thus, at present, our published JSON durations fields look like '{"60": 1}', necessitating a
            # conversion of the keys back to integers. Also, sorting the keys sorts them alphabetically,
            # not numerically.
        if not split_by_mode:
            stats_rows[aggregation_key]['payments'] = float(round_to_cent(stats_rows[aggregation_key]['Payments']))
        else:
            stats_rows[aggregation_key]['mobile_payments'] = float(round_to_cent(stats_rows[aggregation_key]['Mobile Payments']))
            stats_rows[aggregation_key]['meter_payments'] = float(round_to_cent(stats_rows[aggregation_key]['Meter Payments']))
    #####

    #print("space_aggregate_by = {}, time_aggregate_by = {}, len(stats_rows) = {}".format(space_aggregate_by,time_aggregate_by,len(stats_rows)))
    if space_aggregate_by == 'meter':
        list_of_dicts = []
        augmented = []

        #mlist = sorted(list(set(stats_rows.keys()))) # This would be the list of Meter GUIDs
        #mlist = sorted(list(set([u['Meter GUID'] for u in stats_rows.values()]))) # Meter GUIDs
        # For meter-month-hour aggregation, we want to sort by year, month, hour, and then
        # meter ID. By construction, package_for_output should only be called for the same
        # year and month values, so we sort by hour (maybe sorting by local hour and then
        # using UTC hour as the tiebreaker) and then by meter ID.
        #year_month = slot_start.astimezone(tz).strftime("%Y/%m")
        #year = slot_start.astimezone(tz).strftime("%Y")
        #month = slot_start.astimezone(tz).strftime("%m")
        #for hour in range(0,24):
        #    for meter in mlist:
        #        a_key = "{}|{} {:02d}".format(meter,year_month,hour)
        #        a_key = format_a_key(meter,year,month,hour)
        #        if a_key in stats_rows:
        #            print("{} is in stats_rows".format(a_key))
        #            list_of_dicts.append(stats_rows[a_key])
            #raise ValueError("Fill in the rest of this code and get rid of stats_rows[meter].")


        # This approach works, but it would be nicer to sort by meter ID, though using meter GUID
        # as the key seems like it might be slightly more robust (though it's actually unclear
        # how best to handle situations where the meter ID shifts).

        # Perhaps that could be done this way:
        # better_order = sorted(stats_rows, key=lambda x: (x['Year'], x['Month'], x['Hour'], x['Meter ID']))
        for a_key in sorted(list(stats_rows.keys())):
            dt_string, meter_guid = a_key.split('|')
            year_month, hour_string = dt_string.split(' ')
            list_of_dicts.append(stats_rows[a_key])

    else: #if space_aggregate_by == 'zone' and time_aggregate_by is None: # The expectation is that spacetime == 'zone' here.
        list_of_dicts = []
        augmented = []
        # Eventually zlist should be formed like mlist is being formed, in case other kinds of
        # spacetime aggregation are used::
        #       mlist = sorted(list(set([u['Meter ID'] for u in stats_rows.values()]))) # Meter IDs
        zlist = sorted(list(set(sorted(stats_rows.keys())+zonelist))) # I think that the inner "sorted" function can be removed here.
        sorted_zonelist = sorted(zonelist)

        for zone in zlist:
            if zone in stats_rows.keys(): # For spacetime == 'zone', the aggregation keys are still zone values for now.
                d = stats_rows[zone]
            elif augment: # This part is only necessary for the augmented list (which should
            # have inferred occupancy for each zone for each slot (even if there were
            # no transactions during that slot), unlike list_of_dicts).
                d = initialize_zone_stats(slot_start,slot_end,space_aggregate_by,time_aggregate_by,split_by_mode,tz,transactions_only=True)
            #d['zone'] = zone # This is no longer necessary.
            # Elaboration: That line seemed to be unnecessary when aggregation keys were invented
            # as they explicitly put the zone (or whatever the spatial aggregation was) in the key,
            # which shows up in this function in stats_rows. However, it is necessary to specify
            # the zone (or whatever) when generating rows for the augmented file
            # and when a zone has no transactions but still has some residual occupancy to report.
            # Therefore:
            if augment and ('zone' not in d.keys()) and inferred_occupancy is not None:
                d['zone'] = zone
            if zone in stats_rows.keys():
                list_of_dicts.append(copy(d))
            # Note that augmented mode has not been generalized to handle different kinds of spatial
            # aggregation. Thus:
            if augment and space_aggregate_by in ['meter']:
                raise ValueError("Augmented mode has not been generalized to work with aggregating by {}".format(space_aggregate_by))
            if augment and inferred_occupancy is not None:
                d['inferred_occupancy'] = inferred_occupancy[slot_start][zone]
            if augment and zone in zone_info.keys(): # This was originally just "if zone in temp_zone_info",
            # so I was deliberately adding these parameters to all rows (even when not computing
            # augmented statistics). Probably this was being done to allow centroids to be
            # calculated, but for now, I am eliminating such additions.
                base = zone_info[zone]
                #if zone in temp_zone_info.keys():
                #    extra = temp_zone_info[zone]
                #    if 'Latitude' in extra:
                #        d['Latitude'] = extra['Latitude']
                #    else:
                #        print("No latitude found for {}".format(zone))
                #    if 'Longitude' in extra:
                #        d['Longitude'] = extra['Longitude']
                d['space_count'] = base['spaces']
                d['zone_type'] = base['type']

            if augment and zone in zonelist: # By adding the "zone in zonelist" condition, I'm boxing
            #out the "zone in stats_rows.keys()" condition below, that was letting in things like
            # zone = CMU Study.
                if 'inferred_occupancy' not in d:
                    print("zone = {}, d = ".format(zone))
                    pprint(d)
                # Below is the line that would need to be changed to output to the
                # augmented file rows where the inferred occupancy is zero.
                #   To generate the data to send to Carto for a live (or quasi-live)
                #   map, the package_for_output function could be called just at the
                #   end of processing with augment=True.
                if d['inferred_occupancy'] > 0 or zone in stats_rows.keys(): # stats_rows.keys() cannot be simply replaced with zlist.
                # Rather, it must be just the set of zones extracted from the stats_rows field 'zone'.
                    augmented.append(d)

    #        elif zone in stats_rows.keys(): # Allentown is missing, but since all those terminals
            # are listed as inactive, this branch should never get called
            # unless someone (maybe the ParkMobile user entering a code)
            # makes an error.
    #            print("Found a zone not listed in zone_info: {}".format(zone))
    return list_of_dicts, augmented

def eliminate_zeros(ps):
    # ParkMobile accepts $0 transactions out of hours (e.g., payments on Sundays, when
    # parking is free). This function filters them out, so they are not considered in
    # the analysis.
    # return ps
    return [p for p in ps if float(p['@Amount']) != 0.0]

def resource_name(spacetime):
    if spacetime == 'zone':
        return 'Transactions by Zone and Time of Day'
    elif spacetime in ['zone,month', 'month']:
        return 'Transactions by Zone, Month, and Time of Day'
    elif spacetime == 'meter,month':
        return 'Transactions by Meter, Month, and Time of Day'
    elif spacetime == 'meter':
        return 'Transactions by Meter and Time of Day'
    raise ValueError("No resource name specified for spacetime = {}".format(spacetime))

def main(*args, **kwargs):
    # This function accepts slot_start and halting_time datetimes as
    # arguments to set the time range and push_to_CKAN and output_to_csv
    # to control those output channels.
    t_begin = time.time()

    mute_alerts = kwargs.get('mute_alerts',False)
    output_to_csv = kwargs.get('output_to_csv',False)
    push_to_CKAN = kwargs.get('push_to_CKAN',True)
    server = kwargs.get('server', 'testbed') # 'sandbox'

    sampling_transactions_resource_name = 'Transactions by Sampling Zone and Time of Day'
    occupancy_resource_name = 'Transactions and Durations by Zone and Time of Day'

        # [ ] augment and update_live_map are a little entangled now since update_live_map = True
        # is assuming that augment = True, but there's nothing forcing that parameter to be
        # set when calling process_data.main(). Thus, some refactoring is in order. These are also
        # entangled with raw_only.
        # raw_only means no calculated parameters should be output.
        # augment means that, in addition to calculated parameters, a separate augmented file
        # with estimated occupancies and the kitchen sink is output.
        # augmented mode is very compatible with updating the live map since both involve tracking
        # inferred occupancies.

    default_filename = 'transactions-1.csv'
    filename = kwargs.get('filename',default_filename)
    overwrite = kwargs.get('overwrite',False)

    verbose = False
    turbo_mode = kwargs.get('turbo_mode',False)
    # When turbo_mode is true, skip time-consuming stuff,
    # like correct calculation of durations.
    raw_only = kwargs.get('raw_only', False)
    # When raw_only is True, calculated columns like Durations
    # and car_minutes should contain null values.
    if raw_only:
        turbo_mode = True
    skip_processing = kwargs.get('skip_processing',False)
    caching_mode = kwargs.get('caching_mode','utc_sqlite')
    utc_json_folder = kwargs.get('utc_json_folder','utc_json')

    threshold_for_uploading = kwargs.get('threshold_for_uploading',1000) # The
    # minimum length of the list of dicts that triggers uploading to CKAN.
    update_live_map = kwargs.get('update_live_map',False)

    zone_info = get_zone_info(server)

    if caching_mode == 'db_caching':
        db_filename = kwargs.get('db_filename','transactions_cache.db') # This can be
        # changed with a passed parameter to substitute a test database
        # for running controlled tests with small numbers of events.
        db = create_or_connect_to_db(db_filename)
    else:
        db = None

    zone_kind = 'new' # 'old' maps to enforcement zones
    # (specifically corrected_zone_name). 'new' maps to numbered reporting
    # zones.
    if zone_kind == 'old':
        zonelist = lot_list + other_zones_list # Note that other_zones_list (previously
        # called "pure_zones_list") was modified in January 2019, being turned into
        # more of an exclude-from-mini-zones list, eroding support for
        # zone_kind = 'old', which could probably be removed at this point.
    else:
        zonelist = numbered_reporting_zones_list

    timechunk = kwargs.get('timechunk',DEFAULT_TIMECHUNK)
  #  timechunk = timedelta(seconds=1)
    #######
    # space_aggregate_by is a parameter used to tell distill_stats how to spatially aggregate
    # data (by zone, sampling zone, or meter GUID). We need a different parameter to choose
    # among spatiotemporal aggregations, including:
    # 1) default: by 10-minute interval and zone/sampling zone (TIMECHUNK = 10 minutes)
    # 2) alternative: by 1-hour intervals and meter, but also summed over every day in a
    # month (the timechunk will be separately controlled by the 'timechunk' parameter).

    # spacetime = 'zone' for case 1 and 'meter,month' for case 2
    spacetime = kwargs.get('spacetime','zone') # This is the spatiotemporal aggregation mode.
    split_by_mode = True # If this is True,split transactions
    # into meter transactions and mobile transactions. If not, don't.
    if spacetime == 'zone':
        space_aggregation = 'zone'
        time_aggregation = None # So, there are really two different time aggregations going
        # on. Fine-grained aggregation into bins of duration timechunk (10 minutes) by default
        # and then this extra optional time_aggregation, which is by month for 'meter,month'
        # aggregation (where timechunk is switched to one hour) or else none at all in the
        # default case.
    elif spacetime == 'meter,month':
        space_aggregation = 'meter'
        time_aggregation = 'month'
        timechunk = timedelta(hours=1)

    pgh = pytz.timezone('US/Eastern')
    use_cache = kwargs.get('use_cache', False)
    terminals = get_terminals(use_cache)

    t_ids = [t['@Id'] for t in terminals]
    t_guids = [t['@Guid'] for t in terminals]


    if skip_processing:
        timechunk = timedelta(hours=24)

    # It is recommended that all work be done in UTC time and that the
    # conversion to a local time zone only happen at the end, when
    # presenting something to humans.
    slot_start = pgh.localize(datetime(2012,7,23,0,0)) # The actual earliest available data.
    slot_start = pgh.localize(datetime(2017,4,15,0,0))
    slot_start = kwargs.get('slot_start',slot_start)

########
    halting_time = slot_start + timedelta(hours=24)

    halting_time = pgh.localize(datetime(3030,4,13,0,0)) # Set halting time
    # to the far future so that the script runs all the way up to the most
    # recent data (based on the slot_start < now check in the loop below).
    #halting_time = pgh.localize(datetime(2017,3,2,0,0)) # Set halting time
    halting_time = kwargs.get('halting_time',halting_time)

    if time_aggregation == 'month':
        # Round to the beginning and end of the months of the respective starting and ending datetimes:
        slot_start = slot_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        next_month = (halting_time.month + 1 - 1) % 12 + 1
        #next_month -=1
        halting_time = halting_time.replace(month=next_month, day=1, hour=0, minute=0, second=0, microsecond=0)
        print("halting_time = {}".format(halting_time))

    # Setting slot_start and halting_time to UTC has no effect on
    # getting_ps_from_somewhere, but totally screws up get_batch_parking
    # (resulting in zero transactions after 20:00 (midnight UTC).
    if caching_mode == 'db_caching':
        slot_start = slot_start.astimezone(pytz.utc)
        halting_time = halting_time.astimezone(pytz.utc)

    slot_start = slot_start.astimezone(pytz.utc) # Changed for utc_sqlite
    halting_time = halting_time.astimezone(pytz.utc)
    # This is not related to the resetting of session_dict, since extending
    # session_dict by adding on previous_session_dict did not change the fact that
    # casting slot_start and halting_time to UTC caused all transactions
    # after 20:00 ET to not appear in the output.

    # Therefore, (until the real reason is uncovered), slot_start and halting_time
    # will only be converted to UTC when using database caching.

    try:
        sampling_zones, parent_zones, uncharted_numbered_zones, uncharted_enforcement_zones, group_lookup_addendum = pull_terminals(use_cache=use_cache, mute_alerts=mute_alerts, return_extra_zones=True)
    except KeyError: # Address occasional glitches in looking up Terminals or CustomAttributes.
        sampling_zones, parent_zones, uncharted_numbered_zones, uncharted_enforcement_zones, group_lookup_addendum = pull_terminals(use_cache=False, mute_alerts=mute_alerts, return_extra_zones=True)
    print("sampling zones = {}".format(sampling_zones))

    print("parent_zones = ...")
    pprint(parent_zones)


    virtual_zone_checked = []

    # There are presently two to three places to change the names
    # of fields to allow them to be pushed to CKAN (or even written
    # to a CSV file):
    # 1) util/util.py/build_keys()
    # 2) maybe in process_data.py/package_for_output()
    # 3) the Marshmallow schema
    # and if changing the field names within the script as well,
    # global (manual) search and replace can be used.

    cumulated_dicts = []
    cumulated_sampling_dicts = []
    session_dict = defaultdict(list) # hash-based sessions
    previous_session_dict = defaultdict(list)

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

    # An edge case of concern is where a parking purchase that happens at 12:05am
    # extends a previous purchase. To handle this, two dicts are maintained:
    # session_dict (for the day being worked on) and previous_session_dict
    # (which holds yesterday's contents). Both must be checked when trying to
    # link a new transaction with an existing session.

    # Using a separate seeding-mode stage considerably speeds up the warming-up
    # period (from maybe 10 minutes to closer to one or two).

    # For a high-availability (like streaming solution), try running the program
    # continuously. This would allow the recent purchase history to be stored
    # in memory, massively cutting down on the warm-up time. You would just
    # need to wrap process_data in a loop that sends the CALE API a new
    # request every 30 seconds, processes those new transactions, and update
    # the relevant output slots.


    # Occupancy calculations are currently broken for long pulls (since session_dict is incomplete and all_unlinkable
    # grows to beyond the computer's memory limits).

    estimate_occupancy = False
    if not estimate_occupancy:
        print("Occupancy estimation is turned off.")
    else:
        print("Occupancy estimation (currently on) needs to be fixed.")

    seeding_mode = True
    linkable = [] # Purchases that can be sorted into hash-based sessions.
    all_unlinkable = [] # For now, we're excluding the warm_up period transactions.
    if seeding_mode:
        warm_up_period = timedelta(hours=12)
        print("slot_start - warm_up_period = {}".format(slot_start - warm_up_period))
        purchases = eliminate_zeros(get_parking_events(db,slot_start - warm_up_period,slot_start,pgh,True,False,caching_mode,utc_json_folder))

        if estimate_occupancy:
            for p in sorted(purchases, key = lambda x: x['@DateCreatedUtc']):
                if 'hash' in p:
                    session_dict[p['hash']].append(p)
                    linkable.append(p)

            for session in session_dict.values():
                fix_durations(session)

            print("len(session_dict) = {}, len(linkable) = {}, len(purchases) = {}".format(len(session_dict), len(linkable), len(purchases)))
            # If we could guarantee that all transactions would be in session_dict, we could just iterate
            # through the pre-packaged sessions.
        else:
            print("len(purchases) = {}".format(len(purchases)))

    slot_end = slot_start + timechunk
    current_day = slot_start.date()
    warmup_unlinkable_count = len(purchases) - len(linkable)
    dkeys, sampling_dkeys, occ_dkeys = build_keys(space_aggregation, time_aggregation, split_by_mode)

    # [ ] Check that primary keys are in fields for writing to CKAN. Maybe check that dkeys are valid fields.

    starting_time = copy(slot_start) # The passed parameters slot_start should actually be renamed,
    # but it appears in like 160 places across many files.
###########################################
    stats_rows = {} # This is only needed for the extra time aggregation modes.

    print("time_aggregation = {}, space_aggregation = {}, spacetime = {}, split_by_mode = {}".format(time_aggregation, space_aggregation, spacetime, split_by_mode))
    while slot_start <= datetime.now(pytz.utc) and slot_start < halting_time:
        # Get all parking events that start between slot_start and slot_end
        if slot_end > datetime.now(pytz.utc): # Clarify the true time bounds of slots that
            slot_end = datetime.now(pytz.utc) # run up against the limit of the current time.

        purchases = eliminate_zeros(get_parking_events(db,slot_start,slot_end,pgh,True,False,caching_mode,utc_json_folder))
        t1 = time.time()

        if skip_processing:
            print("{} | {} purchases".format(datetime.strftime(slot_start.astimezone(pgh),"%Y-%m-%d %H:%M:%S ET"), len(purchases)))
            print("Sleeping...")
            time.sleep(3)
        else:
            reframed_ps = []
            unlinkable = []
            linkable = []
            if not estimate_occupancy: # This is a temporary hack to keep all_unlinkable from growing without bound
                all_unlinkable = [ ] # until a proper fix for estimating occupancy can be made.

            if slot_start.date() != current_day:
                print("Moving session_dict to previous_session_dict at {}.".format(slot_start))
                current_day = slot_start.date()
                previous_session_dict = session_dict
                session_dict = defaultdict(list) # Restart the history when a new day is encountered.


            if estimate_occupancy:
                # First cluster into sessions
                for p in sorted(purchases, key = lambda x: x['@DateCreatedUtc']):
                    if 'hash' in p:                        # Keep a running history of all
                        session_dict[p['hash']].append(p)  # purchases for a given day.
                        linkable.append(p)
                    else:
                        unlinkable.append(p)

                all_unlinkable += unlinkable
                # Iterate through the new purchases and add the corrected durations where possible.
                for p in linkable:
                    session = session_dict[p['hash']] + previous_session_dict[p['hash']]
                    fix_one_duration(p,session)
                #for p in unlinkable: # Actually, I think that durations should not be added to
                #    add_duration(p)  # unlinkable (hash-free) transactions, since the value
                                      # is deceptive.
                print("{} | {} purchases, ${}, len(linkable) = {}, len(unlinkable) = {}".format(datetime.strftime(slot_start.astimezone(pgh),"%Y-%m-%d %H:%M:%S ET"),
                    len(purchases),sum([float(p['@Amount']) for p in purchases]),len(linkable), len(unlinkable)))

            for p in purchases: # This was previously "for p in linkable + unlinkable" before the estimate_occupancy hack was put in place.
                if estimate_occupancy:
                    if 'Duration' not in p or p['Duration'] is None:
                        if 'hash' in p and p['hash'] in session_dict.keys():
                            pprint(session_dict[p['hash']])
                        elif verbose:
                            print("No hash found for the transaction ")
                            pprint(p)
                        #raise ValueError("Error!")
                        # Policy change: It's not necessary to assign a Duration to a transaction (since we're breaking the old inaccuate approach
                        # to calculating durations. The Duration field is only a non-None value now when the duration of the transaction can
                        # be determined (e.g., by linking transactions and untangling true segments and durations that the parker paid for).
                reframed_ps.append(hash_reframe(p,terminals,t_guids,session_dict,previous_session_dict,uncharted_numbered_zones,uncharted_enforcement_zones,turbo_mode,raw_only,transactions_only=True))

            # Temporary for loop to check for unconsidered virtual zone codes (will not work for Flowbird App zones).
            #for rp in reframed_ps:
            #    if rp['TerminalID'][:3] == "PBP":
            #        code = rp['TerminalID'][3:]
            #        if code not in virtual_zone_checked:
            #            print("\nVerifying group code {} for a purchase at terminal {}".format(code,rp['TerminalGUID']))
            #            code_group, _, _ = group_by_code(code)
            #            print("Found group {}".format(code_group))
            #            virtual_zone_checked.append(code)


            ### BEGIN AGGREGATION/STATS-COMPILATION/OUTPUT-PREPARATION. Inputs: slot_start, slot_end,
            ### reframed_ps, stats_rows, cumulated_dicts
            ### zone_info, zonelist, terminals, t_ids, t_guids, group_lookup_addendum, zone_kind, pgh
            ### dkeys  # These could conceivably be replaced in the write_to_csv function
                       # by something that extracts the keys from the schema.
            ### server
            ### spacetime, space_aggregation, time_aggregation, turbo_mode, output_to_csv, push_to_CKAN, overwrite

            ### It would be nice to be able to package this code up and call it twice: Once for binned transactions
            ### by payment time and once for binned transactions with durations and inferrable occupancy, by true parking times.

            ### Currently (instead) there is no abstraction to bin objects and the transactions-loop code is a more
            ### complicated version of the occupancy-loop code (chiefly due to the different kinds of aggregations
            ### and support for sampling zones. The transactions-loop is simpler now that the occupancy and
            ### augmented stuff has been pulled out of it.
            if time_aggregation == 'month':
                if is_very_beginning_of_the_month(slot_start) and len(stats_rows) > 0: # Store the old stats_rows and then reset stats_rows
                    print("Found the very beginning of the month")
                    # Store old stats_rows
                    list_of_dicts, _ = package_for_output(stats_rows,zonelist,None,zone_info,pgh,slot_start,slot_end,space_aggregation,time_aggregation,split_by_mode,transactions_only=True)
                    if output_to_csv:
                        write_or_append_to_csv(filename,list_of_dicts,dkeys,overwrite)

                    if push_to_CKAN:
                        if split_by_mode:
                            schema = SplitTransactionsSchema
                        else:
                            schema = TransactionsSchema
                        primary_keys = ['zone', 'utc_start', 'start']
                        success = send_data_to_pipeline(server, SETTINGS_FILE, resource_name(spacetime), schema, list_of_dicts, primary_keys=primary_keys)
                        print("success = {}".format(success))

                    if (push_to_CKAN and success) or not push_to_CKAN:
                        stats_rows = {}
                    if (push_to_CKAN and not success) and output_to_CSV:
                        raise ValueError("stats_rows was not cleared because of failure to write to CKAN, but this would cause data to be double-written to the CSV file. No code exists to resolve this conflict, so this script is throwing its digital hands up to avoid making a mess.")

            elif time_aggregation is None:
                stats_rows = {}

            # Condense to key statistics (including duration counts).
            stats_rows = distill_stats(reframed_ps,terminals,t_guids,t_ids,group_lookup_addendum,slot_start,slot_end,stats_rows, zone_kind, space_aggregation, time_aggregation, split_by_mode, [], tz=pgh, transactions_only=True)
            # stats_rows is actually a dictionary, keyed by zone.
            if time_aggregation is None and space_aggregation == 'zone':
                sampling_stats_rows = distill_stats(reframed_ps,terminals,t_guids,t_ids,group_lookup_addendum,slot_start, slot_end,{}, zone_kind, 'sampling zone', time_aggregation, split_by_mode, parent_zones, tz=pgh, transactions_only=True)

            if spacetime == 'zone': # The original idea for these clauses was to make them all
            # like
            #       if time_aggregation == 'month'
            # or
            #       if time_aggregation is None
            # but there's a parameter in package_for_output which is sometimes 'meter' and sometimes 'zone'
            # suggesting that it should be replaced with space_aggregation, but sometimes it's 'sampling_zone'
            # because of the sampling weirdness, which I am leaving out of meter-month aggregation, so for
            # now, this clause is being governed by the value of spacetime.

                list_of_dicts, _ = package_for_output(stats_rows,zonelist,None,zone_info,pgh,slot_start,slot_end,'zone',None,split_by_mode,transactions_only=True)

                if output_to_csv and len(list_of_dicts) > 0: # Write to files as
                # often as necessary, since the associated delay is not as great as
                # for pushing data to CKAN.
                    write_or_append_to_csv(filename,list_of_dicts,dkeys,overwrite)

                cumulated_dicts += list_of_dicts
                if push_to_CKAN and len(cumulated_dicts) >= threshold_for_uploading:
                    print("len(cumulated_dicts) = {}".format(len(cumulated_dicts)))
                    if split_by_mode:
                        schema = SplitTransactionsSchema
                    else:
                        schema = TransactionsSchema
                    primary_keys = ['zone', 'utc_start', 'start']
                    success = send_data_to_pipeline(server, SETTINGS_FILE, resource_name(spacetime), schema, cumulated_dicts, primary_keys=primary_keys)
                    print("success = {}".format(success))
                    if success:
                        cumulated_dicts = []

                sampling_list_of_dicts, _ = package_for_output(sampling_stats_rows,sampling_zones,None,{},pgh,slot_start,slot_end,'sampling zone',None,split_by_mode,transactions_only=True)
                # Sending the augment parameter here as "augment and False" to prevent
                # package_for_output from even trying to generate augmented output.

                # Between the passed use_sampling_zones boolean and other parameters, more
                # information is being passed than necessary to distinguish between
                # sampling zones and regular zones.

                if output_to_csv and len(sampling_list_of_dicts) > 0:
                    write_or_append_to_csv('sampling-transactions-1.csv',sampling_list_of_dicts,sampling_dkeys,overwrite)

                cumulated_sampling_dicts += sampling_list_of_dicts
                if push_to_CKAN and len(cumulated_sampling_dicts) >= threshold_for_uploading:
                    if split_by_mode:
                        schema = SplitSamplingTransactionsSchema
                    else:
                        schema = SamplingTransactionsSchema
                    primary_keys = ['zone', 'utc_start', 'start']
                    success_a = send_data_to_pipeline(server, SETTINGS_FILE, sampling_transactions_resource_name, schema, cumulated_sampling_dicts, primary_keys=primary_keys)

                    if success_a:
                        cumulated_sampling_dicts = []
            # END if spacetime == 'zone'

        slot_start += timechunk
        slot_end = slot_start + timechunk
    if spacetime == 'zone':
        print("After the main processing loop, len(session_dict) = {}, len(cumulated_dicts) = {}, and len(cumulated_sampling_dicts) = {}".format(len(session_dict), len(cumulated_dicts), len(cumulated_sampling_dicts)))

    if caching_mode == 'db_caching':
        cached_dates,_ = get_tables_from_db(db)
        print("Currently cached dates (These are UTC dates): {}".format(list(cached_dates.all())))

    t_end = time.time()
    print("Run time = {}".format(t_end-t_begin))

    print("spacetime = {}".format(spacetime))
    if spacetime == 'meter,month' and output_to_csv:
        print("len(stats_rows) = {}".format(len(stats_rows)))
        list_of_dicts, _ = package_for_output(stats_rows,zonelist,None,zone_info,pgh,slot_start,slot_end,space_aggregation,time_aggregation,split_by_mode,transactions_only=True)
        print("len(list_of_dicts) = {}".format(len(list_of_dicts)))
        write_or_append_to_csv(filename,list_of_dicts,dkeys,overwrite)

    if push_to_CKAN: # Upload the last batch.
        if spacetime == 'zone':
            filtered_list_of_dicts = cumulated_dicts
        else:
            filtered_list_of_dicts = list_of_dicts

        if split_by_mode:
            schema = SplitTransactionsSchema
        else:
            schema = TransactionsSchema
        primary_keys = ['zone', 'utc_start', 'start']
        success = send_data_to_pipeline(server, SETTINGS_FILE, resource_name(spacetime), schema, filtered_list_of_dicts, primary_keys=primary_keys)

        if success:
            if spacetime == 'zone':
                cumulated_dicts = []
            print("Pushed the last batch of transactions to {}".format(resource_name(spacetime)))

        if spacetime == 'zone':
            if split_by_mode:
                schema = SplitSamplingTransactionsSchema
            else:
                schema = SamplingTransactionsSchema
            primary_keys = ['zone', 'utc_start', 'start']
            success_a = send_data_to_pipeline(server, SETTINGS_FILE, sampling_transactions_resource_name, schema, cumulated_sampling_dicts, primary_keys=primary_keys)
            if success_a:
                cumulated_sampling_dicts = []
                print("Pushed the last batch of sampling-zone transactions to {}".format(sampling_transactions_resource_name))
            success_transactions = success and success_a # This will be true if the last two pushes of data to CKAN are true
            # (and even if all previous pushes failed, the data should be sitting around in cumulated lists, and these last
            # two success Booleans will tell you whether the whole process succeeded).
        else:
            success_transactions = success
    else:
        success_transactions = None # The success Boolean should be defined when push_to_CKAN is false.


    if estimate_occupancy:
        ###
        if len(all_unlinkable) != 0: # This is all that would be in the target time range
            # NOT considering the warm-up period (which is tracked by warmup_unlinkable_count).
            print("Unable to compute occupancy for all transactions with complete confidence due to {} unlinkable transactions.".format(len(all_unlinkable)))
            if verbose:
                print("Let's profile the unlinkable transactions by start and end times...")
                end_times = defaultdict(int)
                for p in all_unlinkable:
                    rounded_to_slot = round_time(p['hybrid_parking_segment_start_utc'],timechunk.seconds,"down")
                    end_times[rounded_to_slot] += 1

                for et in sorted(end_times.keys()):
                    print("{}: {}".format(et, end_times[et]))



        # A different metric for whether there's enough information to save transactions as completely
        # regularized: Make sure that all Durations and Amounts make sense.

        try_to_infer_occupancies = (starting_time > (pytz.utc).localize(datetime(2018,6,19,9,0,0))) or len(all_unlinkable) == 0
        print("try_to_infer_occupancies = {}".format(try_to_infer_occupancies))
        if try_to_infer_occupancies and spacetime == 'zone': # If it's really possible to infer occupancies (and default aggregation is being used).
            # Now that all of the transactions have been given standard parking-segment start times and durations,
            # use the corrected transactions, but with the new time field to figure out how many cars
            # are parked in each zone in each slot.
            ps_by_slot = defaultdict(list)
            calculated_occupancy = defaultdict(lambda: defaultdict(int)) # Number of cars for each time slot and zone.
            for session in session_dict.values():
                # Iterate over every transaction, stick each in the appropriate time bin, and build up all the statistics and then output all at once.
                for p in session:
                    # Round chosen time field to the beginning of one of the slots.
                    if 'parking_segment_start_utc' in p:
                        rounded_to_slot = round_time(p['parking_segment_start_utc'],timechunk.seconds,"down")
                        ps_by_slot[rounded_to_slot].append(p)
                        #if starting_time <= rounded_to_slot <= halting_time: # To ensure that all relevant transactions
                        #    # have been pre-seeded before pushing a new occupancy row to CKAN. # This doesn't make any sense.
                        #    ps_by_slot[rounded_to_slot].append(p)

                        #print("{}Starting {}: {} minutes in {}".format(' '*2*p['segment_number'],p['parking_segment_start_utc'],p['Duration'],p['@TerminalID']))
                    else:
                        print("No parking_segment_start_utc found for this transaction.")

            fmt = "{:<16}   {:>18}: {:>4} {:>18}: {:>4} {:>18}: {:>4}"
            zs = ['401 - Downtown 1', '407 - Oakland 1', '425 - Bakery Sq']
            zs = ['404 - Strip Disctrict', '410 - Oakland 4', '425 - Bakery Sq']

            #for slot in sorted(ps_by_slot.keys()): # Iterating through this way only gives increases
            # in occupancy and won't produce a new row for cases where parking sessions are only ending.
            slot = copy(starting_time)
            cumulated_dicts = []
            while keep_running(slot,halting_time):
                if slot in ps_by_slot:
                    ps = ps_by_slot[slot]
                else:
                    ps = []
                # Condense to key statistics (including duration counts).
                reframed_ps = [hash_reframe(p,terminals,t_guids,session_dict,previous_session_dict,uncharted_numbered_zones,uncharted_enforcement_zones,turbo_mode,raw_only,transactions_only=False,extend=False) for p in ps]
                stats_by_zone = distill_stats(reframed_ps,terminals,t_guids,t_ids,group_lookup_addendum,slot,slot+timechunk, {}, zone_kind, space_aggregation, time_aggregation, False, [], tz=pgh, transactions_only=False) # split_by_mode is set to False here, but the transactions_only variable could be used to override a non-None value in this case.
                calculated_occupancy = update_occupancies(calculated_occupancy,stats_by_zone,slot,timechunk)
                if starting_time <= slot <= halting_time: # Only print/push to these.
                    # package_for_output barely seems necessary here.
                    list_of_dicts, augmented = package_for_output(stats_by_zone,zonelist,calculated_occupancy,zone_info,pgh,slot,slot+timechunk,'zone',None,False,transactions_only=False)
                    # split_by_mode is set to False here when calling package_for_output, but the transactions_only variable could be used to override a non-None value in this case.
                    occ = calculated_occupancy[slot]
                    print(fmt.format(datetime.strftime(slot,"%Y-%m-%d %H:%M"), zs[0], occ[zs[0]], zs[1], occ[zs[1]], zs[2], occ[zs[2]]))
                if output_to_csv and len(augmented) > 0: # Write to files as often as necessary,
                    # since the associated delay is not as great as for pushing data to CKAN.
                    write_or_append_to_csv('occupancy-1.csv',augmented,occ_dkeys,overwrite)
                if push_to_CKAN:
                    cumulated_dicts += augmented
                if push_to_CKAN and (len(cumulated_dicts) >= threshold_for_uploading or not keep_running(slot + timechunk,halting_time)):
                    schema = OccupancySchema
                    primary_keys = ['zone', 'utc_start', 'start']
                    pprint(cumulated_dicts[0])
                    success = send_data_to_pipeline(server, SETTINGS_FILE, occupancy_resource_name, schema, cumulated_dicts, primary_keys=primary_keys)
                    print("success = {}".format(success))

                    if success:
                        cumulated_dicts = []
                        print("Pushed the last batch of transactions to {}".format(occupancy_resource_name))
                slot += timechunk

            assert len(cumulated_dicts) == 0
            print("len(ps_by_slot) = {}".format(len(ps_by_slot)))
            print("len(calculated_occupancy) = {}".format(len(calculated_occupancy)))

            # Do the below but differently (find the correct calculated_occupancy entry and use it).
            #if update_live_map: # Optionally update the live map if the timing of the
            #    # current slot is correct.
            #    if slot_start <= datetime.now(pytz.utc) < slot_start+timechunk:
            #        update_map(dict(calculated_occupancy[slot_start]),zonelist,zone_info)


        print("warmup_unlinkable_count = {}, len(all_unlinkable) = {}".format(warmup_unlinkable_count,len(all_unlinkable)))


    if not estimate_occupancy:
        print("Occupancy estimation is turned off.")
    else:
        print("Occupancy estimation (currently on) needs to be fixed.")

    # Deal with accumulated warnings (rather than sending a Slack message for each one)
    global global_warnings
    if len(global_warnings) > 0:
        msg = 'process_data.py warnings: \n'
        for warning,count in global_warnings.items():
            msg += "{} ({})\n".format(warning,count)
        if not mute_alerts:
            try:
                send_to_slack(msg,username='park-shark',channel='@david',icon=':mantelpiece_clock:')
            except requests.exceptions.ConnectionError:
                print("Unable to transmit this message to Slack")
                print(msg)

    if len(config.global_terminal_ids_without_groups) > 0:
        msg = 'process_data.py warnings: \n'
        msg += "No groups found for the following codes: {}".format(', '.join(config.global_terminal_ids_without_groups))
        print(msg)
        if not mute_alerts:
            try:
                send_to_slack(msg, username='park-shark', channel='@david', icon=':mantelpiece_clock:')
            except requests.exceptions.ConnectionError:
                print("Unable to transmit this message to Slack")
                print(msg)
    return success_transactions

# Overview:
# main() calls get_parking_events to get all transactions between two times.
    #       get_parking_events: Dispatches the correct function based
    #       on recency of the slot and then by caching method.
    #
    #       cache_in_memory_and_filter: Caches the most recent UTC day
    #       of purchases in memory (or else uses the existing cache)
    #       and then filters the results down to the desired slot.
    #
    #       get_utc_ps_for_day_from_json: Takes lots of data (filtered
    #       by DateCreatedUtc) and synthesizes it into a single day
    #       of purchases, filtered instead by StartDateUtc.
    #
    #       get_day_from_json_or_API: Check if the desired day is
    #       in a JSON file. If not, it fetches the data from the API.
    #       Adding hashes, culling fields, and saving local JSON
    #       files of transactions happens here.
# The transactions are obtained for a warm-up period (to ensure
# that it's possible to attempt to join transactions together into
# sessions.

# The main function then iterates over time slots (say, 10-minute increments),
# processing those transactions, joining them into sessions, calculating
# durations, and then optionally aggregating, followed by distilling
# statistics to be published and formatting them for output.

# There's an opportunity to 1) refactor the session synthesis into a single
# function (rather than two different chunks of code for the warm-up and
# main loops) and a separate opportunity to store the session information
# to expedite future analysis (or for more rapid processing of real-time
# data).

# Output/processing modes supported: fine-grained (10-minute or 1-hour) aggregation,
# optionally coarse-grained aggregation (by month), spatial aggregations
# (by meter or by zone or by sampling zone). Also naive transactions (Parking Transactions
# (binned) by Transaction Time), regular (Parking Transactions (binned) by
# Parking Times), and augmented (to include inferred occupancy and other parameters).

if __name__ == '__main__':
    main()
