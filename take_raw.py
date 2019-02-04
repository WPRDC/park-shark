import os, re, csv, json, xmltodict
import dataset, random
from collections import OrderedDict, Counter, defaultdict
from util.util import to_dict, value_or_blank, unique_values, zone_name, is_a_lot, \
lot_code, is_virtual, get_terminals, is_timezoneless, write_or_append_to_csv, \
pull_from_url, remove_field, round_to_cent, corrected_zone_name, lot_list, \
other_zones_list, numbered_reporting_zones_list, sampling_groups, \
add_element_to_set_string, add_if_new, group_by_code, numbered_zone, censor
from fetch_terminals import pull_terminals
import requests
import zipfile
from io import BytesIO # Works only under Python 3
from copy import copy

import decimal

import time, pytz
from pprint import pprint
from datetime import datetime, timedelta
from dateutil import parser

#from util.db_util import create_or_connect_to_db, get_tables_from_db, get_ps_for_day as db_get_ps_for_day
from util.sqlite_util import get_events_from_sqlite, bulk_upsert_to_sqlite, bulk_upsert_to_sqlite_local, time_to_field, mark_date_as_cached, is_date_cached, mark_utc_date_as_cached, is_utc_date_cached
from notify import send_to_slack

from util.carto_util import update_map
from parameters.credentials_file import CALE_API_user, CALE_API_password
from parameters.local_parameters import path, SETTINGS_FILE
from pipe.pipe_to_CKAN_resource import send_data_to_pipeline, get_connection_parameters, TransactionsSchema, SplitTransactionsSchema, OccupancySchema
from pipe.gadgets import get_resource_data

from process_data import get_zone_info, eliminate_zeros, get_parking_events, is_mobile_payment, resource_name
from meters_etl.extract_master_list import main as get_lookups

from nonchalance import add_hashes


last_date_cache = None
all_day_ps_cache = []
dts_cache = []

last_utc_date_cache = None
utc_ps_cache = []
utc_dts_cache = []

def round_schoolwise(x):
    # This kind of rounding (at least for positive numbers, not yet tested for negative numbers)
    # seemed necessary to match CALE's arithmetic.

    # However, using it leaves 429 unresolved as opposed to 435 unresolved when using Python's round function.

    # [ ] It's worth checking if this rounding is also used by ParkMobile.
    if x > 0:
        rounded = int(decimal.Decimal(x).quantize(decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP))
    else:
        rounded = int(decimal.Decimal(x).quantize(decimal.Decimal('1'), rounding=decimal.ROUND_HALF_DOWN))
    return rounded

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
    for k,rp in enumerate(rps):
        t_guid = rp['TerminalGUID']
        t_id = rp['TerminalID']
        zone = None
        space_aggregation_keys = []
        aggregation_keys = []

        if space_aggregate_by == 'zone':
            if zone_kind == 'new':
                zone, _, _ = numbered_zone(t_id,None,group_lookup_addendum)
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
            space_aggregation_keys = censor(space_aggregation_keys)
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
                    if zone != 'FRIENDSHIP AVE RPP': # Exclude bogus zones
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
                                stats_by[a_key]['parent_zone'] = '|'.join(parent_zones[zone])

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

def add_purchase_type(p,row,mobile):
    purchase_type_field = 'purchase_type'
    if not mobile:
        if p['@PurchaseTypeName'] == 'Normal':
            row[purchase_type_field] = 'New'
        elif p['@PurchaseTypeName'] == 'TopUp':
            row[purchase_type_field] = 'Extension'
        else:
            print("No explicit coding for @PurchaseTypeName == {}.".format(p['@PurchaseTypeName']))
            row[purchase_type_field] = None
    else:
        #print("How are we going to deal with those pesky mobile transactions?")
        row[purchase_type_field] = None


def find_payment_type(p):
    # If the 'PurchasePayUnit' field cannot be found, use the terminal ID
    # to detect whether it's a virtual payment.
    if 'PurchasePayUnit' not in p:
        terminal_id = p['@TerminalID']
        if terminal_id[:3] == 'PBP':
            return 'mobile', 'mobile'
        elif terminal_id[0] in ['3','4']:
            return 'meter', None
        else:
            raise ValueError("Unknown terminal type for terminal ID {} from payment {}.".format(terminal_id,p))

    if type(p['PurchasePayUnit']) == list: # It's a list of Coin and Card payments.
        return 'meter', 'cash + card'
    pay_unit_name = p['PurchasePayUnit']['@PayUnitName']
    if pay_unit_name == 'Mobile Payment':
        return 'mobile', 'mobile'
    else: # In addition to "Mobile Payment" and "Coin" and "Card", there's also now "Manual", which is ignorable.
        if pay_unit_name == 'Manual':
            return 'manual', None
        elif pay_unit_name == 'Coin':
            return 'meter', 'cash'
        elif pay_unit_name == 'Card':
            return 'meter', 'card'
        elif pay_unit_name == 'None':
            return 'meter', None
        else:
            raise ValueError("Unknown payment type for @PayUnitName {} from payment {}.".format(pay_unit_name,p))

def raw_reframe(p,terminals,t_guids,group_lookup_addendum):
    """Take a dictionary and generate a new dictionary from it that samples
    the appropriate keys and renames and transforms as desired."""

    row = {}
    row['GUID'] = p['@PurchaseGuid']
    try:
        row['TerminalGUID'] = p['@TerminalGuid'] # This is useful
    # for connecting purchases with terminals when the ID changes
    # but the GUID does not change.
    except:
        print("p['@TerminalGuid'] is missing from {}".format(p))
    row['TerminalID'] = p['@TerminalID']
    if p['@TerminalGuid'] in t_guids:
        t = terminals[t_guids.index(p['@TerminalGuid'])]

    # [ ] Decide whether to make a Coin+Card purchase type or
    # to split those over two rows and require the purchase medium
    # as a primary key (in addition to Purchase GUID).

    mobile = is_mobile_payment(p)
    row['Is Mobile Payment'] = mobile
    row['payment_start_utc'] = p['@PayIntervalStartUtc']
    row['payment_end_utc'] = p['@PayIntervalEndUtc']
    row['payment_end_local'] = p['@PayIntervalEndLocal']
    row['purchase_date_utc'] = p['@PurchaseDateUtc'] if not mobile else None
    row['date_recorded_utc'] = p['@DateCreatedUtc'] # Only used as a timestamp proxy for non-mobile transactions.
    row['@DateCreatedUtc'] = p['@DateCreatedUtc']

    row['mobile_transaction_id'] = p['PurchasePayUnit']['@TransactionReference'] if (mobile and 'PurchasePayUnit' in p and '@TransactionReference' in p['PurchasePayUnit']) else None

    add_purchase_type(p,row,mobile) # Add 'purchase_type' field

    medium, payment_type = find_payment_type(p)
    row['payment_type'] = payment_type
    row['amount'] = float(p['@Amount']) # <== This amount is the total amount.
    row['cumulative_units'] = int(p['@Units'])

    #########
    row['zone'] =  numbered_zone(p['@TerminalID'],None,group_lookup_addendum)[0]
    # Payment type (cash, credit card,
    # new payment, or extension payment); [X] Done for CALE, [ ] Pending for ParkMobile
    # zone/Meter/kiosk ID; [X] meter ID [ ] Zone pending (downstream)
    # Transaction time;  [X] Done for CALE, [ ] Pending for ParkMobile
    # payment starting time; payment expiration time; [.] Payment intervals added
    # parking rates;
    # paid amount;
    # Whether the payment is adding time or for a new parker.

    return row

def build_raw_keys(space_aggregation,time_aggregation,split_by_mode):
    """Based on the types of spatial and temporal aggregation (and now whether
    transactions should be split by payment mode into mobile and meter purchases),
    synthesize and return the dictionary keys (used for writing a bunch of
    dictionaries to a CSV file."""
    # Given that these fields appear elsewhere in the process_data.py code, it might
    # be a good idea to refactor things some more so that there is one source for
    # these field names.


    if space_aggregation == 'meter':
        space_keys = ['TerminalID', 'TerminalGUID', 'zone']

    if time_aggregation is None:
        time_keys = ['payment_start_utc', 'payment_end_utc', 'purchase_date_utc', 'date_recorded_utc']
    base = ['amount', 'payment_type', 'purchase_type']

    # [ ] Should cumulative_units be included here?

    extras = ['rate', 'mobile_transaction_id']

    dkeys = space_keys + time_keys + base + extras
    return dkeys

def infer_rates(ps,original_records):
    computed_rates = defaultdict(int)
    for p,rec in zip(ps,original_records):
        if p['purchase_type'] == 'New':
            inferred_rate = p['amount']/(p['cumulative_units']/60.0)
            # Check whether rate rounds nicely.
            # Currently expected hourly rates:  [0.5,1,1.5,1.75,2,2.5,3,4] (multiples of 25 cents)
            # and then there are the oddballs: $1.50+$2hr SR
            computed_rates[inferred_rate] += 1
            if inferred_rate not in [0.5,1,1.5,1.75,2,2.5,3,4]:
                print("${}/{:.2f} = an inferred rate of ${}/hour.".format(p['amount'], p['cumulative_units']/60.0, inferred_rate))
                print(" * What's the deal with this one? * ")
                pprint(p)
                pprint(rec)
                print(" ********************************** ")
    print("\nOK, here's the distribution of computed rates:")
    pprint(computed_rates)



def main(*args, **kwargs):
    # This function accepts slot_start and halting_time datetimes as
    # arguments to set the time range and push_to_CKAN and output_to_csv
    # to control those output channels.
    t_begin = time.time()

    output_to_csv = kwargs.get('output_to_csv',False)
    push_to_CKAN = kwargs.get('push_to_CKAN',False)
    server = kwargs.get('server', 'testbed') # 'sandbox'

    default_filename = 'raw-transactions-1.csv'
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

    db = None

    threshold_for_uploading = kwargs.get('threshold_for_uploading',1000) # The
    # minimum length of the list of dicts that triggers uploading to CKAN.

    zone_info = get_zone_info(server)

    zone_kind = 'new' # 'old' maps to enforcement zones
    # (specifically corrected_zone_name). 'new' maps to numbered reporting
    # zones.
    zonelist = numbered_reporting_zones_list

    split_by_mode = True # If this is True,split transactions
    # into meter transactions and mobile transactions. If not, don't.

    pgh = pytz.timezone('US/Eastern')
    use_cache = kwargs.get('use_cache', False)
    try:
        terminals = get_terminals(use_cache)
    except requests.exceptions.ConnectionError:
        terminals = get_terminals(True)
        use_cache = True

    t_ids = [t['@Id'] for t in terminals]
    t_guids = [t['@Guid'] for t in terminals]

    # It is recommended that all work be done in UTC time and that the
    # conversion to a local time zone only happen at the end, when
    # presenting something to humans.
    slot_start = pgh.localize(datetime(2012,7,23,0,0)) # The actual earliest available data.
    slot_start = pgh.localize(datetime(2017,4,15,0,0))
    slot_start = pgh.localize(datetime.now() - timedelta(days=1))
    slot_start = pgh.localize(datetime(2018,5,21,10,0))
    slot_start = kwargs.get('slot_start',slot_start)
    timechunk = timedelta(hours=1)

########
    halting_time = slot_start + timedelta(hours=24)

    halting_time = pgh.localize(datetime(3030,4,13,0,0)) # Set halting time
    # to the far future so that the script runs all the way up to the most
    # recent data (based on the slot_start < now check in the loop below).
    #halting_time = pgh.localize(datetime(2017,3,2,0,0)) # Set halting time
    halting_time = slot_start + timedelta(hours=24)
    halting_time = kwargs.get('halting_time',halting_time)

    # Setting slot_start and halting_time to UTC has no effect on
    # getting_ps_from_somewhere, but totally screws up get_batch_parking
    # (resulting in zero transactions after 20:00 (midnight UTC).

    slot_start = slot_start.astimezone(pytz.utc) # Changed for utc_sqlite
    halting_time = halting_time.astimezone(pytz.utc)
    # This is not related to the resetting of session_dict, since extending
    # session_dict by adding on previous_session_dict did not change the fact that
    # casting slot_start and halting_time to UTC caused all transactions
    # after 20:00 ET to not appear in the output.

    # Therefore, (until the real reason is uncovered), slot_start and halting_time
    # will only be converted to UTC when using database caching.

    sampling_zones, parent_zones, uncharted_numbered_zones, uncharted_enforcement_zones, group_lookup_addendum = pull_terminals(use_cache=use_cache,return_extra_zones=True)
    print("sampling zones = {}".format(sampling_zones))

    print("parent_zones = ...")
    pprint(parent_zones)


    virtual_zone_checked = []

    # There are presently two to three places to change the names
    # of fields to allow them to be pushed to CKAN (or even written
    # to a CSV file):
    # 1) util/util.py/build_keys()
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

    rate_lookup_by_tariff, rate_lookup_by_meter = get_lookups()

    seeding_mode = True
    linkable = [] # Purchases that can be sorted into hash-based sessions.
    all_unlinkable = [] # For now, we're excluding the warm_up period transactions.
    if seeding_mode:
        warm_up_period = timedelta(hours=12)
        print("slot_start - warm_up_period = {}".format(slot_start - warm_up_period))
        purchases = eliminate_zeros(get_parking_events(db,slot_start - warm_up_period,slot_start,pgh,True,False,caching_mode))
        purchases = sorted(purchases, key = lambda x: x['@DateCreatedUtc'])
        rps = []
        for p in purchases:
            rps.append(raw_reframe(p,terminals,t_guids,group_lookup_addendum))

        # Augment raw transactions by inferring rate
        # infer_rates(rps,purchases)
        lookup_rates(rps,purchases,rate_lookup_by_tariff,rate_lookup_by_meter)

        k = 0
        #while k < len(rps) and rps[k]['purchase_type'] is None:
        #    k += 1
        k += 3
        pprint(purchases[k])
        pprint(rps[k])

        if False: #estimate_occupancy:
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
    space_aggregation = 'meter'
    time_aggregation = None
    spacetime = 'meter'
    dkeys = build_raw_keys(space_aggregation, time_aggregation, split_by_mode)

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

        purchases = eliminate_zeros(get_parking_events(db,slot_start,slot_end,pgh,True,False,caching_mode))
        t1 = time.time()

        reframed_ps = []
        unlinkable = []
        linkable = []
        if True: #not estimate_occupancy: # This is a temporary hack to keep all_unlinkable from growing without bound
            all_unlinkable = [ ] # until a proper fix for estimating occupancy can be made.

        if slot_start.date() != current_day:
            print("Moving session_dict to previous_session_dict at {}.".format(slot_start))
            current_day = slot_start.date()
            previous_session_dict = session_dict
            session_dict = defaultdict(list) # Restart the history when a new day is encountered.


        if False: #estimate_occupancy:
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
            if False: #estimate_occupancy:
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
            reframed_ps.append(raw_reframe(p,terminals,t_guids,group_lookup_addendum))

        # Temporary for loop to check for unconsidered virtual zone codes.
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


    # Sort transactions by some timestamp
        reframed_ps = sorted(reframed_ps, key = lambda x: x['@DateCreatedUtc'])
    # Add extension/non-extension boolean:
    #    PurchaseTypeName: Normal/TopUp
    #    purchase_type: new/(continuation|extension)

    # Augment raw transactions by inferring rate
        #infer_rates(reframed_ps,purchases)
        lookup_rates(rps,purchases,rate_lookup_by_tariff,rate_lookup_by_meter)

# [ ] Is 'Amount' definitely the sum of all payments in the transaction?

        list_of_dicts = reframed_ps

        if output_to_csv and len(list_of_dicts) > 0: # Write to files as
        # often as necessary, since the associated delay is not as great as
        # for pushing data to CKAN.
            write_or_append_to_csv(filename,list_of_dicts,dkeys,overwrite)

        cumulated_dicts += list_of_dicts # cumulated_dicts are collected just for writing to CKAN.
        if push_to_CKAN and len(cumulated_dicts) >= threshold_for_uploading:
            print("len(cumulated_dicts) = {}".format(len(cumulated_dicts)))
            if split_by_mode:
                schema = SplitTransactionsSchema
            else:
                schema = TransactionsSchema
            primary_keys = ['meter_id', 'utc_start', 'start']
            success = send_data_to_pipeline(server, SETTINGS_FILE, resource_name(spacetime), schema, cumulated_dicts, primary_keys=primary_keys)
            print("success = {}".format(success))
            if success:
                cumulated_dicts = []

        slot_start += timechunk
        slot_end = slot_start + timechunk

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
        primary_keys = ['TerminalID', 'zone', 'payment_start_utc', 'date_recorded_utc']
        success = send_data_to_pipeline(server, SETTINGS_FILE, resource_name(spacetime), schema, filtered_list_of_dicts, primary_keys=primary_keys)

        success_transactions = success
    else:
        success_transactions = None # The success Boolean should be defined when push_to_CKAN is false.


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
    main(output_to_csv = True)
