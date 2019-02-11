import os, sys, re, csv, json, xmltodict
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

def add_purchase_type(p,row,mobile):
    purchase_type_field = 'purchase_type'
    if not mobile:
        if p['@PurchaseTypeName'] == 'Normal':
            row[purchase_type_field] = 'New'
        elif p['@PurchaseTypeName'] == 'TopUp':
            row[purchase_type_field] = 'Extension'
        else:
            # This can be assumed to happen for those missing transactions, as they are stuck in an
            # incomplete state in the CALE system.
            if row['zone'][:3] not in ['324', '325', '410']:
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

def raw_reframe(p,terminals,t_guids,t_ids,group_lookup_addendum,include_rate):
    """Take a dictionary and generate a new dictionary from it that samples
    the appropriate keys and renames and transforms as desired."""

    row = {}
    row['GUID'] = p['@PurchaseGuid']
    row['meter_id'] = p['@TerminalID']
    t = None
    if '@TerminalGuid' in p: # This field is just not available in raw downloads from CWO.
        row['meter_guid'] = p['@TerminalGuid'] # This is useful
        # for connecting purchases with terminals when the ID changes
        # but the GUID does not change.

        if p['@TerminalGuid'] in t_guids:
            t = terminals[t_guids.index(p['@TerminalGuid'])]
    if t is None:
        if '@TerminalId' in p:
            if p['@TerminalId'] in t_ids:
                t = terminals[t_ids.index(p['@TerminalId'])]


    # [ ] Decide whether to make a Coin+Card purchase type or
    # to split those over two rows and require the purchase medium
    # as a primary key (in addition to Purchase GUID).

    mobile = is_mobile_payment(p)
    row['Is Mobile Payment'] = mobile
    row['payment_start_utc'] = p['@PayIntervalStartUtc']
    row['payment_end_utc'] = p['@PayIntervalEndUtc']
    row['payment_end_local'] = p['@PayIntervalEndLocal']
    row['purchase_date_utc'] = p['@PurchaseDateUtc'] if not mobile else None
    if '@DateCreatedUtc' not in p:
        print("Replacing missing @DateCreatedUtc field with an empty string.")
    row['date_recorded_utc'] = p['@DateCreatedUtc'] if '@DateCreatedUtc' in p else "" # Only used as a timestamp proxy for non-mobile transactions.
    row['@DateCreatedUtc'] = p['@DateCreatedUtc'] if '@DateCreatedUtc' in p else ""
    #########
    row['zone'] =  numbered_zone(p['@TerminalID'],None,group_lookup_addendum)[0]
    #########

    row['mobile_transaction_id'] = p['PurchasePayUnit']['@TransactionReference'] if (mobile and 'PurchasePayUnit' in p and '@TransactionReference' in p['PurchasePayUnit']) else None

    add_purchase_type(p,row,mobile) # Add 'purchase_type' field

    medium, payment_type = find_payment_type(p)
    row['payment_type'] = payment_type
    row['amount'] = float(p['@Amount']) # <== This amount is the total amount.
    row['cumulative_units'] = int(p['@Units'])

    row['purchase_guid'] = p['@PurchaseGuid']

    if include_rate:
        if '@TariffPackageID' in p:
            row['TariffProgram'] = p['@TariffPackageID']
        else:
            row['TariffProgram'] = None
            print("No @TariffPackageID found.")
            pprint(p)

    # Payment type (cash, credit card,
    # new payment, or extension payment); [X] Done for CALE, [ ] Pending for ParkMobile
    # zone/Meter/kiosk ID; [X] meter ID [ ] Zone pending (downstream)
    # Transaction time;  [X] Done for CALE, [ ] Pending for ParkMobile
    # payment starting time; payment expiration time; [.] Payment intervals added
    # parking rates;
    # paid amount;
    # Whether the payment is adding time or for a new parker.

    return row

def build_raw_keys(space_aggregation,time_aggregation,include_rate=False):
    """Based on the types of spatial and temporal aggregation (and now whether
    transactions should be split by payment mode into mobile and meter purchases),
    synthesize and return the dictionary keys (used for writing a bunch of
    dictionaries to a CSV file."""
    # Given that these fields appear elsewhere in the process_data.py code, it might
    # be a good idea to refactor things some more so that there is one source for
    # these field names.


    if space_aggregation == 'meter':
        space_keys = ['meter_id', 'zone']

    if time_aggregation is None:
        time_keys = ['payment_start_utc', 'payment_end_utc', 'purchase_date_utc', 'date_recorded_utc']
    base = ['amount', 'payment_type', 'purchase_type']

    # [ ] Should cumulative_units be included here?

    #extras = ['mobile_transaction_id', 'purchase_guid']
    extras = ['mobile_transaction_id']
    if include_rate:
        extras = ['rate'] + extras

    dkeys = space_keys + time_keys + base + extras
    return dkeys

def lookup_rates(ps,rate_lookup_by_tariff,rate_lookup_by_meter):
    unresolved_by_meter = defaultdict(int)
    unresolved_count = 0
    for p in ps:
        rate = None
        if 'TariffProgram' in p:
            tariff = p['TariffProgram']
            if tariff in rate_lookup_by_tariff:
                rate = rate_lookup_by_tariff[tariff]
                #print("Found rate ({}) for tariff = {}.".format(rate,tariff))

        if rate is None:
            meter_id = p['meter_id']
            if meter_id in rate_lookup_by_meter:
                rate = rate_lookup_by_meter[meter_id]
                print("Found rate ({}) for meter_id = {}.".format(rate,meter_id))

        if rate is None:
            unresolved_by_meter[meter_id] += 1
            unresolved_count += 1
            print("Failed to find rate for {}.".format(meter_id))
        else:
            p['rate'] = rate

        #if tariff == '20' and p['Is Mobile Payment']:
        #    print("For tariff == 20, a rate of {} was assigned.".format(rate))
        #    pprint(p)
        #    if '20' in rate_lookup_by_tariff:
        #        print("by tariff")
        #    elif meter_id in rate_lookup_by_meter:
        #        print("maybe by meter_id?")

        #    raise ValueError("check this out")

    print(" {} left unresolved out of {}.".format(unresolved_count,len(ps)))
    pprint(unresolved_by_meter)

def infer_rates(ps,original_records):
    computed_rates = defaultdict(int)
    rates_by_zone = defaultdict(list)
    unresolved_count = 0
    unresolved_by_meter = defaultdict(int)

    known_rates = [0.5,1,1.5,1.75,2,2.5,3,4]
    for p,rec in zip(ps,original_records):
        resolved = False
        payment_end_utc = parser.parse(p['payment_end_utc'])
        payment_start_utc = parser.parse(p['payment_start_utc'])
        assert payment_end_utc > payment_start_utc
        true_cumulative_duration = round_schoolwise((payment_end_utc - payment_start_utc).seconds/60)
        #if p['cumulative_units'] != true_cumulative_duration:
        #    print("Note mismatch between p['cumulative_units'] ({}) and true_cumulative_duration ({})".format(p['cumulative_units'], true_cumulative_duration))
        # These mismatches are all large gaps now, all (I think) resulting from purchases before parking needs to be paid for.
        #    pprint(rec)
        if p['purchase_type'] == 'New':
            inferred_rate = p['amount']/(true_cumulative_duration/60.0)
            # Check whether rate rounds nicely.
            # Currently expected hourly rates:  [0.5,1,1.5,1.75,2,2.5,3,4] (multiples of 25 cents)
            # and then there are the oddballs: $1.50+$2hr SR
            computed_rates[inferred_rate] += 1
            zone, _, _ = numbered_zone(p['meter_id'])
            rates_by_zone[zone].append(inferred_rate)

            if (p['amount'] == 0.5 and true_cumulative_duration == 8) or (p['amount'] == 0.25 and true_cumulative_duration == 4):
            #   This seems necessary since otherwise I get this:
            #     $0.5/0.13 = an inferred rate of $3.75/hour, rounded rate = $3.75/hour.
            #       payment/rounded_rate = 8.0 minutes vs. actual minutes (8). Error = 0.000
            #        round(minutes_bought) == @Units, so this one seems to add up.
                inferred_rate = 4.0
                print("Coercing ${:.2f}, {}-minute transaction to $4/hour.".format(p['amount'], true_cumulative_duration))
                # But then this is the result:
                #      payment/rounded_rate = 22.5 minutes vs. actual minutes (23). Error = -0.500
                # This suggests that CALE is not using banker's rounding...

            # Some meters have mixed rates: S. Craig changes from a $3/hour rate to a $2.50/hour rate at some time (3pm?)


            if inferred_rate not in known_rates:
                rounded_guess = round_schoolwise(inferred_rate/(0.25))*0.25
                # Assume the rate is what you get by rounding to the nearest multiple of 25 cents.
                print("${}/{:.2f} = an inferred rate of ${}/hour, rounded rate = ${}/hour.".format(p['amount'], true_cumulative_duration/60.0, inferred_rate, rounded_guess))
                # Is the number of minutes just the result of rounding to the nearest minute?
                if rounded_guess != 0.0:
                    minutes_bought = 60*p['amount']/rounded_guess
                    error = minutes_bought - true_cumulative_duration
                    print("  payment/rounded_rate = {} minutes vs. actual minutes ({}). error = {:.3f}".format(minutes_bought, true_cumulative_duration, error))
                    if round_schoolwise(minutes_bought) == true_cumulative_duration:
                        print("   round_schoolwise(minutes_bought) == true_cumulative_duration, so this one seems to add up.")
                        if rounded_guess in known_rates:
                            resolved = True
                        else:
                            print("rounded_guess for the rate (${}) is not in the expected set!!!!! resolved = {}".format(rounded_guess,resolved))
                    else:
                        print("   round_schoolwise(minutes_bought) - true_cumulative_duration == {}".format(round_schoolwise(minutes_bought) - true_cumulative_duration))
                        print("   What's going on here?")
                if not resolved:
                    if rounded_guess == 0 or error != 0.0:
                        unresolved_count += 1
                        meter_id = p['meter_id']
                        unresolved_by_meter[meter_id] += 1
                    print(" * What's the deal with this one? * ")
                    pprint(p)
                    pprint(rec)
                    print(" ********************************** ")
    #print("\nOK, here's the distribution of computed rates:")
    #pprint(computed_rates)
    for zone in sorted(rates_by_zone):
        print(zone, sorted(rates_by_zone[zone]))

    print(" {} left unresolved out of {}.".format(unresolved_count,len(ps)))
    pprint(unresolved_by_meter)

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

    include_rate = False
    if include_rate:
        rate_lookup_by_tariff, rate_lookup_by_meter = get_lookups()

    seeding_mode = True
    linkable = [] # Purchases that can be sorted into hash-based sessions.
    all_unlinkable = [] # For now, we're excluding the warm_up period transactions.
    if seeding_mode:
        warm_up_period = timedelta(hours=12)
        print("slot_start - warm_up_period = {}".format(slot_start - warm_up_period))
        purchases = eliminate_zeros(get_parking_events(db,slot_start - warm_up_period,slot_start,pgh,True,False,caching_mode))
        #purchases = sorted(purchases, key = lambda x: x['@DateCreatedUtc'])
        rps = []
        for p in purchases:
            rps.append(raw_reframe(p,terminals,t_guids,t_ids,group_lookup_addendum,include_rate))

        if include_rate:
            # Augment raw transactions by inferring rate
            # infer_rates(rps,purchases)
            lookup_rates(rps,rate_lookup_by_tariff,rate_lookup_by_meter)

        print("len(purchases) = {}".format(len(purchases)))

    slot_end = slot_start + timechunk
    current_day = slot_start.date()
    warmup_unlinkable_count = len(purchases) - len(linkable)
    space_aggregation = 'meter'
    time_aggregation = None
    spacetime = 'meter'
    dkeys = build_raw_keys(space_aggregation, time_aggregation, include_rate)

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


        for p in purchases: # This was previously "for p in linkable + unlinkable" before the estimate_occupancy hack was put in place.
            reframed_ps.append(raw_reframe(p,terminals,t_guids,t_ids,group_lookup_addendum,include_rate))

        if include_rate:
            # Augment raw transactions by inferring rate
            #infer_rates(reframed_ps,purchases)
            lookup_rates(reframed_ps,rate_lookup_by_tariff,rate_lookup_by_meter)
    # Sort transactions by some timestamp
    #    reframed_ps = sorted(reframed_ps, key = lambda x: x['@DateCreatedUtc'])
    # Add extension/non-extension boolean:
    #    PurchaseTypeName: Normal/TopUp
    #    purchase_type: new/(continuation|extension)


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
            primary_keys = ['meter_id', 'zone', 'payment_start_utc', 'date_recorded_utc']
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
        primary_keys = ['meter_id', 'zone', 'payment_start_utc', 'date_recorded_utc']
        success = send_data_to_pipeline(server, SETTINGS_FILE, resource_name(spacetime), schema, filtered_list_of_dicts, primary_keys=primary_keys)

    else:
        success = None # The success Boolean should be defined when push_to_CKAN is false.


    return success

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
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        output_to_csv = False
        push_to_CKAN = False

        copy_of_args = list(args)

        pgh = pytz.timezone('US/Eastern')

        list_of_servers = ["meters-etl", #"official-terminals",
                "transactions-production",
                "transactions-payment-time-of",
                "transactions-prototype",
                "transactions-by-pdl",
                "split-transactions-by-pdl",
                "debug",
                "testbed",
                "sandbox",
                #"aws-test"
                ] # This list could be automatically harvested from SETTINGS_FILE.

        slot_start = None
        halting_time = None
        kwparams = {}
        # This is a new way of parsing command-line arguments that cares less about position
        # and just does its best to identify the user's intent.
        for k,arg in enumerate(copy_of_args):
            if arg in ['scan', 'save', 'csv']:
                output_to_csv = True
                args.remove(arg)
            #elif arg in ['pull', 'push', 'ckan']:
            #    push_to_CKAN = True
            #    args.remove(arg)
            #elif arg in list_of_servers:
            #    kwparams['server'] = arg
            #    args.remove(arg)
            elif slot_start is None:
                slot_start_string = arg
                slot_start = pgh.localize(parser.parse(slot_start_string))
                kwparams['slot_start'] = slot_start
                args.remove(arg)
                #except:
                #    slot_start = pgh.localize(datetime.strptime(slot_start_string,'%Y-%m-%dT%H:%M:%S'))
            elif halting_time is None:
                halting_time_string = arg
                halting_time = pgh.localize(parser.parse(halting_time_string))
                kwparams['halting_time'] = halting_time
                args.remove(arg)
            else:
                print("I have no idea what do with args[{}] = {}.".format(k,arg))

        kwparams['output_to_csv'] = output_to_csv
        kwparams['push_to_CKAN'] = push_to_CKAN
        pprint(kwparams)
        main(**kwparams)
    else:
        raise ValueError("Please specify some command-line parameters")
