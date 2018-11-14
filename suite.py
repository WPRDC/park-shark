# This script is currently a non-updated version of the old batch_analysis.py
# but could eventually become a suite of tests.
import sys, requests, fire
from collections import defaultdict
import xmltodict
from util.util import zone_name, get_terminals, round_to_cent, corrected_zone_name, numbered_zone, lot_list, pure_zones_list, numbered_reporting_zones_list
from util.dates_util import parking_days_in_month, is_holiday
from process_data import get_batch_parking, get_parking_events, to_dict, get_utc_ps_for_day_from_json
from datetime import date, datetime, timedelta
from dateutil import parser
import time
import pytz
from pprint import pprint

from proto_get_revenue import get_revenue_and_count, set_table, clear_table

def print_dict_by_y_m_foo(d):
    for year in d.keys():
        for month in d[year].keys():
            for foo in d[year][month].keys():
                if type(foo) == int:
                    day = foo
                    dotw = datetime(year,month,day).strftime('%a')
                    print("{}/{:0>2}/{:0>2} ({}): {: >7}".format(year,month,day, dotw, d[year][month][day]))
                else:
                    print("{}/{:0>2}/{:0>2}: {: >7}".format(year,month,foo, d[year][month][foo]))
            print(" ")

def print_dict_by_foo_bar_baz(d):
    for foo in d.keys():
        for bar in d[foo].keys():
            for baz in d[foo][bar].keys():
                print("{}/{:0>2}/{:0>2}: {: >7}".format(foo,bar,baz, d[foo][bar][baz]))
            print(" ")

def quarter(month):
    return 'Q'+str((month-1)/3+1)

def aggregate_by_quarter(d):
    a = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for foo in d.keys():
        for bar in d[foo].keys():
            for baz in d[foo][bar].keys():
                a[foo][quarter(bar)][baz] += d[foo][bar][baz]
    return a

def get_parking_for_day(dt_date,tz,time_field):
    # dt_date needs to be converted to UTC (midnight in Pittsburgh
    # time maps to 4am or 5am UTC, depending on Daylight Savings Time)
    dt_naive = datetime.combine(dt_date, datetime.min.time())
    dt = tz.localize(dt_naive)
    dt_start_utc = dt.astimezone(pytz.utc)
    dt_end_utc = (dt + timedelta(days = 1)).astimezone(pytz.utc)

    pgh = pytz.timezone('US/Eastern')
    #ps_utc = get_utc_ps_for_day_from_json(dt_start_1,local_tz=pgh) # Here slot_start = 2018-09-23 04:00:00+00:00

    #print("get_parking_events actually gives the wrong answer when given a UTC datetime instead of  an Eastern time zone datetime.")

    #dt_start_utc = dt.astimezone(pgh)
    #dt_end_utc = (dt + timedelta(days = 1)).astimezone(pgh)
    #ps_utc, _ = get_utc_ps_for_day_from_json(dt_start_utc,local_tz=pgh) # While here, slot_start = 2018-09-23 00:00:00-04:00
    #print("The difference between {} and {} is {}.".format(dt_start_1, dt_start_utc, dt_start_1 - dt_start_utc))
    #print("{} events are pulled when using a UTC timestamp.".format(len(ps_utc)))
    #pprint(ps_utc[0])
    #ps_utc, _ = get_utc_ps_for_day_from_json(dt_start_utc - timedelta(days=1),local_tz=pgh) # While here, slot_start = 2018-09-23 00:00:00-04:00
    #print("The difference between {} and {} is {}.".format(dt_start_1, dt_start_utc, dt_start_1 - dt_start_utc))
    #print("{} events are pulled for the day before when using a UTC timestamp.".format(len(ps_utc)))

    #print("{} events are pulled when using a UTC timestamp and {} when using an Eastern timestamp.".format(len(ps_utc),len(ps_pgh)))

    print("Trying to fetch all parking events between {} and {}.".format(dt_start_utc,dt_end_utc))
    #purchases = get_batch_parking(dt_start_utc,dt_end_utc,cache=True,tz=pgh)
    # TESTED WITH get_batch_parking  and tz=pgh BUT THAT RELIES ON THE /json cache
    # NOT THE /utc_json cache.
    # SWITCHING TO tz=pytz.utc
    #old_purchases = get_batch_parking(dt_start_utc,dt_end_utc,cache=True,tz=pytz.utc,time_field=time_field)
    #old_purchases = get_parking_events(None,dt_start_utc,dt_end_utc,cache=True,mute=False,caching_mode='local_json') # This is equivalent to calling get_batch_parking.

    # get_parking_events may be working differently depending on whether UTC or ET time zones are 
    # thrown at it.
    purchases = get_parking_events(None,dt_start_utc,dt_end_utc,local_tz=pgh,cache=True,mute=False,caching_mode='utc_json')
    #assert len(old_purchases) == len(purchases) # This is now failing for 2018-09-24 (as one would expect).
    #purchases = get_parking_events(none,dt_start_utc,dt_end_utc,cache=true,mute=false,caching_mode='utc_json')
    print("Fetched {} candidate purchases.".format(len(purchases)))
    # [ ] Is the lack of warm-up period going to be a problem at some point?
    return purchases

def modal_zone(t,zone_type):
    if zone_type == 'old zone':
        return zone_name(t)
    elif zone_type == 'corrected zone':
        return corrected_zone_name(t)
    elif zone_type == 'numbered reporting zone':
        return numbered_zone(t['@Id'],t)[0]
        # This numbered_zone call will fall back to finding uncharted numbered zones when necessary
        # since no group_lookup_addedndum is supplied.

def find_holes():
    pass

def batch_analysis(start_date_str=None,end_date_str=None):
    if end_date_str is None and start_date_str is not None:
        end_date = (datetime.strptime(start_date_str,"%Y-%m-%d") + timedelta(days=1)).date() # This gives a date that is two days later, not one.
        print("end_date = {}".format(end_date))
        print("Assuming that end_date should be one day after start_date.")

    print("start_date_str = {}".format(start_date_str))
    zonelist = None
    use_cache = False
    use_cache = True
    terminals = get_terminals(use_cache)

    start_date = date(2018,9,30)
    end_date = date(2018,10,1)

    start_date = date(2018,9,29)
    end_date = date(2018,9,30)

    start_date = date(2018,6,27)
    end_date = date(2018,6,28)
    if False:
        start_date = date(2018,9,23)
        end_date = date(2018,9,24)

    if start_date_str is not None:
        start_date = datetime.strptime(start_date_str,"%Y-%m-%d").date()

    if end_date_str is not None:
        end_date = datetime.strptime(end_date_str,"%Y-%m-%d").date()

    date_i = start_date
    purchase_threshold = 2000
    print("Verify the purchase threshold number!")

    print("Compare parking days value to those report numbers.")

    list_of_official_zones = lot_list + pure_zones_list + numbered_reporting_zones_list

    transactions = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    overall_transactions = defaultdict(int)
    overall_payments = defaultdict(float)
    all_json_payments = 0
    payments = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    car_minutes = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    utilization = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

    transactions_by_month_zone_hour = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    number_of_hours = {'8am-10am': 2.0, '10am-2pm': 4.0, '2pm-6pm': 4.0}

    group_by_day = False
    #group_by_day = True

    group_by_hour = False

    #zone = 'Walnut St'
    #zone = 'SHADYSIDE'
    #zone = 'OAKLAND2'
    #zone = 'OAKLAND3'
    #zone = '407 - Oakland 1'
    #zone = '408 - Oakland 2'
    #zone = '409 - Oakland 3'
    #zone = '401 - Downtown 1'
    #zone = '411 - Shadyside'
    #zone = '412 - East Liberty'
    #zone = 'EASTLIB'
    #zone = 'BROOKLINE'
    #zone = '419 - Brookline'
    #zone = 'DOWNTOWN1'
    #zone = 'DOWNTOWN2'
    #zone = '402 - Downtown 2'
    #zone = 'SHADYSIDE1'
    #zone = 'NORTHSHORE'
    #zone = 'UPTOWN'
    zone = None
    if zone is None:
        for_a_particular_zone = False
    else:
        for_a_particular_zone = True

    #zone_type = 'old zone' # Probably this one should no longer be used.
    #zone_type = 'corrected zone'
    zone_type = 'numbered reporting zone'

    inactive_count = 0
    inactive_meter_set = []

    t_guids = [t['@Guid'] for t in terminals]

    missing_terminals = []

    parking_days = 0
    ps_by_meter = defaultdict(list)
    total_ps = 0
    while date_i < datetime.now().date() and date_i < end_date:
        year = date_i.year
        month = date_i.month
        if for_a_particular_zone:
            spaces = {
                'DOWNTOWN1': 284,
                'DOWNTOWN2': 351,
                'NORTHSHORE': 197, # It looks like the Q1 2016 reports switched
                # the NORTHSHORE and NORTHSIDE space counts and parking rates.
                'SHADYSIDE': 349,
                'UPTOWN': 587}
            spaces['SHADYSIDE1'] = 73 # This is just an estimate based on
            # the proportions of SHADYSIDE1 and SHADYSIDE2 non-virtual meters (10:42).
            spaces['SHADYSIDE2'] = spaces['SHADYSIDE'] - spaces['SHADYSIDE1']
            spaces['402 - Downtown 2'] = spaces['DOWNTOWN2']
            spaces['411 - Shadyside'] = spaces['SHADYSIDE']
            spaces['412 - East Liberty'] = spaces['EASTLIB'] = 395
            spaces['419 - Brookline'] = spaces['BROOKLINE'] = 235

            spaces['OAKLAND1'] = 220
            spaces['407 - Oakland 1'] = 220
            spaces['OAKLAND2'] = spaces['408 - Oakland 2'] = 313
            spaces['409 - Oakland 3'] = spaces['OAKLAND3'] = 521
            spaces['DOWNTOWN'] = spaces['DOWNTOWN1'] + spaces['DOWNTOWN2']
            spaces['401 - Downtown 1'] = spaces['DOWNTOWN1']
            cost_per_hour = { # Of course, these numbers actually change
            # over time, so it would be better to either get the real historical
            # data or else try to infer it from the amount/units ratio.
            'DOWNTOWN1': 4,
            '401 - Downtown 1': 4,
            'DOWNTOWN2': 4,
            'EASTLIB': 1,
            'NORTHSHORE': 3,
            'OAKLAND1': 3,
            '407 - Oakland 1': 3,
            '411 - Shadyside': 1.5 if date_i < date(2016,2,1) else 2.0,
            'SHADYSIDE': 1.5 if date_i < date(2016,2,1) else 2.0,
            'SHADYSIDE1': 1.5 if date_i < date(2016,2,1) else 2.0,
            'SHADYSIDE2': 1.5 if date_i < date(2016,2,1) else 2.0,
            '412 - East Liberty': 1,
            'UPTOWN': 1.5,
            'UPTOWN1': 1.5,
            'UPTOWN2': 1.5
            }
            cost_per_hour['402 - Downtown 2'] = cost_per_hour['DOWNTOWN2']
            cost_per_hour['419 - Brookline'] = cost_per_hour['BROOKLINE'] = 1
            cost_per_hour['408 - Oakland 2'] = cost_per_hour['OAKLAND2'] = 3
            cost_per_hour['409 - Oakland 3'] = cost_per_hour['OAKLAND3'] = 3

            subzones = {'SHADYSIDE': ['SHADYSIDE1','SHADYSIDE2'],
                        'SHADYSIDE1': ['SHADYSIDE1'],
                        'UPTOWN': ['UPTOWN1','UPTOWN2'],
                        'SQ.HILL': ['SQ.HILL1','SQ.HILL2']}
            for z in spaces.keys():
                if z not in subzones:
                    subzones[z] = [z]
            zonelist = subzones[zone]


        time_field = '@PurchaseDateUtc'
        if time_field == '@PurchaseDateUtc':
            local_time_field = '@PurchaseDateLocal'
        ps = get_parking_for_day(date_i,pgh,time_field) # This seems to include more than just that day's purchases.
     
        # Debugging code below
        #p_guid = "53F693C2-4BF9-4E70-89B5-5B9532461B8C"
        #terminal_id = "422005-ISABEL0001"
        ##different_tr_id = '167386925'
        ##print("Checking for Purchase GUID = {}".format(p_guid))
        #print("Checking for terminal ID = {}".format(terminal_id))
        #pdls = []
        #for p in ps:
        #    if p['@TerminalID'] == terminal_id:
        #        pprint(p)
        #        pdls.append(p['@PurchaseDateLocal'])
        ##    if p['@PurchaseGuid'] == p_guid:
        ##        print("Found @PurchaseGuid = {}!".format(p_guid))
        ##    if 'PurchasePayUnit' in p and '@TransactionReference' in p['PurchasePayUnit']:
        ##        if p['PurchasePayUnit']['@TransactionReference'] == different_tr_id:
        ##            print("Found @TransactionReference = {}".format(different_tr_id))
        #for k,pdl in enumerate(sorted(pdls)):
        #    print("{}) {}".format(k,pdl))
        #return

        for p in ps:
            local_datetime = pgh.localize(parser.parse(p[local_time_field]))
            #print("{} {} {}".format(time_field, local_datetime, date_i))
            try:
                assert local_datetime.date() == date_i # This will not be true until after the filtering.
                #                                         Although maybe the filtering has already happened above in get_parking_for_day.
            except:
                pprint(p)
                print("has a local time field ({}) that is inconsistent with the local date being {}.".format(local_time_field,date_i))
                raise ValueError("ERRor")

            ps_by_meter[p['@TerminalID']].append(p) 

        total_ps += len(ps)

        print("{}, {} purchases".format(date_i, len(ps)))
        if len(ps) > purchase_threshold:
            parking_days += 1
            if date_i.weekday() == 6 or is_holiday(date_i):
                print("     Non-parking day {} has more than {} purchases".format(date_i.strftime('%Y-%m-%d'), purchase_threshold))

        for p in ps:
            # If start time is between 8 am and 10am, add to those transactions and revenue for the month.

            all_json_payments += float(p['@Amount'])
            start_date_local = parser.parse(p[local_time_field])
    #        start_date_local = datetime.strptime(p['@StartDateLocal'],'%Y-%m-%dT%H:%M:%S')
            start_hour = start_date_local.hour

            overall_transactions[start_hour] += 1
            overall_payments[start_hour] += float(p['@Amount'])

            if p['@TerminalGuid'] in t_guids:
                t = terminals[t_guids.index(p['@TerminalGuid'])]
                mz = modal_zone(t,zone_type)
                if mz not in list_of_official_zones:
                    print("ID = {}, mz = {}, numbered_zone(t) = {}".format(p['@TerminalID'],mz,numbered_zone(t['@Id'],t)[0]))
                    # This numbered_zone call will fall back to finding uncharted numbered zones when necessary
                    # since no group_lookup_addedndum is supplied.
                transactions_by_month_zone_hour[start_date_local.month][mz][start_hour] += 1

            include = False
            if for_a_particular_zone:
                if p['@TerminalGuid'] in t_guids: # Extend this to see if a
                # corrected_zone_name (or whatever the zone_type is) can be obtained.
                    t = terminals[t_guids.index(p['@TerminalGuid'])]
                    if modal_zone(t,zone_type) in zonelist:
                        include = True
                    if modal_zone(t,zone_type) == 'Z - Inactive/Removed Terminals':
                        #print 'Z - Inactive/Removed Terminals terminal: ' + p['@TerminalID']
                        inactive_count += 1
                        if p['@TerminalGuid'] not in inactive_meter_set:
                            inactive_meter_set.append(p['@TerminalGuid'])
                elif p['@TerminalID'] not in missing_terminals:
                    print("    New missing TerminalGuid: "+p['@TerminalGuid'], missing_terminals.append(p['@TerminalGuid']))
                    #zn = corrected_zone_name(t) # We can't use corrected_zone_name when
                    # dealing with a modal_zone type of 'numbered reporting zone'.

                    if zn is not None and zn != "Z - Inactive/Removed Terminals":
                        print("Corrected zone found {}".format(zn))
                    else:
                        print("NO CORRECTED ZONE NAME FOUND.")

            else:
                include = True

            if include:
                if group_by_day:
                    year = start_date_local.year
                    month = start_date_local.month
                    day = start_date_local.day
                    transactions[year][month][day] += 1
                    payments[year][month][day] += float(p['@Amount'])
                    car_minutes[year][month][day] += int(p['@Units']) # This sum is bogus since Units are cumulative, not incremental.

                else:
                    if 8 <= start_hour < 10:
                        hours = '8am-10am'
                    elif 10 <= start_hour < 14:
                        hours = '10am-2pm'
                    elif 14 <= start_hour < 18:
                        hours = '2pm-6pm'

                    if 8 <= start_hour < 18:
                        year = start_date_local.year
                        month = start_date_local.month
                        transactions[year][month][hours] += 1
                        payments[year][month][hours] += float(p['@Amount'])
                        car_minutes[year][month][hours] += int(p['@Units'])
                        #print "Inferred rate:", float(p['@Amount'])/float(p['@Units'])
        print("Transactions: {}".format(transactions[year][month]))

                # distill_stats could also be useful here.
        #time.sleep(8.0)

        date_i += timedelta(days = 1)
    #[ ] Ensure that utilization is averaged over the correct period (which, depending on how this script is run, may be different from one month).
    if for_a_particular_zone:
        for year in transactions.keys():
            for month in transactions[year].keys():
                if not group_by_day:
                    for hour_range in transactions[year][month].keys():
                        utilization[year][month][hour_range] = payments[year][month][hour_range]/spaces[zone]/cost_per_hour[zone]/number_of_hours[hour_range]/parking_days_in_month(year,month)
    #            else:
    #                for day in sorted(transactions[year][month].keys()):
    #                    utilization[year][month][day] = payments[year][month][day]/spaces[zone]/cost_per_hour[zone]/number_of_hours[hour_range]/parking_days_in_month(year,month)
    tkeys = list(transactions.keys())
    if len(tkeys) > 0:
        print(type(tkeys[0]))
    else:
        print("No transactions found.")
    print(" ----- Transactions by month/hour/zone --------")
    print_dict_by_foo_bar_baz(transactions_by_month_zone_hour)
    if zonelist is not None:
        print("For zonelist = {}".format(str(zonelist)))
    else:
        print("If zonelist were defined, this is where it would be printed.")
    print(" ---------------- Transactions ----------------")
    print_dict_by_y_m_foo(transactions)
    print_dict_by_foo_bar_baz(aggregate_by_quarter(transactions))

    print(" ------------------ Payments ------------------")
    print_dict_by_y_m_foo(payments)
    print_dict_by_foo_bar_baz(aggregate_by_quarter(payments))
    #print(" ---------------- Car-minutes -----------------")
    #print_dict_by_y_m_foo(car_minutes) # Unadjusted car-minutes are not summable.
    print(" ---------------- Utilization -----------------")
    print_dict_by_y_m_foo(utilization)
    print_dict_by_foo_bar_baz(aggregate_by_quarter(utilization))

    print("\nParking days: {}".format(parking_days))

    print("Inactive-meter purchases: {}".format(inactive_count))

    print("Inactive meters and their corrected zone names:")
    for im in inactive_meter_set:
        t = terminals[t_guids.index(im)]
        print("{} {}".format(im, corrected_zone_name(t)))

    meter_ids = sorted(list(ps_by_meter.keys()))
    if False:
        for meter in meter_ids:
            #print("{:<20} | {}".format(meter,len(ps_by_meter[meter])))
            print("{},{}".format(meter,len(ps_by_meter[meter])))

    print(" ===== Overall transactions =======")
    for hour in sorted(overall_transactions.keys()):
        print("{:>3}   {:>6} ${:6.2f}".format(hour,overall_transactions[hour],overall_payments[hour]))

    print("\nFor start_date = {} and end_date = {}, a total of {} purchases were found, totalling ${}.".format(start_date,end_date,total_ps,all_json_payments))

    # Here end_date is the last full day. I think this might be the source of the off-by-one error above (when inferring end_date_str from start_date_str
    # and it gives an off-by-one-day error in valet month-summary calculations.

    # We therefore want to be able to move to using start_dt and end_dt as start and end boundary datetimes.
    
    start_dt = None
    end_dt = None
    def date_to_datetime(d):
        return datetime(year = d.year, month = d.month, day = d.day)

    if True:
        start_dt = date_to_datetime(start_date)
        end_dt = date_to_datetime(end_date)
        print("start_dt = {}, start_date = {}, end_dt = {}, end_date = {}".format(start_dt, start_date, end_dt, end_date))

    cumulative_ckan_revenue = 0.0
    cumulative_ckan_count = 0
    ref_time = "purchase_time"
    #ref_time = "hybrid"
    split_by_mode = True
    try:
        set_table(ref_time)
        start_hour = 0
        end_hour = 24
        revenue, transaction_count = get_revenue_and_count(split_by_mode=split_by_mode,ref_time=ref_time,zone=None,start_date=start_date,end_date=end_date,start_hour=start_hour,end_hour=end_hour,start_dt=start_dt,end_dt=end_dt,save_all=False)
        print("For comparison, pulling from the CKAN transactions repository, for start_date = {}, end_date = {}, start_hour = {}, end_hour = {}, we get {} purchases, totalling ${}."
        .format(start_date,end_date,start_hour,end_hour,transaction_count,revenue))

        print("hour CKAN results      sums from JSON files      deltas")

        fmt = "{:>2} {:>6} ${:>8.2f}      {:>6} ${:>8.2f}       {:>6} {}"
        for start_hour in [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]:
            end_hour = start_hour + 1
            start_dt = datetime(year=start_date.year, month=start_date.month, day=start_date.day, hour=start_hour) # minute, second, and millisecond will default to zero
            end_dt = start_dt + timedelta(hours=1)
            revenue, transaction_count = get_revenue_and_count(split_by_mode=split_by_mode,ref_time=ref_time,zone=None,start_date=start_date,end_date=end_date,start_hour=start_hour,end_hour=end_hour,start_dt=start_dt,end_dt=end_dt,save_all=False)
            json_transactions = 0
            json_payments = 0.0
            if start_hour in overall_transactions.keys():
                json_transactions = overall_transactions[start_hour]
                json_payments = overall_payments[start_hour]

            delta_t = transaction_count - json_transactions
            delta_p = revenue - json_payments
            delta_t_str = "" if abs(delta_t) < 0.5 else delta_t
            delta_p_str = "" if abs(delta_p) < 0.005 else "${:<8.2f}".format(delta_p)
            print(fmt.format(start_hour,transaction_count,revenue,json_transactions,json_payments,delta_t_str,delta_p_str))
            cumulative_ckan_revenue += revenue
            cumulative_ckan_count += transaction_count
            time.sleep(0.1)

        clear_table(ref_time)
        print("-------------------------------------------------------")
        print(fmt.format("", cumulative_ckan_count,cumulative_ckan_revenue,total_ps,all_json_payments,cumulative_ckan_count-total_ps,cumulative_ckan_revenue-all_json_payments))
        print("cumulative_ckan_revenue = ${}".format(cumulative_ckan_revenue))
        print("cumulative_ckan_count = {}".format(cumulative_ckan_count))

    # To process CALE extracts, this forumla is useful for converting AM/PM times to get the hour for the bin.
    # hour+12*(ampm=='PM')-12*(hour in [0,12])

    except requests.exceptions.ConnectionError:
        print("[Unable to check CKAN repository while offline.]")


def main(start_date_str=None,end_date_str=None):
    batch_analysis(start_date_str,end_date_str)

if __name__ == '__main__':
    pgh = pytz.timezone('US/Eastern')
    if len(sys.argv) == 1:
        batch_analysis()
    else:
        fire.Fire()
