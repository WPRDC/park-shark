"""This script takes CSV exports from CALE Web Office and adds the transactions to a directory of SQLite databases, where each database holds the transactions from a particular day, where the day is chosen (and the SQLite directory is named) based on the reference_time field."""
import csv, pytz
from dateutil import parser
from process_data import time_to_field, bulk_upsert_to_sqlite
from collections import defaultdict
from pprint import pprint

from parameters.local_parameters import missing_data_path, path

def special_conversion(d):
    #Terminal - Terminal ID,Pay Unit - Name,Node,Purchase Date Local,Amount,Created in CWO,Payment Service Type - Name,Tariff Package - Name,Units,Transaction Reference,External ID,Purchase Guid,Start Date Local,Delivery Delay,End Date Local,Purchase State - Name,Pay Interval Start Local,Pay Interval End Local,Article ID
    #324541-FMURRY0001,Card,FORB-MUR-L,2017-12-30 7:49:17 PM,1.75,2017-12-30 7:49:48 PM,PrePay Code,Pgm4,105,bad0de3a8dfed005,"38029350,,25917039",96a66a95-33ae-889b-ca02-98b80790b151,2017-12-30 7:48:54 PM,31,2017-12-30 9:33:54 PM,Ongoing,2017-12-30 7:48:54 PM,2017-12-30 9:33:54 PM,1

    #Node
    #Payment Service Type - Name
    #Tariff Package - Name
    #External ID
    #Delivery Delay
    #Purchase State - Name
    #Article ID
    #{'@TerminalGuid': '596756FB-D4A9-4813-8151-FF0E317806FC', '@Amount': '3', '@PurchaseDateUtc': '2018-10-01T12:10:09', '@PaymentServiceType': 'None', 'json_PurchasePayUnit': '{"@TransactionReference": "168767264", "@Amount": "3", "@PayUnitName": "Mobile Payment"}', '@StartDateUtc': '2018-10-01T12:10:09', '@PurchaseTypeName': 'Normal', '@Units': '120', '@PurchaseGuid': '3A1AFE0C-652D-4975-8B7A-821A3BE07381', 'minute': 10, '@PayIntervalStartLocal': '2018-10-01T08:10:09', 'hour': 12, '@PayIntervalEndUtc': '2018-10-01T14:10:09', '@TerminalID': 'PBP413-1', '@PayIntervalStartUtc': '2018-10-01T12:10:09', '@PayIntervalEndLocal': '2018-10-01T10:10:09', '@StartDateLocal': '2018-10-01T08:10:09', '@EndDateLocal': '2018-10-01T10:10:09', '@EndDateUtc': '2018-10-01T14:10:09', 'hash': '$pbkdf2-sha256$2$$Seg.Z8Sly212iPr3VcKUgFb.YaYQaSiALHW633zr/w0', '@DateCreatedUtc': '2018-10-01T12:10:23.560', '@PurchaseDateLocal': '2018-10-01T08:10:09'}

    # @TerminalGuid could not be extracted through CWO.

    pgh = pytz.timezone('US/Eastern')
    utc = pytz.utc
    jsonPPU = '{{"@TransactionReference": "{}", "@Amount": "{}", "@PayUnitName": "{}"}}'.format(d['Transaction Reference'], d['Amount'], d['Pay Unit - Name'])

    # Now, one problem might be duplication of Purchase Guid values so that separate Pay Unit - Names for the same transaction can be on separate lines.
    # However, none of the transactions I downloaded have this problem.
    p = {'@TerminalID': d['Terminal - Terminal ID'],
        '@PurchaseGuid': d['Purchase Guid'].upper(),
        '@Amount': d['Amount'],
        '@Units': d['Units'],
        '@PurchaseDateLocal': d['Purchase Date Local'],
        '@PurchaseDateUtc': pgh.localize(parser.parse(d['Purchase Date Local'])).astimezone(utc).strftime("%Y-%m-%dT%H:%M:%S"),
        'json_PurchasePayUnit': jsonPPU, #'{"@TransactionReference": "168767264", "@Amount": "3", "@PayUnitName": "Mobile Payment"}'
        '@StartDateLocal': d['Start Date Local'],
        '@StartDateUtc': pgh.localize(parser.parse(d['Start Date Local'])).astimezone(utc).strftime("%Y-%m-%dT%H:%M:%S"),
        #'@PurchaseTypeName': None, # I can't find this one, unfortunately. This is like "PrePay Code" and doesn't seem that important.
        # sqlite_adapter (called from sqlite functions) takes care of missing @PurchaseTypeName values.
        '@PayIntervalStartLocal': d['Pay Interval Start Local'],
        '@PayIntervalStartUtc': pgh.localize(parser.parse(d['Pay Interval Start Local'])).astimezone(utc).strftime("%Y-%m-%dT%H:%M:%S"),
        '@PayIntervalEndLocal': d['Pay Interval End Local'],
        '@PayIntervalEndUtc': pgh.localize(parser.parse(d['Pay Interval End Local'])).astimezone(utc).strftime("%Y-%m-%dT%H:%M:%S"),
        '@EndDateLocal': d['End Date Local'],
        '@EndDateUtc': pgh.localize(parser.parse(d['End Date Local'])).astimezone(utc).strftime("%Y-%m-%dT%H:%M:%S"),
        '@DateCreatedUtc': d['Created in CWO'],
        }
    return p

def add_missing_purchases(filepath,reference_time):
    local_tz = pytz.timezone('US/Eastern')
    with open(filepath) as f:
        list_of_ds = csv.DictReader(f)

        ps_by_day = defaultdict(list)
        dts_by_day = defaultdict(list)
        for d in list_of_ds:
            purchase_i = special_conversion(d)
            utc_reference_field, local_reference_field = time_to_field(reference_time)
            #upsert_to_sqlite(purchase_i,datetime_i,reference_time)
            datetime_i = (pytz.utc).localize(parser.parse(purchase_i[utc_reference_field]))
            day = datetime_i.astimezone(local_tz).date()
            # There's a shorter set of commands to get the local day, but I am doing
            # it this way to copy how process_data:get_utc_ps_for_day_from_json is 
            # currently doing it, making the day local but the datetime UTC.

            ps_by_day[day].append(purchase_i)
            dts_by_day[day].append(datetime_i)

    for day in sorted(list(ps_by_day.keys())):
        print("Grafting missing purchases from {} onto corresponding sqlite database.".format(day))
        purchases = ps_by_day[day]
        dts = dts_by_day[day]
        bulk_upsert_to_sqlite(path,purchases,dts,day,reference_time)


reference_time = 'purchase_time'
filenames = ['Purchases-2017-Operational.csv', 'Purchases-1801-1806-Operational.csv', 'Purchases-2018-07-Operational.csv',
        #'Purchases-324541-FMURRY0001-through-2013.csv', 'Purchases-324541-FMURRY0001-2014.csv', 'Purchases-324541-FMURRY0001-2015.csv', 
        'Purchases-324541-FMURRY0001-2016.csv', 'Purchases-324541-FMURRY0001-2017-to-201809.csv',
        #'Purchases-324542-FMURRY0002-through-2013.csv', 'Purchases-324542-FMURRY0002-2014.csv',  'Purchases-324542-FMURRY0002-2015.csv', 
        'Purchases-324542-FMURRY0002-2016.csv', 'Purchases-324542-FMURRY0002-2017-to-201809.csv',
        #'Purchases-325543-JCCLOT0001-through-2013.csv', 'Purchases-325543-JCCLOT0001-2014.csv', 'Purchases-325543-JCCLOT0001-2015.csv', 
        'Purchases-325543-JCCLOT0001-2016.csv', 'Purchases-325543-JCCLOT0001-2017-to-201809.csv', 
        'Purchases-410190-FORBES4002-through-2018-10.csv',
        ]

for filename in filenames:
    print("Merging in transactions from {}".format(missing_data_path+filename))
    add_missing_purchases(missing_data_path + filename,reference_time)
