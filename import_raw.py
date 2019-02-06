"""This script takes CSV exports from CALE Web Office and adds the transactions to a directory of SQLite databases, where each database holds the transactions from a particular day, where the day is chosen (and the SQLite directory is named) based on the reference_time field. 

This script is a variant on grafter.py, but is designed to be the main source of data, rather than a supplement, for a given cached day."""
import sys, csv, re, pytz
from dateutil import parser
from process_data import time_to_field, bulk_upsert_to_sqlite
from collections import defaultdict
from pprint import pprint

from parameters.local_parameters import raw_downloads_path, path

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
    #{'@TerminalGuid': '596756FB-D4A9-4813-8151-FF0E317806FC', '@Amount': '3', '@PurchaseDateUtc': '2018-10-01T12:10:09', '@PaymentServiceType': 'None', 'json_PurchasePayUnit': '{"@TransactionReference": "168767264", "@Amount": "3", "@PayUnitName": "Mobile Payment"}', '@StartDateUtc': '2018-10-01T12:10:09', '@PurchaseTypeName': 'Normal', '@Units': '120', '@PurchaseGuid': '3A1AFE0C-652D-4975-8B7A-821A3BE07381', 'minute': 10, '@PayIntervalStartLocal': '2018-10-01T08:10:09', 'hour': 12, '@PayIntervalEndUtc': '2018-10-01T14:10:09', '@TerminalID': 'PBP413-1', '@PayIntervalStartUtc': '2018-10-01T12:10:09', '@PayIntervalEndLocal': '2018-10-01T10:10:09', '@StartDateLocal': '2018-10-01T08:10:09', '@EndDateLocal': '2018-10-01T10:10:09', '@EndDateUtc': '2018-10-01T14:10:09', '@DateCreatedUtc': '2018-10-01T12:10:23.560', '@PurchaseDateLocal': '2018-10-01T08:10:09'}

    # @TerminalGuid could not be extracted through CWO.

    pgh = pytz.timezone('US/Eastern')
    utc = pytz.utc
    if 'TransactionReference' in d:
        jsonPPU = '{{"@TransactionReference": "{}", "@Amount": "{}", "@PayUnitName": "{}"}}'.format(d['Transaction Reference'], d['Amount'], d['Pay Unit - Name'])
    else:
        jsonPPU = '{{"@Amount": "{}", "@PayUnitName": "{}"}}'.format(d['Amount'], d['Pay Unit - Name'])

    # Now, one problem might be duplication of Purchase Guid values so that separate Pay Unit - Names for the same transaction can be on separate lines.
    # However, none of the transactions I downloaded have this problem.
    p = {'@TerminalID': d['Terminal - Terminal ID'],
        '@Amount': d['Amount'],
        '@Units': d['Units'],
        '@PurchaseDateLocal': parser.parse(d['Purchase Date Local']).strftime("%Y-%m-%dT%H:%M:%S"),
        '@PurchaseDateUtc': pgh.localize(parser.parse(d['Purchase Date Local'])).astimezone(utc).strftime("%Y-%m-%dT%H:%M:%S"),
        'json_PurchasePayUnit': jsonPPU, #'{"@TransactionReference": "168767264", "@Amount": "3", "@PayUnitName": "Mobile Payment"}'
        '@StartDateLocal': parser.parse(d['Start Date Local']).strftime("%Y-%m-%dT%H:%M:%S"),
        '@StartDateUtc': pgh.localize(parser.parse(d['Start Date Local'])).astimezone(utc).strftime("%Y-%m-%dT%H:%M:%S"),
        #'@PurchaseTypeName': None, # I can't find this one, unfortunately. This is like "PrePay Code" and doesn't seem that important.
        # sqlite_adapter (called from sqlite functions) takes care of missing @PurchaseTypeName values.
        '@PayIntervalStartLocal': parser.parse(d['Pay Interval Start Local']).strftime("%Y-%m-%dT%H:%M:%S"),
        '@PayIntervalStartUtc': pgh.localize(parser.parse(d['Pay Interval Start Local'])).astimezone(utc).strftime("%Y-%m-%dT%H:%M:%S"),
        '@PayIntervalEndLocal': parser.parse(d['Pay Interval End Local']).strftime("%Y-%m-%dT%H:%M:%S"),
        '@PayIntervalEndUtc': pgh.localize(parser.parse(d['Pay Interval End Local'])).astimezone(utc).strftime("%Y-%m-%dT%H:%M:%S"),
        '@EndDateLocal': parser.parse(d['End Date Local']).strftime("%Y-%m-%dT%H:%M:%S"),
        '@EndDateUtc': pgh.localize(parser.parse(d['End Date Local'])).astimezone(utc).strftime("%Y-%m-%dT%H:%M:%S"),
        '@DateCreatedUtc': parser.parse(d['Created in Data Warehouse']).strftime("%Y-%m-%dT%H:%M:%S"),
        '@PurchaseTypeName': d['Purchase Type - Name'],
        }

    if 'Purchase Guid' in d: # Unfortunately, some of the extracted data in raw_downloads is missing this field.
        p['@PurchaseGuid'] = d['Purchase Guid'].upper() # But it's OK since External ID also works.
    else: # Unfortunately, my SQLite import process is using @PurchaseGuid as a unique ID, so
        # something must go here for it to work, so as a klugy workaround...
        p['@PurchaseGuid'] = d['External ID'] + d['Ticket Number']
    if 'Tariff Package - Name' in d:
        p['@TariffPackageID'] = re.sub("Pgm","",d['Tariff Package - Name']) # normalize weirdly named tariffs

    return p

def merge(ps):
# [ ] One tricky part is that CWO downloads separate coin+card transactions into two separate rows, unlike the API.
#       * Maybe compare 'Net Amount' field to 'Amount' field to see if there are two transactions that need to be joined.
#           * However, the only good thing to join on is Purchase GUID, which is missing from a lot of the downloads.

# A cash + credit card transaction could look like this in the UTC JSON file:
#u'PurchasePayUnit': [OrderedDict([(u'@PayUnitID', u'1'), (u'@PayUnitName', u'Coin'), (u'@Amount', u'0.75'), (u'@CardFeeAmount', u'0')]),
#                      OrderedDict([(u'@PayUnitID', u'2'), (u'@PayUnitName', u'Card'), (u'@Amount', u'4'), (u'@CardIssuer', u'Visa UPDATED $1 MIN'), (u'@TransactionReference', u'17532af64eab21dc'), (u'@CardFeeAmount', u'0')])]}
# All we need is PayUnitName and Amount.

# * Simplest solution: Just join transaction on External ID. This works.
    new_p = dict(ps[0])

    new_jsonPPU = "["
    for p in ps:
        new_jsonPPU += p['json_PurchasePayUnit'] #'{"@TransactionReference": "168767264", "@Amount": "3", "@PayUnitName": "Mobile Payment"}'
    new_jsonPPU += "]"

    p['json_PurchasePayUnit'] = new_jsonPPU
    return new_p

def add_missing_purchases(filepath,reference_time):
    local_tz = pytz.timezone('US/Eastern')
    with open(filepath) as f:
        list_of_ds = csv.DictReader(f)

        ps_by_day = defaultdict(list)
        dts_by_day = defaultdict(list)
        external_id_counts = defaultdict(int)
        purchases_by_external_id = defaultdict(list)
        for d in list_of_ds:
            purchase_i = special_conversion(d)
            purchases_by_external_id.append(purchase_i)
            external_id_counts[d['External ID']] += 1

        for ps in purchases_by_external_id:
            if len(ps) > 1:
                purchase_i = merge(ps)
            elif len(ps) == 1:
                purchase_i = ps[0]

            utc_reference_field, local_reference_field = time_to_field(reference_time)
            #upsert_to_sqlite(purchase_i,datetime_i,reference_time)
            datetime_i = (pytz.utc).localize(parser.parse(purchase_i[utc_reference_field]))

            if reference_time == 'purchase_time':
                day = datetime_i.astimezone(local_tz).date()
            elif reference_time == 'purchase_time_utc':
                day = datetime_i.astimezone(pytz.utc).date()
            # There's a shorter set of commands to get the local day, but I am doing
            # it this way to copy how process_data:get_utc_ps_for_day_from_json is 
            # currently doing it, making the day local but the datetime UTC.

            ps_by_day[day].append(purchase_i)
            dts_by_day[day].append(datetime_i)

    for day in sorted(list(ps_by_day.keys())):
        print("Grafting missing purchases from {} onto corresponding sqlite database.".format(day))
        purchases = ps_by_day[day]
        dts = dts_by_day[day]
        if reference_time == 'purchase_time':
            bulk_upsert_to_sqlite_local(path,purchases,dts,day,reference_time)
        elif reference_time == 'purchase_time_utc':
            bulk_upsert_to_sqlite(path,purchases,dts,day,reference_time)

        # [ ] Update the sqlite date cache to consider this date handled (once all Purchase Date UTC transactions
        # have been handled... so basically keep track of last two dates handled, and if they're consecutive,
        # mark the previous date as cached.


#reference_time = 'purchase_time'
try:
    input = raw_input
except NameError:
    pass

reference_time = input('Choose a reference time (either purchase_time or purchase_time_utc): ')
if reference_time not in ['purchase_time', 'purchase_time_utc']:
    raise ValueError("Invalid reference time value.")

filenames = [#'Purchases-20150101-20150103-Historical.csv',
        'Purchases-20180930-20181002-Historical.csv',
        ]

for filename in filenames:
    full_path = raw_downloads_path+filename
    print("Merging in transactions from {}".format(full_path))
    add_missing_purchases(full_path,reference_time)
