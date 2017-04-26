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
from process_data import roundTime, build_url, convert_doc_to_purchases

last_date_cache = None
all_day_ps_cache = []
dts = []

#To do: Remove more unneeded functions from this file and import them from process_data
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
    slot_start = pgh.localize(datetime(2014,2,5,0,0))
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
