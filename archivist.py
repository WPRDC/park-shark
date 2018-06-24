import os, re, json, xmltodict

from collections import OrderedDict, Counter, defaultdict
from util import is_timezoneless, remove_field
import requests
import zipfile
from io import BytesIO # Works only under Python 3

import time, pytz
from pprint import pprint
from datetime import datetime, timedelta
from dateutil import parser

from db_util import create_or_connect_to_db, get_tables_from_db, get_ps_for_day

from parameters.credentials_file import CALE_API_user, CALE_API_password
from parameters.local_parameters import path

from nonchalance import add_hashes

#from process_data import 

def beginning_of_month(dt=None):
    """Takes a datetime and returns the first datetime before
    that that corresponds to LOCAL midnight (00:00)."""
    if dt == None : dt = datetime.now()
    return dt.replace(day=1,hour=0, minute=0, second=0, microsecond=0)

def beginning_of_week(dt=None):
    """Takes a datetime and returns the first datetime before
    that that corresponds to LOCAL midnight (00:00)."""
    if dt == None : dt = datetime.now()
    offset = dt.weekday()
    dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt - timedelta(days = offset)

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
    # the future slot_end may be.
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
    #purchases = remove_field(purchases,'@TransactionReference','PurchasePayUnit')
    purchases = remove_field(purchases,'@CardIssuer','PurchasePayUnit')

    return purchases

def get_doc_from_url_improved(url,pause=10):
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
        print("url2 = {}".format(url2))
    except:
        print("The CALE API response looks like this:")
        pprint(doc)
        print("Unable to get the first URL ({}) by using the command url2 = doc['BatchDataExportResponse']['Url'].".format(url))
        print("Waiting {} seconds and restarting.".format(pause))
        time.sleep(pause)
        return None, False

    r2 = requests.get(url2, auth=(CALE_API_user, CALE_API_password))
    if r2.status_code == 403:
        raise RuntimeError("The CALE API is returning a 403 Forbidden error, making it difficult to accomplish anything.")

    doc = xmltodict.parse(r2.text,encoding = r2.encoding)

    delays = 0
    while not r2.ok or doc['BatchDataExportFileResponse']['ExportStatus'] != 'Processed':
        time.sleep(pause)
        r2 = requests.get(url2, auth=(CALE_API_user, CALE_API_password))
        doc = xmltodict.parse(r2.text,encoding = r2.encoding)
        delays += 1
        if delays % 5 == 0:
            print("|", end="", flush=True)
        else:
            print(".", end="", flush=True)
        if delays > 100:
            return None, False

    url3 = doc['BatchDataExportFileResponse']['Url']
    print("url3 = {}".format(url3))

    # When the ZIP file is ready:
    delays = 0
    r3 = requests.get(url3, stream=True, auth=(CALE_API_user, CALE_API_password))
    if r3.status_code == 403:
        raise RuntimeError("The CALE API is returning a 403 Forbidden error, making it difficult to accomplish anything.")
    while not r3.ok and delays < 100:
        time.sleep(pause/2.0)
        r3 = requests.get(url3, stream=True, auth=(CALE_API_user, CALE_API_password))
        delays += 1
        if delays % 5 == 0:
            print("|", end="", flush=True)
        else:
            print(",", end="", flush=True)
        if delays >= 100:
            return None, False

    z = zipfile.ZipFile(BytesIO(r3.content))
    time.sleep(0.5)

    # Extract contents of a one-file zip file to memory:
    xml = z.read(z.namelist()[0])
    doc = xmltodict.parse(xml,encoding = 'utf-8')
    return doc, True

def get_week_from_json_or_api(slot_start,tz=pytz.utc,cache=True,mute=False):
    """Caches parking once it's been downloaded and checks
    cache before redownloading.

    Note that no matter what time of day is associated with slot_start,
    this function will get all of the transactions for that entire month.
    Filtering the results down to the desired time range is handled 
    elsewhere (in the calling function (e.g., get_utc_ps_for_day_from_json)).

    Caching by month ties this approach to a particular time zone. This
    is why transactions are dropped if we send this function a UTC
    slot_start (I think)."""


    slot_start = slot_start.astimezone(tz)
    month_start = beginning_of_month(slot_start)
    month_end = beginning_of_month(month_start + timedelta(days = 32))
    week_start = beginning_of_week(slot_start)
    week_end = beginning_of_week(slot_start + timedelta(days=8))

    date_format = '%Y-%m-%d'

    dashless = slot_start.strftime('%y%m')
    dashless = "{}-{}".format(week_start.strftime('%y%m%d'),(week_end - timedelta(days=1)).strftime('%y%m%d'))

    if tz == pytz.utc:
        filename = path + "month_utc_json/"+dashless+".json"
    else:
        raise ValueError("Only tz = pytz.utc is supported currently.")
   
    #too_soon = slot_start.date() >= datetime.now(tz).date() 
    # If the day that is being requested is today, definitely don't cache it.

    recent = datetime.now(tz) - week_end <= timedelta(days = 6) # This 
    # definition of recent is a little different since a) it uses slot_start 
    # rather than slot_end (which is fine here, as we know that slot_start
    # and slot_end are separated by one day) and b) it uses the time zone tz 
    # (though that should be fine since slot_start has already been converted 
    # to time zone tz).

    if not os.path.isfile(filename) or os.stat(filename).st_size == 0:
        if not mute:
            print("Sigh! {} not found, so I'm pulling the data from the API...".format(filename))

        slot_start = week_start
        slot_end = week_end
        
        if recent:
            base_url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/4/LiveDataExportService.svc/purchases/'
        else:
            base_url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/BatchDataExport/4/BatchDataExport.svc/purchase/ticket/'
            
        url = build_url(base_url,slot_start,slot_end)

        if not mute:
            print("Here's the URL: {}".format(url))

        if recent:
            raise ValueError("Unable to pull entire week from Batch Data endpoint if the end of that week is {}".format(week_end))
        else:
            downloaded = False
            while not downloaded:
                doc, downloaded = get_doc_from_url_improved(url,pause=60)
                print("!", end="", flush=True)
                

                if not downloaded:
                    raise ValueError("Unable to download a week of historical data after first attempt.")

            ps = convert_doc_to_purchases(doc['BatchExportRoot'],slot_start,date_format)

        ps = add_hashes(ps)
        purchases = cull_fields(ps)

        #print("cache = {}, recent = {}, too_soon = {}".format(cache,recent,too_soon))


        if cache and not recent:
            # Caching data from the LiveDataExport endpoint (but not today's data) is an interesting experiment.
            with open(filename, "w") as f:
                json.dump(purchases,f,indent=2)
                print("Saved a week of data from the BatchDataExport endpoint in {}".format(filename))
    else: # Load locally cached version
        with open(filename, "r", encoding="utf-8") as f:
            ps = json.load(f)
            raise ValueError("get_week_... needs to be altered to actually return these purchases.")



    return True

def get_month_from_json_or_api(slot_start,tz=pytz.utc,cache=True,mute=False):
    """Caches parking once it's been downloaded and checks
    cache before redownloading.

    Note that no matter what time of day is associated with slot_start,
    this function will get all of the transactions for that entire month.
    Filtering the results down to the desired time range is handled 
    elsewhere (in the calling function (e.g., get_utc_ps_for_day_from_json)).

    Caching by month ties this approach to a particular time zone. This
    is why transactions are dropped if we send this function a UTC
    slot_start (I think)."""

    date_format = '%Y-%m'
    slot_start = slot_start.astimezone(tz)
    month_start = beginning_of_month(slot_start)
    month_end = beginning_of_month(month_start + timedelta(days = 32))

    dashless = slot_start.strftime('%y%m')
    if tz == pytz.utc:
        filename = path + "month_utc_json/"+dashless+".json"
    else:
        raise ValueError("Only tz = pytz.utc is supported currently.")
   
    #too_soon = slot_start.date() >= datetime.now(tz).date() 
    # If the day that is being requested is today, definitely don't cache it.

    recent = datetime.now(tz) - month_end <= timedelta(days = 6) # This 
    # definition of recent is a little different since a) it uses slot_start 
    # rather than slot_end (which is fine here, as we know that slot_start
    # and slot_end are separated by one day) and b) it uses the time zone tz 
    # (though that should be fine since slot_start has already been converted 
    # to time zone tz).

    if not os.path.isfile(filename) or os.stat(filename).st_size == 0:
        if not mute:
            print("Sigh! {} not found, so I'm pulling the data from the API...".format(filename))

        slot_start = month_start
        slot_end = month_end
        
        if recent:
            base_url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/4/LiveDataExportService.svc/purchases/'
        else:
            base_url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/BatchDataExport/4/BatchDataExport.svc/purchase/ticket/'
            
        url = build_url(base_url,slot_start,slot_end)

        if not mute:
            print("Here's the URL: {}".format(url))

        if recent:
            raise ValueError("Unable to pull entire month from Batch Data endpoint if the end of that month is {}".format(month_end))
        else:
            downloaded = False
            while not downloaded:
                doc, downloaded = get_doc_from_url_improved(url,pause=60)
                print("!", end="", flush=True)
                

                if not downloaded:
                    raise ValueError("Unable to download a month of historical data after first attempt.")

            ps = convert_doc_to_purchases(doc['BatchExportRoot'],slot_start,date_format)

        ps = add_hashes(ps)
        purchases = cull_fields(ps)

        #print("cache = {}, recent = {}, too_soon = {}".format(cache,recent,too_soon))


        if cache and not recent:
            # Caching data from the LiveDataExport endpoint (but not today's data) is an interesting experiment.
            with open(filename, "w") as f:
                json.dump(purchases,f,indent=2)
                print("Saved a month of data from the BatchDataExport endpoint in {}".format(filename))
    else: # Load locally cached version
        with open(filename, "r", encoding="utf-8") as f:
            ps = json.load(f)
            raise ValueError("get_month_... needs to be altered to actually return these purchases.")



    return True



def main(*args, **kwargs):
    # This function accepts slot_start and halting_time datetimes as
    # arguments to set the time range and push_to_CKAN and output_to_csv
    # to control those output channels.
    t_begin = time.time()

    caching_mode = kwargs.get('caching_mode','utc_json')

    if caching_mode == 'db_caching':
        db_filename = kwargs.get('db_filename','transactions_cache.db') # This can be
        # changed with a passed parameter to substitute a test database
        # for running controlled tests with small numbers of events.
        db = create_or_connect_to_db(db_filename)
    else:
        db = None

    pgh = pytz.timezone('US/Eastern')
    use_cache = kwargs.get('use_cache', False)
    slot_start = pgh.localize(datetime(2012,7,23,0,0)) # The actual earliest available data.
    slot_start = pgh.localize(datetime(2018,2,5,0,0)) 
    slot_start = kwargs.get('slot_start',slot_start)

########
    #halting_time = slot_start + timedelta(hours=24)

    # halting_time = beginning_of_day(datetime.now(pgh))
    halting_time = pgh.localize(datetime(3030,4,13,0,0)) # Set halting time
    # to the far future so that the script runs all the way up to the most
    # recent data (based on the slot_start < now check in the loop below).
    #halting_time = pgh.localize(datetime(2017,3,2,0,0)) # Set halting time
    halting_time = kwargs.get('halting_time',halting_time)

    # Setting slot_start and halting_time to UTC has no effect on 
    # getting_ps_from_somewhere, but totally screws up get_batch_parking
    # (resulting in zero transactions after 20:00 (midnight UTC).
    if caching_mode == 'db_caching':
        slot_start = slot_start.astimezone(pytz.utc)
        halting_time = halting_time.astimezone(pytz.utc)
    # This is not related to the resetting of session_dict, since extending 
    # session_dict by adding on previous_session_dict did not change the fact that 
    # casting slot_start and halting_time to UTC caused all transactions
    # after 20:00 ET to not appear in the output.

    # Therefore, (until the real reason is uncovered), slot_start and halting_time
    # will only be converted to UTC when using database caching.


    it_worked = get_week_from_json_or_api(slot_start,tz=pytz.utc,cache=True,mute=False)
    return it_worked

# Overview:
# Normally, main() calls get_parking_events to get all transactions between two times.
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

if __name__ == '__main__':
    main()
