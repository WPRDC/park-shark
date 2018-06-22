import xmltodict
import os
import re

import json
from collections import OrderedDict, Counter, defaultdict
from util import to_dict, value_or_blank, unique_values, zone_name, is_a_lot, \
lot_code, is_virtual, get_terminals, is_timezoneless, write_or_append_to_csv, \
pull_from_url, remove_field, round_to_cent, corrected_zone_name, lot_list, \
pure_zones_list, numbered_reporting_zones_list, sampling_groups, \
group_by_code, numbered_zone
from fetch_terminals import pull_terminals
import requests
import zipfile
try:
    from StringIO import StringIO as BytesIO # For Python 2
except ImportError:
    from io import BytesIO # For Python 3
from copy import copy

import time
from pprint import pprint
from datetime import datetime, timedelta
import pytz

import dataset, sqlalchemy


from parameters.credentials_file import CALE_API_user, CALE_API_password
from parameters.local_parameters import path


DEFAULT_TIMECHUNK = timedelta(minutes=10)

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
# ~~~~~~~~~~~~~~~~

# Attempting to split get_ps_from_somewhere into two functions to enable day-by-day
# caching, to reduce database hits, like in the previous approach.

def get_ps_for_day(db,slot_start,cache=True,mute=False):
    # (This is designed to be the "from_somewhere" part of the function
    # formerly known as get_ps_from_somewhere.)

    # This function is for 'db_caching' caching mode.
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

            base_url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/BatchDataExport/4/BatchDataExport.svc/purchase/ticket/'
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
                        pprint(purchase_i)
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
                pprint(should_be_none)
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

    # Implicity, caching_mode is 'db_caching" in this function.

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

            base_url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/BatchDataExport/4/BatchDataExport.svc/purchase/ticket/'
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
                        pprint(purchase_i)
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
                pprint(should_be_none)
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
   
    # This function is only for cases where caching_mode == 'db_caching'.

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

