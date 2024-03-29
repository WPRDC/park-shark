import os, json
import dataset, random
import time, pytz
from pprint import pprint
from datetime import datetime, timedelta
from dateutil import parser
import sqlalchemy
import traceback

def shorten_reference_time(reference_time):
    if reference_time == 'hybrid':
        return 'hybrid'
    if reference_time == 'purchase_time':
        return 'pdl'
    if reference_time == 'purchase_time_utc':
        return 'pdu'
    raise ValueError("No abbreviated version found for reference_time = {}".format(reference_time))

## Begin SQLite database functions ##

### Begin cached-date functions ####
# These functions are used to check whether the contents of two
# UTC JSON files have been loaded into SQLite databases: The one
# for the same local date and the one for the following day.

def format_date(date_i):
    date_format = '%Y-%m-%d'
    return date_i.strftime(date_format)

def get_date_filepath(path,reference_time):
    short_ref = shorten_reference_time(reference_time)
    return path + "sqlite-" + short_ref + "/cached_dates.sqlite"

def get_cached_dates_table(date_filepath,date_table_name):
    try:
        db = dataset.connect('sqlite:///'+date_filepath)
        table = db[date_table_name]
    except sqlalchemy.exc.OperationalError as e:
        # If unable to load the table because the directory does not exist, try creating the directory.
        print("Unable to load database {}.".format(date_filepath))
        directory = '/'.join(date_filepath.split('/')[:-1])
        if not os.path.isdir(directory):
            os.makedirs(directory)
        # Note that the purchases database has a dedicated creation function,
        #   create_sqlite(filepath),
        # but this seems not to be true of the cached_dates table.
        db = dataset.connect('sqlite:///'+date_filepath)
        table = db[date_table_name]
    except sqlalchemy.exc.NoSuchTableError as e:
        print("Unable to load database {} and table {}.".format(date_filepath,date_table_name))
        table = db.create_table(date_table_name, primary_id = 'date', primary_type = db.types.text) # 'String')
    return table

def mark_date_as_cached(path,reference_time,date_i,offset):
    date_filepath = get_date_filepath(path,reference_time)
    date_table_name = 'cached_dates'
    table = get_cached_dates_table(date_filepath, date_table_name)
    record = {'date': format_date(date_i), 'offset': offset}
    table.upsert(record, keys=['date', 'offset'])  # The second
    # argument is a list of keys used in the upserting process.

def is_date_cached(path,reference_time,date_i):
    # Return a Boolean indicating whether the date is in cached_dates
    # which specifies whether all events with [reference_time_field] values
    # equal to date_i have been cached in the database (or at
    # least all of the utc_json values, providing a base).
    date_filepath = get_date_filepath(path,reference_time)
    date_table_name = 'cached_dates'
    cached_dates = get_cached_dates_table(date_filepath, date_table_name)

    date_string = format_date(date_i)
    return (cached_dates.find_one(date=date_string, offset=0) is not None) and (cached_dates.find_one(date=date_string, offset=1) is not None)

def mark_utc_date_as_cached(path,reference_time,date_i):
    date_filepath = get_date_filepath(path,reference_time)
    date_table_name = 'cached_dates'
    table = get_cached_dates_table(date_filepath, date_table_name)
    record = {'date': format_date(date_i)}
    table.upsert(record, keys=['date'])  # The second
    # argument is a list of keys used in the upserting process.

def is_utc_date_cached(path,reference_time,date_i):
    # Return a Boolean indicating whether the date is in cached_dates
    # which specifies whether all events with [reference_time_field] values
    # equal to date_i have been cached in the database (or at
    # least all of the utc_json values, providing a base).
    date_filepath = get_date_filepath(path,reference_time)
    date_table_name = 'cached_dates'
    cached_dates = get_cached_dates_table(date_filepath, date_table_name)

    date_string = format_date(date_i)
    return (cached_dates.find_one(date=date_string) is not None)

### End cached-date functions ###

def get_table_name():
    return 'purchases'

def get_purchases_filepath(path,reference_time,date_i):
    dashless = date_i.strftime('%y%m%d')
    short_ref = shorten_reference_time(reference_time)
    filepath = path + "sqlite-" + short_ref + "/" + dashless + ".sqlite"
    return filepath

def create_sqlite(db_filename):
    db = dataset.connect('sqlite:///'+db_filename)
    table_name = get_table_name()
    db.create_table(table_name, primary_id='@PurchaseGuid', primary_type=db.types.text) #'String')
    cached_ps = db[table_name]
    #cached_ps.create_index(['hour', 'minute']) # Creating these indices should massively speed up queries. # Commented this line out to make the push_recent_days.py script work.
    #cached_ps.create_index(['unix_time']) # Creating this index should massively speed up queries.
    return db

def create_or_connect_to_sqlite(filepath):
    # It may be possible to do some of this more succinctly by using
    # get_table(table_name, primary_id = 'id', primary_type = 'Integer)
    # to create the table if it doesn't already exist.
    print("Checking for filepath = {}".format(filepath))
    if not os.path.isfile(filepath):
        db = create_sqlite(filepath)
        return db
    try:
        db = dataset.connect('sqlite:///'+filepath)
        print("Database file found with tables {}.".format(db.tables))
    except sqlalchemy.exc.NoSuchTableError as e:
        print("Unable to load database {} and table {}.".format(filepath,get_table_name()))
        directory = '/'.join(filepath.split('/')[:-1])
        if not os.path.isdir(directory):
            os.makedirs(directory)
        db = create_sqlite(filepath)
        print("Created new database ({}).".format(filepath))
    return db

def get_sqlite_table(path,date_i,reference_time):
    """For a given date, return the corresponding database table.
    The name of the directory of databases reflects the reference field,
    and all of the transactions in the database for a particular day
    are based on local time.


    """
    filepath = get_purchases_filepath(path,reference_time,date_i)
    tz = pytz.timezone('US/Eastern')
    too_soon = date_i >= datetime.now(tz).date()
    # If the day that is being requested is today, definitely don't cache it.

    recent = datetime.now(tz).date() - date_i <= timedelta(days = 5) # This
    # definition of recent is a little different since a) it uses slot_start
    # rather than slot_end (which is fine here, as we know that slot_start
    # and slot_end are separated by one day) and b) it uses the time zone tz
    # (though that should be fine since slot_start has already been converted
    # to time zone tz).
    table_name = get_table_name()
    if not os.path.isfile(filepath) or os.stat(filepath).st_size == 0:
        #if not mute:
        print("{} not found. Stand by while the database is created.".format(filepath))

    db = create_or_connect_to_sqlite(filepath)
    return db[table_name], db

def time_to_field(reference_time):
    if reference_time in ['purchase_time', 'purchase_time_utc']:
        return '@PurchaseDateUtc', '@PurchaseDateLocal'
    raise ValueError('time_to_field does not know how to handle reference_time = {}'.format(reference_time))

def reverse_sqlite_adapter(d_input):
    d = dict(d_input)
    del(d['hour'])
    del(d['minute'])
    #'hybrid_parking_segment_start_utc' will not be in the returned dict.
    if 'json_PurchasePayUnit' in d and d['json_PurchasePayUnit'] is not None:
        # PurchasePayUnit looks like this:
        #    "PurchasePayUnit": {
        #              "@PayUnitName": "None",
        #              "@Amount": "0.25"
        #            }
        # or this:
        #        "PurchasePayUnit": {
        #          "@PayUnitName": "Card",
        #          "@Amount": "1",
        #          "@TransactionReference": "ccaff5f1ebbc101c"
        #        },
        # or this:
        #    "PurchasePayUnit": {
        #      "@PayUnitName": "Mobile Payment",
        #      "@Amount": "3",
        #      "@TransactionReference": "174994079"
        #    }
        # or this:
        #    "PurchasePayUnit": { "@PayUnitName": "Coin", "@Amount": "0.25" }
        # or occasionally it's a list of two different dicts.
        # To capture all this data, let's just stringify/serialize it
        # and then deserialize it after getting it from the SQLite database
        # (or as needed).
        d['PurchasePayUnit'] = json.loads(d['json_PurchasePayUnit'])
        del d['json_PurchasePayUnit']
    if 'hash' in d and d['hash'] is None: # 'hash' fields with values of None
        del d['hash'] # are added before inserting records into the SQLite database
        # for compatibility with hash-having records, but they also need to be
        # removed to conform to process_data's expectations about transactions that
        # have hashes.
    return d

def sqlite_adapter(d_input,datetime_i):
    d = dict(d_input)
    d['hour'] = datetime_i.hour # Note that this is the UTC hour.
    d['minute'] = datetime_i.minute
    if '@PurchaseTypeName' not in d:
        d['@PurchaseTypeName'] = None
    if 'hash' not in d:
        d['hash'] = None
    if 'hybrid_parking_segment_start_utc' in d:
        del d['hybrid_parking_segment_start_utc']
    if 'PurchasePayUnit' in d:
        # PurchasePayUnit looks like this:
        #    "PurchasePayUnit": {
        #              "@PayUnitName": "None",
        #              "@Amount": "0.25"
        #            }
        # or this:
        #        "PurchasePayUnit": {
        #          "@PayUnitName": "Card",
        #          "@Amount": "1",
        #          "@TransactionReference": "ccaff5f1ebbc101c"
        #        },
        # or this:
        #    "PurchasePayUnit": {
        #      "@PayUnitName": "Mobile Payment",
        #      "@Amount": "3",
        #      "@TransactionReference": "174994079"
        #    }
        # or this:
        #    "PurchasePayUnit": { "@PayUnitName": "Coin", "@Amount": "0.25" }
        # or occasionally it's a list of two different dicts.
        # To capture all this data, let's just stringify/serialize it
        # and then deserialize it after getting it from the SQLite database
        # (or as needed).
        d['json_PurchasePayUnit'] = json.dumps(d['PurchasePayUnit'])
        del d['PurchasePayUnit']
    return d

def upsert_to_sqlite(path,purchase_i,datetime_i,reference_time):
    """Add purchase to SQLite database for corresponding day."""
    # The date is going to be used to select
    # a SQLite database that should contain all the
    # purchases from midnight to midnight (local Pittsbugh time)
    # where purchases have been assigned based on the
    # reference_time.
    table, _ = get_sqlite_table(path,datetime_i.date(),reference_time)
    p_i = sqlite_adapter(purchase_i,datetime_i)
    table.upsert(p_i, keys=['@PurchaseGuid'])  # The second
    # argument is a list of keys used in the upserting process.

def bulk_upsert_to_sqlite(path,purchases,dts,date_i,reference_time):
    """Upsert many purchases to SQLite database for corresponding UTC day."""
    # The date is going to be used to select
    # a SQLite database that should contain all the
    # purchases from midnight to midnight (local Pittsbugh time)
    # where purchases have been assigned based on the
    # reference_time.
    filing_date = date_i
    utc_field, local_field = time_to_field(reference_time)
    sample_date_string = random.sample(purchases,1)[0][utc_field]
    sample_date = parser.parse(sample_date_string).date()
    try:
        assert filing_date == sample_date
    except:
        print("filing_date = {}, sample_date = {}, sample_date_string = {}".format(filing_date,sample_date,sample_date_string))
        assert filing_date == sample_date

    table, db = get_sqlite_table(path,filing_date,reference_time)

    adapted_ps = [sqlite_adapter(p,dt) for p,dt in zip(purchases,dts)]

    db.begin()
    try:
        for ap in adapted_ps:
            table.upsert(ap, keys=['@PurchaseGuid'])
        db.commit()
        print("Upsert of {} rows to {} succeeded.".format(len(adapted_ps), db))
    except Exception as err:
        db.rollback()
        print("Upsert of {} rows failed. Rolling back {}".format(len(adapted_ps), db))
        traceback.print_tb(err.__traceback__)
        print("Let's try another approach since the database-transactions-commit approach is failing.")
        for ap in adapted_ps:
            table.upsert(ap, keys=['@PurchaseGuid'])

    #For example, in sqlite3library I often use this calls:

#db.executemany('REPLACE INTO .... ', list_of_dicts)
#https://stackoverflow.com/questions/18219779/bulk-insert-huge-data-into-sqlite-using-python
# Do most of the work of creation in memory to make it fast.
# THEN save the result to a file.

def bulk_upsert_to_sqlite_local(path,purchases,dts,date_i,reference_time):
    """Upsert many purchases to SQLite database for corresponding local day."""
    # The date is going to be used to select
    # a SQLite database that should contain all the
    # purchases from midnight to midnight (local Pittsbugh time)
    # where purchases have been assigned based on the
    # reference_time.
    filing_date = date_i
    utc_field, local_field = time_to_field(reference_time)
    sample_date_string = random.sample(purchases,1)[0][local_field]
    sample_date = parser.parse(sample_date_string).date()
    try:
        assert filing_date == sample_date
    except:
        print("filing_date = {}, sample_date = {}, sample_date_string = {}".format(filing_date,sample_date,sample_date_string))
        assert filing_date == sample_date

    table, db = get_sqlite_table(path,filing_date,reference_time)

    adapted_ps = [sqlite_adapter(p,dt) for p,dt in zip(purchases,dts)]

    db.begin()
    try:
        for ap in adapted_ps:
            table.upsert(ap, keys=['@PurchaseGuid'])
        db.commit()
        print("Upsert of {} rows to {} succeeded.".format(len(adapted_ps), db))
    except Exception as err:
        db.rollback()
        print("Upsert of {} rows failed. Rolling back {}".format(len(adapted_ps), db))
        traceback.print_tb(err.__traceback__)
        print("Let's try another approach since the database-transactions-commit approach is failing.")
        for ap in adapted_ps:
            table.upsert(ap, keys=['@PurchaseGuid'])

    #For example, in sqlite3library I often use this calls:

#db.executemany('REPLACE INTO .... ', list_of_dicts)
#https://stackoverflow.com/questions/18219779/bulk-insert-huge-data-into-sqlite-using-python
# Do most of the work of creation in memory to make it fast.
# THEN save the result to a file.

def bulk_insert_into_sqlite(path,purchases,dts,date_i,reference_time):
    """Insert purchases into SQLite database for corresponding UTC day."""
    # The date is going to be used to select
    # a SQLite database that should contain all the
    # purchases from midnight to midnight (local Pittsbugh time)
    # where purchases have been assigned based on the
    # reference_time.
    filing_date = date_i
    utc_field, local_field = time_to_field(reference_time)
    sample_date_string = random.sample(purchases,1)[0][utc_field]
    sample_date = parser.parse(sample_date_string).date()
    assert filing_date == sample_date
    table, _ = get_sqlite_table(path,filing_date,reference_time)

    adapted_ps = [sqlite_adapter(p,dt) for p,dt in zip(purchases,dts)]
    table.insert_many(adapted_ps, chunk_size=2000) # Setting ensure = True
    # was supposed to handle cases where some fields are missing values,
    # but it didn't.

    #For example, in sqlite3library I often use this calls:

#db.executemany('REPLACE INTO .... ', list_of_dicts)
#https://stackoverflow.com/questions/18219779/bulk-insert-huge-data-into-sqlite-using-python
# Do most of the work of creation in memory to make it fast.
# THEN save the result to a file.

def bulk_insert_into_sqlite_local(path,purchases,dts,date_i,reference_time):
    """Insert purchases into SQLite database for corresponding local day."""
    # The date is going to be used to select
    # a SQLite database that should contain all the
    # purchases from midnight to midnight (local Pittsbugh time)
    # where purchases have been assigned based on the
    # reference_time.
    filing_date = date_i
    utc_field, local_field = time_to_field(reference_time)
    sample_date_string = random.sample(purchases,1)[0][local_field]
    sample_date = parser.parse(sample_date_string).date()
    assert filing_date == sample_date
    table, _ = get_sqlite_table(path,filing_date,reference_time)

    adapted_ps = [sqlite_adapter(p,dt) for p,dt in zip(purchases,dts)]
    table.insert_many(adapted_ps, chunk_size=2000) # Setting ensure = True
    # was supposed to handle cases where some fields are missing values,
    # but it didn't.

    #For example, in sqlite3library I often use this calls:

#db.executemany('REPLACE INTO .... ', list_of_dicts)
#https://stackoverflow.com/questions/18219779/bulk-insert-huge-data-into-sqlite-using-python
# Do most of the work of creation in memory to make it fast.
# THEN save the result to a file.
## End SQLite database functions ##

def get_events_from_sqlite(path,date_i,reference_time):
    table, db = get_sqlite_table(path,date_i,reference_time)
    adapted_ps = [reverse_sqlite_adapter(p) for p in table.all()]
    utc_reference_field, local_reference_field = time_to_field(reference_time)
    dts_all = [(pytz.utc).localize(parser.parse(p[utc_reference_field])) for p in adapted_ps]
    return adapted_ps, dts_all


def clear_cache_for_date(path,reference_time,date_i):
    # Step 1: Revise the cached-dates table to reflect the fact that the
    # date is no longer considered cached.
    date_filepath = get_date_filepath(path,reference_time)
    date_table_name = 'cached_dates'
    cached_dates = get_cached_dates_table(date_filepath, date_table_name)
    cached_dates.delete(date = format_date(date_i))

    # Step 2: Delete the SQLite file for that date.
    filepath = get_purchases_filepath(path,reference_time,date_i)
    if os.path.isfile(filepath):
        os.remove(filepath)
