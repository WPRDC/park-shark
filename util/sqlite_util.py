import os, json
import dataset, random
import time, pytz
from pprint import pprint
from datetime import datetime, timedelta
from dateutil import parser

def shorten_reference_time(reference_time):
    if reference_time == 'hybrid':
        return 'hybrid'
    if reference_time == 'purchase_time':
        return 'pdl'
    raise ValueError("No abbreviated version found for reference_time = {}".format(reference_time))

## Begin SQLite database functions ##

### Begin cached-date functions ####
# These functions are used to check whether the contents of two
# UTC JSON files have been loaded into SQLite databases: The one
# for the same local date and the one for the following day.

def get_date_filepath(path,reference_time):
    short_ref = shorten_reference_time(reference_time)
    return path + "sqlite-" + short_ref + "/cached_dates.sqlite"

def mark_date_as_cached(path,reference_time,date_i,offset):
    date_filepath = get_date_filepath(path,reference_time)
    date_table_name = 'cached_dates'
    try:
        db = dataset.connect('sqlite:///'+date_filepath)
        table = db[date_table_name]
    except sqlalchemy.exc.NoSuchTableError as e:
        print("Unable to load database {} and table {}.".format(date_filepath,data_table_name))
        table = db.create_table(date_table_name, primary_id = 'date', primary_type = 'String')
    record = {'date': date_i.strftime("%Y-%m-%d"), 'offset': offset}
    table.upsert(record, keys=['date', 'offset'])  # The second
    # argument is a list of keys used in the upserting process.

def is_date_cached(path,reference_time,date_i):
    # Return a Boolean indicating whether the date is in cached_dates
    # which specifies whether all events with [reference_time_field] values
    # equal to date_i have been cached in the database (or at
    # least all of the utc_json values, providing a base).
    date_filepath = get_date_filepath(path,reference_time)
    date_table_name = 'cached_dates'
    db = dataset.connect('sqlite:///'+date_filepath)
    cached_dates = db[date_table_name]

    date_format = '%Y-%m-%d'
    date_string = date_i.strftime(date_format)
    return (cached_dates.find_one(date=date_string, offset=0) is not None) and (cached_dates.find_one(date=date_string, offset=1) is not None)
### End cached-date functions ###

def get_table_name():
    return 'purchases'

def create_sqlite(db_filename):
    db = dataset.connect('sqlite:///'+db_filename)
    table_name = get_table_name()
    db.create_table(table_name, primary_id='@PurchaseGuid', primary_type='String')
    cached_ps = db[table_name]
    cached_ps.create_index(['hour', 'minute']) # Creating these indices should massively speed up queries.
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
    dashless = date_i.strftime('%y%m%d')
    short_ref = shorten_reference_time(reference_time)
    filepath = path + "sqlite-" + short_ref + "/" + dashless + ".sqlite"
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
    if reference_time == 'purchase_time':
        return '@PurchaseDateUtc', '@PurchaseDateLocal'
    raise ValueError('time_to_field does not know how to handle reference_time = {}'.format(reference_time))

def reverse_sqlite_adapter(d_input):
    d = dict(d_input)
    del(d['hour'])
    del(d['minute'])
    #'hybrid_parking_segment_start_utc' will not be in the returned dict.
    if 'json_PurchasePayUnit' in d:
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
    return d

def sqlite_adapter(d_input,datetime_i):
    d = dict(d_input)
    d['hour'] = datetime_i.hour
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
    # where purhcases have been assigned based on the
    # reference_time.
    table, _ = get_sqlite_table(path,datetime_i.date(),reference_time)
    p_i = sqlite_adapter(purchase_i,datetime_i)
    table.upsert(p_i, keys=['@PurchaseGuid'])  # The second
    # argument is a list of keys used in the upserting process.

def bulk_upsert_to_sqlite(path,purchases,dts,date_i,reference_time):
    """Upsert many purchases to SQLite database for corresponding day."""
    # The date is going to be used to select
    # a SQLite database that should contain all the
    # purchases from midnight to midnight (local Pittsbugh time)
    # where purhcases have been assigned based on the
    # reference_time.
    filing_date = date_i
    utc_field, local_field = time_to_field(reference_time)
    sample_date_string = random.sample(purchases,1)[0][local_field]
    sample_date = parser.parse(sample_date_string).date()
    try:
        assert filing_date == sample_date
    except:
        print("sample_date_string = {}".format(sample_date_string))
        assert filing_date == sample_date

    table, db = get_sqlite_table(path,filing_date,reference_time)

    adapted_ps = [sqlite_adapter(p,dt) for p,dt in zip(purchases,dts)]

    db.begin()
    try:
        for ap in adapted_ps:
            table.upsert(ap, keys=['@PurchaseGuid'])
        db.commit()
    except:
        db.rollback()

    #table.insert_many(adapted_ps, chunk_size=2000) # Setting ensure = True
    # was supposed to handle cases where some fields are missing values,
    # but it didn't.

    #For example, in sqlite3library I often use this calls:

#db.executemany('REPLACE INTO .... ', list_of_dicts)
#https://stackoverflow.com/questions/18219779/bulk-insert-huge-data-into-sqlite-using-python
# Do most of the work of creation in memory to make it fast.
# THEN save the result to a file.
def bulk_insert_into_sqlite(path,purchases,dts,date_i,reference_time):
    """Insert purchases into SQLite database for corresponding day."""
    # The date is going to be used to select
    # a SQLite database that should contain all the
    # purchases from midnight to midnight (local Pittsbugh time)
    # where purhcases have been assigned based on the
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
