#!/usr/bin/env python
from . import datapusher # Switch to this import statement to support Python 3
import sys, json

from collections import OrderedDict, defaultdict
import re, os
from . import gadgets

DEFAULT_CKAN_INSTANCE = 'https://data.wprdc.org'

def upsert_data(dp,resource_id,data):
    # Upsert data to the CKAN datastore (as configured in dp) and the
    # given resource ID.

    # The format of the data variable is a list of dicts, where each
    # dict represents a row of the array, with the column name being
    # the key and the column value being the value.

    # The types of the columns are defined when the datastore is
    # created/recreated, in a command like this:
    # dp.create_datastore(resource_id, reordered_fields, keys)

    # which returns a result like this:
    # {u'fields': [{u'type': u'text', u'id': u'Year+month'}, {u'type': u'text', u'id': u'Package'}, {u'type': u'text', u'id': u'Resource'}, {u'type': u'text', u'id': u'Publisher'}, {u'type': u'text', u'id': u'Groups'}, {u'type': u'text', u'id': u'Package ID'}, {u'type': u'text', u'id': u'Resource ID'}, {u'type': u'int', u'id': u'Pageviews'}], u'method': u'insert', u'primary_key': [u'Year+month', u'Resource ID'], u'resource_id': u'3d6b60f4-f25a-4e93-94d9-730eed61f69c'}
    #fields_list =
    #OrderedDict([('Year+month', u'201612'), ('Package', u'Allegheny County Air Quality'), ('Resource', u'Hourly Air Quality Data'), ('Publisher', u'Allegheny County'), ('Groups', u'Environment'), ('Package ID', u'c7b3266c-adc6-41c0-b19a-8d4353bfcdaf'), ('Resource ID', u'15d7dbf6-cb3b-407b-ae01-325352deed5c'), ('Pageviews', u'0')])
    r = dp.upsert(resource_id, data, method='upsert')
    if r.status_code != 200:
        print(r.text)
    else:
        print("Data successfully stored.")
    print("Status code: %d" % r.status_code)
    return r.status_code == 200

def open_a_channel(server):
    # Open a channel
    with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.
        settings = json.load(f)
    dp = datapusher.Datapusher(settings, server=server)
    site = gadgets.get_site(settings,server)
    API_key = settings["API Keys"][server]

    return dp, settings, site, API_key

def push_to_extant_datastore(server, resource_id, list_of_dicts, upload_in_chunks=True, chunk_size=1000, keys=None):

    dp, _, _, _ = open_a_channel(server)

    if not upload_in_chunks:
        return upsert_data(dp,resource_id,list_of_dicts)

    success = True
    k = 0
    while(k < len(list_of_dicts)):
        print("k = {}".format(k))
        rows_to_upload = list_of_dicts[k:k+chunk_size]
        done = upsert_data(dp,resource_id,list_of_dicts)
        success = success and done
        k += chunk_size


        #Upserting to resource B:
        #k = 0
        #{"help": "https://data.wprdc.org/api/3/action/help_show?name=datastore_upsert", "success": false, "error": {"table": ["table does not have a unique key defined"], "__type": "Validation Error"}}
        #Status code: 409

        # Translation: If the existing aggregation table does not have a unique key
        # defined, upserting does not work.
        # Solution: Recreate the entire datastore for that table from scratch.
        # dp.delete_datastore(resource_id)
        # dp.create_datastore(resource_id, reordered_fields, keys='CRASH_CRN')
    return success

def push_data_to_ckan(server, resource_id, list_of_dicts, upload_in_chunks=True, chunk_size=1000, keys=None):
    # This function currently assumes that the repository has already been
    # set up (that is, the datastore exists, the fields are defined and
    # typed and have an order).

    # [ ] Eventually extend this to check whether the datastore needs to
    # be set up, and if it does, to somehow specify the order of the
    # columns.

    # If the datastore has not been set up with a unique key or keys
    # already, trying to upsert results in a 409 error
    # ("table does not have a unique key defined").

    dp, _, _, API_key = open_a_channel(server)

    # [ ] Eventually check here to see if a) the datastore exists and
    # b) it's got fields set up.
    #       * If not, set that stuff in up in another function called
    #         from here.
    success = push_to_extant_datastore(server, resource_id, list_of_dicts, upload_in_chunks, chunk_size, keys)

    return success


def main():
    upload_in_chunks = True
    server = "Live"

    resource_id = sys.argv[1]
    filename = None
    if len(sys.argv) > 2:
        filename = sys.argv[2] # Name of the file that contains the data to be uploaded.
    #upload_file_to_CKAN(resource_id,filename) # This functionality would best be reproduced
    #by calling the existing wprdc-etl pipeline library.

    #pipe_csv_to_CKAN will also eventually provide this functionality.
    
############

if __name__ == '__main__':
    main()
