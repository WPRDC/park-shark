# This script can reupload a downloaded CSV of the Aggregated Parking Transactions
# main transactions table. (The downloaded CSV file should first have the _id column
# stripped.)
from pipe.pipe_to_CKAN_resource import get_connection_parameters, BaseTransactionsSchema

import sys, os, re, json, csv, ckanapi
from pipe.gadgets import get_package_parameter
#import util
#from . import util
#import loaders, schema, pipeline
from process_data import resource_name

from datetime import datetime

from marshmallow import fields, post_load, pre_load
from collections import OrderedDict, defaultdict
from pprint import pprint

from parameters.local_parameters import PATH_TO_PIPELINE
sys.path.insert(0, PATH_TO_PIPELINE)
import pipeline as pl # This comes from the wprdc-etl repository.

class SplitTransactionsSchemaUp(BaseTransactionsSchema):
    """The split transactions schema handles the case where transactions are to be split between
    mobile transactions and meter transactions."""
    meter_transactions = fields.Integer()
    meter_payments = fields.Float()
    mobile_transactions = fields.Integer()
    mobile_payments = fields.Float()

    #@pre_load
    #def cast_fields(self,data):
    #    # If there are zero meter payments in a time slot when there are some
    #    # mobile payments, convert the None values for meter-payment parameters
    #    # to appropriately typed zeros.
    #    data['meter_payments'] = float(convert_none_to(data['meter_payments'],0.0))
    #    data['mobile_payments'] = float(convert_none_to(data['mobile_payments'],0.0))
    #    data['meter_transactions'] = convert_none_to(data['meter_transactions'],0)
    #    data['mobile_transactions'] = convert_none_to(data['mobile_transactions'],0)
    #    # This may not be necessary, but ensuring that datetimes are in
    #    # ISO format is the best way of preparing timestamps to be
    #    # sent to CKAN.
    #    data['start'] = datetime.strptime(data['start'],"%Y-%m-%d %H:%M:%S").isoformat()
    #    data['end'] = datetime.strptime(data['end'],"%Y-%m-%d %H:%M:%S").isoformat()
    #    data['utc_start'] = datetime.strptime(data['utc_start'],"%Y-%m-%d %H:%M:%S").isoformat()

def send_file_to_pipeline(server,settings_file_path,resource_name,schema,target,primary_keys,clear_first=False,chunk_size=5000):
    if resource_name is not None:
        specify_resource_by_name = True
    else:
        specify_resource_by_name = False
    if specify_resource_by_name:
        kwargs = {'resource_name': resource_name}
    #else:
        #kwargs = {'resource_id': ''}

    # Synthesize virtual file to send to the FileConnector
    #from tempfile import NamedTemporaryFile
    #ntf = NamedTemporaryFile()

    # Save the file path
    #target = ntf.name
    fields_to_publish = schema().serialize_to_ckan_fields() # These are field names and types together
    print("fields_to_publish = {}".format(fields_to_publish))
    #field_names = [f['id'] for f in fields_to_publish]
    #write_to_csv(target,list_of_dicts,field_names)

    # Testing temporary named file:
    #ntf.seek(0)
    #with open(target,'r') as g:
    #    print(g.read())

    #ntf.seek(0)
    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    #with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.

    settings, site, package_id, API_key = get_connection_parameters(server, settings_file_path)

    update_method = 'upsert'
    if len(primary_keys) == 0:
        update_method = 'insert'

    if update_method == 'insert':
        # If the datastore already exists, we need to delete it.
        # We can do this through a CKAN API call (if we know
        # the resource ID) or by setting clear_first = True
        # on the pipeline.

        # However, the ETL framework fails if you try to
        # use clear_first = True when the resource doesn't
        # exist, so check that it exists.
        resource_exists = (find_resource_id(site,package_id,kwargs['resource_name'],API_key) is not None)
        if resource_exists:
            clear_first = True

    print("Preparing to pipe data from {} to resource {} package ID {} on {}, using the update method {} with clear_first = {}".format(target,list(kwargs.values())[0],package_id,site,update_method,clear_first))

    super_pipeline = pl.Pipeline('parking_pipeline',
                                      'Pipeline for Parking Data',
                                      log_status=False,
                                      settings_file=settings_file_path,
                                      settings_from_file=True,
                                      start_from_chunk=0,
                                      chunk_size=chunk_size
                                      ) \
        .connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, server,
              clear_first=clear_first,
              fields=fields_to_publish,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              key_fields=primary_keys,
              method=update_method,
              **kwargs)

    pipe_output = super_pipeline.run()

    #package_name = get_package_parameter(site,package_id,'title',API_key)

    #log = open('uploaded.log', 'w+')

    #if specify_resource_by_name:
    #    print("Data successfully piped to {}/{}.".format(package_name,resource_name))
    #    success = True
    #    log.write("Finished upserting {} at {} \n".format(kwargs['resource_name'],datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    #else:
    #    print("Data successfully piped to {}/{}.".format(package_name,kwargs['resource_id']))
    #    success = True
    #    log.write("Finished upserting {} at {} \n".format(kwargs['resource_id'],datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    #log.close()
    #ntf.close()
    #assert not os.path.exists(target)

    #resource_id = find_resource_id(site,package_id,kwargs['resource_name'],API_key)

# Reupload to field-test
#downloaded_file = "/Users/drw/WPRDC/datasets/Parking-Data__public/downloaded-backups/aggregated-transactions/1ad5394f-d158-46c1-9af7-90a9ef4e0ce1_upload.csv"

clear_first = False
if len(sys.argv) > 1:
    if 'clear_first' in sys.argv[1:]:
        clear_first = True

#downloaded_file = "/Users/drw/WPRDC/datasets/Parking-Data__public/downloaded-backups/aggregated-transactions/2018+partial-2019-to-upload.csv"
downloaded_file = "/Users/drw/WPRDC/datasets/Parking-Data__public/downloaded-backups/aggregated-transactions/2014-2017-to-upload.csv"

primary_keys = ['zone', 'utc_start', 'start']
from parameters.local_parameters import SETTINGS_FILE
#send_file_to_pipeline(server='debug', settings_file_path=SETTINGS_FILE, resource_name='The Day', schema=SplitTransactionsSchemaUp, target=downloaded_file, primary_keys=primary_keys, chunk_size=5000)
table_name = resource_name('zone')
send_file_to_pipeline(server='transactions-production', settings_file_path=SETTINGS_FILE, resource_name=table_name, schema=SplitTransactionsSchemaUp, target=downloaded_file, primary_keys=primary_keys, clear_first=clear_first, chunk_size=5000)
