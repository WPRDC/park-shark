#!/usr/bin/env python
import sys, os, re, json, csv, ckanapi
from pipe.gadgets import get_package_parameter
#import util
#from . import util
#import loaders, schema, pipeline

from datetime import datetime

sys.path.insert(0, '/home/david/wprdc-etl') # A path that we need to import code from
import pipeline as pl

from marshmallow import fields, post_load, pre_load
from collections import OrderedDict, defaultdict
from pprint import pprint


DEFAULT_CKAN_INSTANCE = 'https://data.wprdc.org'

def convert_none_to(x,new_value):
    if x is None:
        return new_value
    return x

class BaseTransactionsSchema(pl.BaseSchema):
    zone = fields.String()
    start = fields.DateTime()
    end = fields.DateTime()
    utc_start = fields.DateTime()

    class Meta:
        ordered = True

class TransactionsSchema(BaseTransactionsSchema):
    transactions = fields.Integer()
    payments = fields.Float()

    @pre_load
    def cast_fields(self,data):
        data['payments'] = float(data['payments'])
        # This may not be necessary, but ensuring that datetimes are in
        # ISO format is the best way of preparing timestamps to be
        # sent to CKAN.
        data['start'] = datetime.strptime(data['start'],"%Y-%m-%d %H:%M:%S").isoformat()
        data['end'] = datetime.strptime(data['end'],"%Y-%m-%d %H:%M:%S").isoformat()
        data['utc_start'] = datetime.strptime(data['utc_start'],"%Y-%m-%d %H:%M:%S").isoformat()

class SplitTransactionsSchema(BaseTransactionsSchema):
    """The split transactions schema handles the case where transactions are to be split between
    mobile transactions and meter transactions."""
    meter_transactions = fields.Integer()
    meter_payments = fields.Float()
    mobile_transactions = fields.Integer()
    mobile_payments = fields.Float()

    @pre_load
    def cast_fields(self,data):
        # If there are zero meter payments in a time slot when there are some
        # mobile payments, convert the None values for meter-payment parameters
        # to appropriately typed zeros.
        data['meter_payments'] = float(convert_none_to(data['meter_payments'],0.0))
        data['mobile_payments'] = float(convert_none_to(data['mobile_payments'],0.0))
        data['meter_transactions'] = convert_none_to(data['meter_transactions'],0)
        data['mobile_transactions'] = convert_none_to(data['mobile_transactions'],0)
        # This may not be necessary, but ensuring that datetimes are in
        # ISO format is the best way of preparing timestamps to be
        # sent to CKAN.
        data['start'] = datetime.strptime(data['start'],"%Y-%m-%d %H:%M:%S").isoformat()
        data['end'] = datetime.strptime(data['end'],"%Y-%m-%d %H:%M:%S").isoformat()
        data['utc_start'] = datetime.strptime(data['utc_start'],"%Y-%m-%d %H:%M:%S").isoformat()

class SamplingTransactionsSchema(TransactionsSchema):
    parent_zone = fields.String()

class SplitSamplingTransactionsSchema(SplitTransactionsSchema):
    parent_zone = fields.String()

class OccupancySchema(pl.BaseSchema):
    zone = fields.String()
    start = fields.DateTime()
    end = fields.DateTime()
    utc_start = fields.DateTime()
    transactions = fields.Integer()
    car_minutes = fields.Integer()
    payments = fields.Float()
    durations = fields.Dict() # [ ] Verify that the deployed version of wprdc-etl can handle such Dict/JSON fields.
    inferred_occupancy = fields.Integer()

    class Meta:
        ordered = True

    @pre_load
    def cast_fields(self,data):
        if data['durations'] is None:
            data['durations'] = '{}'
        data['durations'] = json.loads(data['durations'])

        if data['payments'] is None:
            data['payments'] = 0.0
        data['payments'] = float(data['payments'])

        if data['car_minutes'] is None:
            data['car_minutes'] = 0
        # This may not be necessary, but ensuring that datetimes are in
        # ISO format is the best way of preparing timestamps to be
        # sent to CKAN.
        data['start'] = datetime.strptime(data['start'],"%Y-%m-%d %H:%M:%S").isoformat()
        data['end'] = datetime.strptime(data['end'],"%Y-%m-%d %H:%M:%S").isoformat()
        data['utc_start'] = datetime.strptime(data['utc_start'],"%Y-%m-%d %H:%M:%S").isoformat()

#    @pre_load
#    def just_print_out_the_data(self,data):
#        pprint(data)
#        print("ParkingSchema.pre_load: type of data = {}".format(type(data)))

    #@pre_load
    #def process_na_zone(self, data):
    #    zone = data.get('zone')
    #    if zone.lower() in ['n/a', 'osc']:
    #        data['zone'] = None
    #    return data

    #@post_load
    #def combine_date_and_time(self, in_data):
    #    in_data['arrest_datetime'] = (datetime(
    #        in_data['arrest_date'].year, in_data['arrest_date'].month,
    #        in_data['arrest_date'].day, in_data['arrest_time'].hour,
    #        in_data['arrest_time'].minute, in_data['arrest_time'].second
    #    ))

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def get_package_parameter(site,package_id,parameter=None,API_key=None):
    """Gets a CKAN package parameter. If no parameter is specified, all metadata
    for that package is returned."""
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        if parameter is None:
            return metadata
        else:
            return metadata[parameter]
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))

def find_resource_id(site,package_id,resource_name,API_key=None):
#def get_resource_id_by_resource_name():
    # Get the resource ID given the package ID and resource name.
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

def get_connection_parameters(server, settings_file_path):
    with open(settings_file_path) as f:
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']
    API_key = settings['loader'][server]['ckan_api_key']
    return settings, site, package_id, API_key 

def send_data_to_pipeline(server,settings_file_path,resource_name,schema,list_of_dicts,primary_keys,chunk_size=5000):
    # Taken from github.com/WPRDC/stop-in-the-name-of-data.

    if resource_name is not None:
        specify_resource_by_name = True
    else:
        specify_resource_by_name = False
    if specify_resource_by_name:
        kwargs = {'resource_name': resource_name}
    #else:
        #kwargs = {'resource_id': ''}

    # Synthesize virtual file to send to the FileConnector
    from tempfile import NamedTemporaryFile
    ntf = NamedTemporaryFile()

    # Save the file path
    target = ntf.name
    fields_to_publish = schema().serialize_to_ckan_fields() # These are field names and types together
    print("fields_to_publish = {}".format(fields_to_publish))
    field_names = [f['id'] for f in fields_to_publish]
    write_to_csv(target,list_of_dicts,field_names)

    # Testing temporary named file:
    #ntf.seek(0)
    #with open(target,'r') as g:
    #    print(g.read())

    ntf.seek(0)
    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    #with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.

    settings, site, package_id, API_key = get_connection_parameters(server, settings_file_path)

    update_method = 'upsert'
    if len(primary_keys) == 0:
        update_method = 'insert'

    clear_first = False
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
                                      #start_from_chunk=0, # Unsupported by /home/sds25/wprdc-etl/ version of pipeline.
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

    package_name = get_package_parameter(site,package_id,'title',API_key)

    log = open('uploaded.log', 'w+')

    if specify_resource_by_name:
        print("Data successfully piped to {}/{}.".format(package_name,resource_name))
        success = True
        log.write("Finished upserting {} at {} \n".format(kwargs['resource_name'],datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    else:
        print("Data successfully piped to {}/{}.".format(package_name,kwargs['resource_id']))
        success = True
        log.write("Finished upserting {} at {} \n".format(kwargs['resource_id'],datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    log.close()
    ntf.close()
    assert not os.path.exists(target)

    resource_id = find_resource_id(site,package_id,kwargs['resource_name'],API_key)

    return success


def main():
    upload_in_chunks = True
    server = "testbed"
    resource_id = sys.argv[1]
    filename = None
    if len(sys.argv) > 2:
        filename = sys.argv[2] # Name of the file that contains the data to be uploaded.
    #upload_file_to_CKAN(resource_id,filename) # This functionality would best be reproduced
    #by calling the existing wprdc-etl pipeline library.

############

if __name__ == '__main__':
    main()
