import sys
try:
    sys.path.insert(0, '~/WPRDC') # A path that we need to import code from
    from utility_belt.gadgets import get_resource_parameter, get_package_parameter
except:
    try:
        sys.path.insert(0, '~/bin') # Office computer location
        from utility_belt.gadgets import get_resource_parameter, get_package_parameter
    except:
        print("Trying Option 3")
        from prime_ckan.gadgets import get_resource_parameter, get_package_parameter

import os, json
import datetime
from marshmallow import fields, pre_dump, pre_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl # This comes from the wprdc-etl repository.

from fetch_terminals import pull_terminals, csv_file_path

from prime_ckan import local_config # This is yet another workaround.


class MetersSchema(pl.BaseSchema):
    #ID,Location,LocationType,Latitude,Longitude,Status,Zone,ParentStructure,OldZone,AllGroups,GUID,Cost per hour,Rate information,Restrictions
    id = fields.String(dump_to='id')
    location = fields.String(dump_to='location',allow_none=True)
    locationtype = fields.String(dump_to='location_type',allow_none=True)
    latitude = fields.String(dump_to='latitude',allow_none=True)
    longitude = fields.String(dump_to='longitude',allow_none=True)
    status = fields.String(dump_to='status',allow_none=True)
    zone = fields.String(dump_to='zone',allow_none=True)
    parentstructure = fields.String(dump_to='parent_structure',allow_none=True)
    oldzone = fields.String(dump_to='old_zone',allow_none=True)
    allgroups = fields.String(dump_to='all_groups',allow_none=True) # Should this be JSON?
    guid = fields.String(dump_to='guid')
    cost_per_hour = fields.Float(dump_to='cost_per_hour',allow_none=True)
    rate_information = fields.String(dump_to='rate_information',allow_none=True)
    restrictions = fields.String(dump_to='restrictions',allow_none=True)

    class Meta:
        ordered = True

class CumulativeMetersSchema(MetersSchema):
    year_month = fields.String(dump_only=True,dump_to='year_month',default=datetime.datetime.now().strftime("%Y-%m")) 
    # The year and month for which the meters data was pulled.
    as_of = fields.DateTime(dump_only=True,dump_to='as_of',default=datetime.datetime.now().isoformat())
    # The datetime when the meters data was pulled.

#    @pre_load()
#    def add_year_month(self):
#        data['year_month'] = datetime.datetime.now().strftime("%Y-%m")

def check_and_run_pipeline(pipe,key_fields,schema,package_id,resource_name,upsert_method):

    fields_and_types = schema().serialize_to_ckan_fields()
    fieldnames = [ft['id'] for ft in fields_and_types]
    print("The fields and types are {}".format(fields_and_types))
    print("The fieldnames are {}".format(fieldnames))

    for kf in key_fields:
        if kf not in fieldnames:
            raise RuntimeError("key field {} is not in the list of fields ({})".format(kf, fieldnames))

    pipe_output = pipe.run()
    package_name = get_package_parameter(site,package_id,'name',API_key)

    if hasattr(pipe_output,'upload_complete') and pipe_output.upload_complete:
        print("Data successfully piped to {}/{} via {}.".format(package_name,current_resource_name,upsert_method))
        return True
    else:
        print("Data not successfully piped to {}/{}.".format(package_name,current_resource_name))
        return False

def move_to_front(f,f_ts):
    # Move the dict with the indicated fieldname to the beginning of the list
    # to reorder the fields without copying the whole schema over to a new schema.
    popped = [d for d in f_ts if d['id'] == f]
    remaining_fs = [d for d in f_ts if d['id'] != f]
    return popped + remaining_fs


yesterday = datetime.date.today() - datetime.timedelta(days=1)

#package_id = '530f334b-4d7c-40c5-bf50-ba55645bb8b3' # "Testy" test package
package_id = '4ec3583a-b6e8-4a4e-bfb2-7609bee33cea' # Test Meters package under PPA organization

monthly_resource_name = 'Payment Points - {:02d}/{}'.format(yesterday.month, yesterday.year)
current_resource_name = 'Current Payment Points'


# Combined Data 
#   primary keys: Meter ID and/or GUID and the year/month 

#pull_terminals(*args, **kwargs):
    # This function accepts keyword arguments use_cache (to
    # set whether cached data is used, for offline testing),
    # return_extra_zones (to set whether the ad hoc and parent
    # zones are returned rather than the table of terminals),
    # and push_to_CKAN and output_to_csv (to control those output
    # channels).

    #use_cache = kwargs.get('use_cache',False)
    #return_extra_zones = kwargs.get('return_extra_zones',True)
    #output_to_csv = kwargs.get('output_to_csv',False)
    #push_to_CKAN = kwargs.get('push_to_CKAN',True)


# FunctionConnector specifications:
#          target: a valid filepath to a file with Python code in it
#            function: the function within the file to call (defaults
#                to `main`)
#            parameters: list of (unnamed) arguments to send to the
#                function [essentially these would be args so
#                this might need to be complemented by the argument
#                kwparameters]



# Run the pull_terminals function to get the tabular data on parking
# meter parameters, and also output that data to a CSV file.
unfixed_list_of_dicts, unfixed_keys = pull_terminals(output_to_csv=True,return_extra_zones=False)

csv_path = csv_file_path()

# Load CKAN parameters to get the package name.
with open(local_config.SETTINGS_FILE,'r') as f:
    settings = json.load(f)
    site = settings['loader']['ckan']['ckan_root_url']
    API_key = settings['loader']['ckan']['ckan_api_key']


kwdict = {'return_extra_zones': False,
    'output_to_csv': False,
    'push_to_CKAN': True }


########## CURRENT METERS ####################################
# Current Meters Data # Maybe eventually switch to this approach,
# once the FunctionConnector and NoOpExtractor are working.
#   primary keys: Meter ID and/or GUID
#current_meters_pipeline = pl.Pipeline('current_meters_pipeline', 
#                                      'Current Meters Pipeline', log_status=False) \
#    .connect(pl.FunctionConnector, 
#             target=path+'fetch_terminals.py', 
#             function='pull_terminals', 
#             kwparameters=kwdict) \
#    .extract(pl.NoOpExtractor) \
#    .schema(MetersSchema) \
#    .load(pl.CKANDatastoreLoader, 'ckan',
#          fields=MetersSchema().serialize_to_ckan_fields(),
#          package_id=package_id,
#          resource_name=current_resource_name,
#          method='upsert')


schema = MetersSchema
key_fields = ['id','guid']#['id']
shoving_method = 'upsert'

current_meters_pipeline = pl.Pipeline('current_meters_pipeline', 
                                      'Current Meters Pipeline', 
                                      settings_file=local_config.SETTINGS_FILE,                                     
                                      log_status=False) \
    .connect(pl.FileConnector, csv_path) \
    .extract(pl.CSVExtractor) \
    .schema(schema) \
    .load(pl.CKANDatastoreLoader, 'ckan',
          fields=schema().serialize_to_ckan_fields(),
          key_fields=key_fields,
          package_id=package_id,
          resource_name=current_resource_name,
          method=shoving_method)

check_and_run_pipeline(current_meters_pipeline,key_fields,schema,package_id,current_resource_name,shoving_method)

# Will this script overwrite an existing CSV file (or just append to it)?
########### CUMULATIVE METERS ARCHIVE #############################
cumulative_resource_name = 'Payment Points (Monthly Archives)'

schema = CumulativeMetersSchema
key_fields = ['id','year_month','guid'] 
shoving_method = 'upsert' # Here upserting means that every time we run this
# script, we update that month's values with the freshest values. Since we 
# are running this script every day, by the end of the month, it should have 
# worked many times. Hence, this is a better approach than using the 'insert'
# method (which doesn't like to be run more than once with the same key 
# values).

reordered_fields_and_types = move_to_front('year_month',schema().serialize_to_ckan_fields())

print(reordered_fields_and_types)
cumulative_meters_pipeline = pl.Pipeline('cumulative_meters_pipeline', 
                                      'Cumulative Meters Pipeline', 
                                      settings_file=local_config.SETTINGS_FILE,                                     
                                      log_status=False) \
    .connect(pl.FileConnector, csv_path) \
    .extract(pl.CSVExtractor) \
    .schema(schema) \
    .load(pl.CKANDatastoreLoader, 'ckan',
          fields=reordered_fields_and_types,
          key_fields=key_fields,
          package_id=package_id,
          resource_name=cumulative_resource_name,
          method=shoving_method)

check_and_run_pipeline(cumulative_meters_pipeline,key_fields,schema,package_id,cumulative_resource_name,shoving_method)

##################################################################
os.remove(csv_path) # In any event, delete the CSV file. 

