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

import datetime
from marshmallow import fields, pre_dump, pre_load

import sys, os, json
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

#    @pre_load()
#    def format_date(self, data):
#        data['date'] = datetime.date(
#            int(data['date'][0:4]),
#            int(data['date'][4:6]),
#            int(data['date'][6:])).isoformat()

class MonthlyMetersSchema(MetersSchema):
    as_of = fields.DateTime(dump_to='as_of') # The datetime when the meters data was pulled.

    @pre_load()
    def add_datetime(self):
        data['as_of'] = datetime.now().isoformat()


yesterday = datetime.date.today() - datetime.timedelta(days=1)

package_id = '530f334b-4d7c-40c5-bf50-ba55645bb8b3' # "Testy" test package
archive_resource_name = 'Payment Points (Monthly Archives)'
monthly_resource_name = 'Payment Points - {:02d}/{}'.format(yesterday.month, yesterday.year)
current_resource_name = 'Current Payment Points'



# Combined Data 
#   primary keys: Meter ID and/or GUID and the year/month 
#    The year/month field needs  to come from.... a pre_load function
#    Though maybe a logged datestamp would be better (more standard 
#    and more accurate).


#aggregated_meters_pipeline = pl.Pipeline('aggregated_meters_pipeline', 'Aggregated Meters History Pipeline', log_status=True) \
#    .connect(pl.SFTPConnector, target, config_string='sftp.county_sftp', encoding='utf-8') \
#    .extract(pl.CSVExtractor, firstline_headers=True) \
#    .schema(MetersSchema) \
#    .load(pl.CKANDatastoreLoader, 'ckan',
#          fields=MetersSchema().serialize_to_ckan_fields(),
#          package_id=package_id,
#          resource_name=archive_resource_name,
#          method='upsert')






# Monthly Data
#   primary keys: Meter ID and/or GUID
#monthly_meters_pipeline = pl.Pipeline('monthly_meters_pipeline', 
#                                    'Monthly Meters Pipeline', log_status=False) \
#    .connect(pl.FunctionConnector, 
#             target=path+'fetch_terminals.py', 
#             function='pull_terminals', 
#             kwparameters=kwdict) \ 
#    .extract(pl.NoOpExtractor) \
#    .schema(MonthlyMetersSchema) \
#    .load(pl.CKANDatastoreLoader, 'ckan',
#          fields=MetersSchema().serialize_to_ckan_fields(),
#          package_id=package_id,
#          resource_name=monthly_resource_name,
#          method='upsert')






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


# [ ] Maybe try running the pipeline on a CSV file first and
# then switch to pulling from the function
#target = 'jail_census_data/acj_daily_population_{}.csv'.format(yesterday.strftime('%Y%m%d'))

# Run the pull_terminals function to get the tabular data on parking
# meter parameters, and also output that data to a CSV file.

unfixed_list_of_dicts, unfixed_keys = pull_terminals(output_to_csv=True)

csv_path = csv_file_path()
print(csv_path)

# Load CKAN parameters to get the package name.
with open(local_config.SETTINGS_FILE,'r') as f:
    settings = json.load(f)
    site = settings['loader']['ckan']['ckan_root_url']
    API_key = settings['loader']['ckan']['ckan_api_key']

package_name, _ = get_package_parameter(site,package_id,'name',API_key)

kwdict = {'return_extra_zones': False,
    'output_to_csv': False,
    'push_to_CKAN': True }

# Current Meters Data
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


fields_and_types = MetersSchema().serialize_to_ckan_fields()
fieldnames = [ft['id'] for ft in fields_and_types]
print("The fields and types are {}".format(MetersSchema().serialize_to_ckan_fields()))
print("The fieldnames are {}".format(fieldnames))

key_fields = ['id']#['id','guid']

for kf in key_fields:
    if kf not in fieldnames:
        raise RuntimeError("key field {} is not in the list of fields ({})".format(kf, fieldnames))

current_meters_pipeline = pl.Pipeline('current_meters_pipeline', 
                                      'Current Meters Pipeline', 
                                      settings_file=local_config.SETTINGS_FILE,
                                      log_status=False) \
    .connect(pl.FileConnector, csv_path) \
    .extract(pl.CSVExtractor) \
    .schema(MetersSchema) \
    .load(pl.CKANDatastoreLoader, 'ckan',
          fields=MetersSchema().serialize_to_ckan_fields(),
          key_fields=key_fields,
          package_id=package_id,
          resource_name=current_resource_name,
          method='upsert')

pipe_output = current_meters_pipeline.run()

os.remove(csv_path) # In any event, delete the CSV file.

if hasattr(pipe_output,'upload_complete') and pipe_output.upload_complete:
    print("Data successfully piped to {}/{}.".format(package_name,current_resource_name))
    #return True
else:
    print("Data not successfully piped to {}/{}.".format(package_name,current_resource_name))
    #return False


