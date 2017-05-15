import sys, os
sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from

import datetime
from marshmallow import fields, pre_dump, pre_load
import pipeline as pl # This comes from the wprdc-etl repository.

from fetch_terminals import pull_terminals

class MetersSchema(pl.BaseSchema):
    #ID,Location,LocationType,Latitude,Longitude,Status,Zone,ParentStructure,OldZone,AllGroups,GUID,Cost per hour,Rate information,Restrictions
    meter_id = fields.String(dump_to='id')
    location = fields.String(dump_to='location')
    location_type = fields.String(dump_to='location_type')
    latitude = fields.String(dump_to='latitude')
    longitude = fields.String(dump_to='longitude')
    status = fields.String(dump_to='status')
    zone = fields.String(dump_to='zone')
    parent_structure = fields.String(dump_to='parent_structure')
    old_zone = fields.String(dump_to='old_zone')
    all_groups = fields.String(dump_to='all_groups') # Should this be JSON?
    guid = fields.String(dump_to='guid')
    cost_per_hour = fields.Float(dump_to='cost_per_hour')
    rate_information = fields.String(dump_to='rate_information')
    restrictions = fields.String(dump_to='restrictions')

    class Meta:
        ordered = True

    @pre_load()
    def format_date(self, data):
        data['date'] = datetime.date(
            int(data['date'][0:4]),
            int(data['date'][4:6]),
            int(data['date'][6:])).isoformat()


yesterday = datetime.date.today() - datetime.timedelta(days=1)

package_id = '530f334b-4d7c-40c5-bf50-ba55645bb8b3' # "Testy" test package
combined_resource_name = 'Payment Points (Combined)'
monthly_resource_name = 'Payment Points - {:02d}/{}'.format(yesterday.month, yesterday.year)
current_resource_name = 'Current Payment Points'


# [ ] Specify key fields


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
#          resource_name=combined_resource_name,
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
#    .schema(MetersSchema) \
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


path = os.path.dirname(os.path.abspath(__file__)) # The filepath of this script.
kwdict = {'return_extra_zones': False,
    'output_to_csv': False,
    'push_to_CKAN': True }

# Current Meters Data
#   primary keys: Meter ID and/or GUID
current_meters_pipeline = pl.Pipeline('current_meters_pipeline', 
                                      'Current Meters Pipeline', log_status=False) \
    .connect(pl.FunctionConnector, 
             target=path+'fetch_terminals.py', 
             function='pull_terminals', 
             kwparameters=kwdict) \ 
    .extract(pl.NoOpExtractor) \
    .schema(MetersSchema) \
    .load(pl.CKANDatastoreLoader, 'ckan',
          fields=MetersSchema().serialize_to_ckan_fields(),
          package_id=package_id,
          resource_name=current_resource_name,
          method='upsert')


current_meters_pipeline.run()
