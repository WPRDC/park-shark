"""This is a stripped-down version of pipelines.py designed just to run a specific archive of old meters data
through the ETL process (because otherwise MessyTables currently stops the whole thing from being uploaded 
and also this is currently the best way to set field types)."""

# Move this file to /Users/drw/WPRDC/datasets/Parking-Data__public before running it.
import sys, json
from pprint import pprint

from pipelines import MetersSchema, move_to_front, check_and_run_pipeline
from marshmallow import fields, pre_dump, pre_load

from parameters.local_parameters import SETTINGS_FILE, base_monthly_meters_archive

from parameters.local_parameters import PATH_TO_PIPELINE
sys.path.insert(0, PATH_TO_PIPELINE)
import pipeline as pl # This comes from the wprdc-etl repository.

server = "meters-etl"
with open(SETTINGS_FILE,'r') as f:
    settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    API_key = settings['loader'][server]['ckan_api_key']
    package_id = settings['loader'][server]['package_id']

csv_path = base_monthly_meters_archive

class ModifiedCumulativeMetersSchema(MetersSchema):
    year_month = fields.String(dump_to='year_month')
    # The year and month for which the meters data was (originally) pulled.
    as_of = fields.DateTime(dump_to='as_of')
    # The datetime when the meters data was (originally) pulled.

cumulative_resource_name = 'Payment Points (Archives)'
schema = ModifiedCumulativeMetersSchema
key_fields = ['id','year_month','guid']
shoving_method = 'upsert' # Here upserting means that every time we run this
# script, we update that month's values with the freshest values. Since we
# are running this script every day, by the end of the month, it should have
# worked many times. Hence, this is a better approach than using the 'insert'
# method (which doesn't like to be run more than once with the same key
# values).

reordered_fields_and_types = move_to_front('year_month',schema().serialize_to_ckan_fields())

print(reordered_fields_and_types)
cumulative_meters_pipeline = pl.Pipeline('modified_cumulative_meters_pipeline',
                                      'Modified Cumulative Meters Pipeline',
                                      settings_file=SETTINGS_FILE,
                                      log_status=False) \
    .connect(pl.FileConnector, csv_path) \
    .extract(pl.CSVExtractor) \
    .schema(schema) \
    .load(pl.CKANDatastoreLoader, server,
          fields=reordered_fields_and_types,
          clear_first=True,
          key_fields=key_fields,
          package_id=package_id,
          resource_name=cumulative_resource_name,
          method=shoving_method)

check_and_run_pipeline(cumulative_meters_pipeline,site,API_key,key_fields,schema,package_id,cumulative_resource_name,shoving_method)
