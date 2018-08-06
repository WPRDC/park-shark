import sys
try:
    sys.path.insert(0, '~/WPRDC') # A path that we need to import code from
    from utility_belt.gadgets import get_resource_parameter, get_package_parameter
except:
    try:
        sys.path.insert(0, '~/bin') # Office computer location
        from pipe.gadgets import get_resource_parameter, get_package_parameter
    except:
        print("Trying Option 3")
        from pipe.gadgets import get_resource_parameter, get_package_parameter

import os, json, traceback
import datetime
from marshmallow import fields, pre_dump, pre_load
from pprint import pprint

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl # This comes from the wprdc-etl repository.

from notify import send_to_slack
from parameters.local_parameters import SETTINGS_FILE # This is yet another workaround.

def find_resource_id(site,package_id,resource_name,API_key=None):
    # Get the resource ID given the package ID and resource name.
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

def build_csv_path():
    path = os.path.dirname(os.path.abspath(__file__)) # The filepath of this script.
    filename = 'spaces_etl/lease-counts.csv'
    csv_path = path+'/'+filename
    return csv_path

class LeasesSchema(pl.BaseSchema):
    #ID,Location,LocationType,Latitude,Longitude,Status,Zone,ParentStructure,OldZone,AllGroups,GUID,Cost per hour,Rate information,Restrictions
    zone = fields.String()
    as_of = fields.String(dump_to='as_of')
    active_leases = fields.Integer(allow_none=True)

    class Meta:
        ordered = True

    #@pre_load()
    #def cast_fields(self,data): # Marshmallow takes care of the kind of casting shown below.
    #    if 'rate' in data and data['rate'] is not None:
    #        data['rate'] = float(data['rate'])
    #    if 'spaces' in data and data['spaces'] is not None:
    #        data['spaces'] = int(data['spaces'])
    #    if 'meters' in data and data['meters'] is not None:
    #        data['meters'] = int(data['meters'])

#class CumulativeMetersSchema(MetersSchema):
#    year_month = fields.String(dump_only=True,dump_to='year_month',default=datetime.datetime.now().strftime("%Y-%m")) 
    # The year and month for which the meters data was pulled.
    #as_of = fields.DateTime(dump_only=True,dump_to='as_of',default=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    # The datetime when the meters data was pulled.

#    @pre_load()
#    def add_year_month(self):
#        data['year_month'] = datetime.datetime.now().strftime("%Y-%m")

def check_and_run_pipeline(pipe,site,API_key,key_fields,schema,package_id,resource_name,upsert_method):

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
        print("Data successfully piped to {}/{} via {}.".format(package_name,resource_name,upsert_method))
        return True
    else:
        print("Data not successfully piped to {}/{}.".format(package_name,resource_name))
        return False

def move_to_front(f,f_ts):
    # Move the dict with the indicated fieldname to the beginning of the list
    # to reorder the fields without copying the whole schema over to a new schema.
    popped = [d for d in f_ts if d['id'] == f]
    remaining_fs = [d for d in f_ts if d['id'] != f]
    return popped + remaining_fs

def main(keep_archive):
    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    #monthly_resource_name = 'Payment Points - {:02d}/{}'.format(yesterday.month, yesterday.year)
    #current_resource_name = 'Latest Space Counts and Rates'

    server = "spaces-etl" # Note that this the package ID associated with this "server"
    # is just for zone and lot information.

    # Combined Data 
    #   primary keys: Meter ID and/or GUID and the year/month 

    csv_path = build_csv_path()

    # Load CKAN parameters to get the package name.
    with open(SETTINGS_FILE,'r') as f:
        settings = json.load(f)
        site = settings['loader'][server]['ckan_root_url']
        API_key = settings['loader'][server]['ckan_api_key']
        package_id = settings['loader'][server]['package_id']

#    kwdict = {'return_extra_zones': False,
#        'output_to_csv': False,
#        'push_to_CKAN': True }

# A lot of stuff is being left here, because this script is starting to have the
# makings of a good generic data-piping script (based on its check_and_run_pipeline
# functionality for verifying that keys are fields and its reordering, inserting,
# removal of fields to work between cumulative and non-cumulative schemas).
#
#    ########## CURRENT METERS ####################################
#    # Current Meters Data # Maybe eventually switch to this approach,
#    # once the FunctionConnector and NoOpExtractor are working.
#    #   primary keys: Meter ID and/or GUID
#    #current_meters_pipeline = pl.Pipeline('current_meters_pipeline', 
#    #                                      'Current Meters Pipeline', log_status=False) \
#    #    .connect(pl.FunctionConnector, 
#    #             target=path+'fetch_terminals.py', 
#    #             function='pull_terminals', 
#    #             kwparameters=kwdict) \
#    #    .extract(pl.NoOpExtractor) \
#    #    .schema(MetersSchema) \
#    #    .load(pl.CKANDatastoreLoader, 'ckan',
#    #          fields=MetersSchema().serialize_to_ckan_fields(),
#    #          package_id=package_id,
#    #          resource_name=current_resource_name,
#    #          method='upsert')
#
#    # Set clear_first based on whether the resource is already there.
#    resource_id = find_resource_id(site,package_id,current_resource_name,API_key)
#    clear_first = (resource_id is not None)
#    # This will clear the Current Meters resource so that no rogue meters
#    # get left in that table.
#    print("clear_first = {}".format(clear_first))
#
#    schema = SpacesSchema
#    key_fields = ['id','guid']#['id']
#    shoving_method = 'upsert'
#
#    current__pipeline = pl.Pipeline('current_meters_pipeline', 
#                                          'Current Meters Pipeline', 
#                                          settings_file=SETTINGS_FILE,
#                                          log_status=False) \
#        .connect(pl.FileConnector, csv_path) \
#        .extract(pl.CSVExtractor) \
#        .schema(schema) \
#        .load(pl.CKANDatastoreLoader, server,
#              fields=schema().serialize_to_ckan_fields(),
#              key_fields=key_fields,
#              clear_first=clear_first,
#              package_id=package_id,
#              resource_name=current_resource_name,
#              method=shoving_method)
#
#    check_and_run_pipeline(current_meters_pipeline,site,API_key,key_fields,schema,package_id,current_resource_name,shoving_method)
#
#    # Will this script overwrite an existing CSV file (or just append to it)?
    ########### CUMULATIVE METERS ARCHIVE #############################
    cumulative_resource_name = 'Lease Counts by Lot'
    clear_first = False # Setting this variable here is just a precaution
    # since clear_first is not specified in the pipeline call below.
    schema = LeasesSchema
    key_fields = ['as_of', 'zone'] 
    shoving_method = 'upsert' # Here upserting means that every time we run this
    # script, we update that month's values with the freshest values. Since we 
    # are running this script every day, by the end of the month, it should have 
    # worked many times. Hence, this is a better approach than using the 'insert'
    # method (which doesn't like to be run more than once with the same key 
    # values).

    reordered_fields_and_types = schema().serialize_to_ckan_fields()

    print(reordered_fields_and_types)
    cumulative_leases_pipeline = pl.Pipeline('cumulative_leases_pipeline', 
                                          'Cumulative Leases Pipeline', 
                                          settings_file=SETTINGS_FILE,
                                          log_status=False) \
        .connect(pl.FileConnector, csv_path) \
        .extract(pl.CSVExtractor) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=reordered_fields_and_types,
              key_fields=key_fields,
              package_id=package_id,
              resource_name=cumulative_resource_name,
              method=shoving_method)

    check_and_run_pipeline(cumulative_leases_pipeline,site,API_key,key_fields,schema,package_id,cumulative_resource_name,shoving_method)

    ##################################################################
    #if not keep_archive: # Governs whether a local archive of pulled data is kept. (Not useful for lease counts.)
    #    os.remove(csv_path) # In any event, delete the CSV file.

if __name__ == '__main__':
    keep_archive = True
    if len(sys.argv) > 1:
        to_store = sys.argv[1]
        if to_store in ['delete','skip','burninate']:
            keep_archive = False
    try:
        main(keep_archive=keep_archive)
    except:
        e = sys.exc_info()[0]
        print("Error: {} : ".format(e))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_msg = ''.join('!! ' + line for line in lines)
        print(traceback_msg)  # Log it or whatever here
        msg = "pipelines.py ran into an error: {}.\nHere's the traceback:\n{}".format(e,traceback_msg)
        #mute_alerts = kwargs.get('mute_alerts',False)
        mute_alerts = False
        if not mute_alerts:
            send_to_slack(msg,username='Leaky Pipe',channel='@david',icon=':droplet:')

