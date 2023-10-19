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

from parameters.local_parameters import PATH_TO_PIPELINE
sys.path.insert(0, PATH_TO_PIPELINE)
import pipeline as pl # This comes from the wprdc-etl repository.

from fetch_terminals import pull_terminals, csv_file_path

from notify import send_to_slack
from parameters.local_parameters import SETTINGS_FILE # This is yet another workaround.

def find_resource_id(site,package_id,resource_name,API_key=None):
    # Get the resource ID given the package ID and resource name.
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

class MetersSchema(pl.BaseSchema):
    #ID,Location,LocationType,Latitude,Longitude,Status,Zone,ParentStructure,OldZone,AllGroups,GUID,Cost per hour,Rate information,Restrictions
    id = fields.String(dump_to='id')
    location = fields.String(dump_to='location',allow_none=True)
    location_type = fields.String(dump_to='location_type',allow_none=True)
    latitude = fields.String(dump_to='latitude',allow_none=True)
    longitude = fields.String(dump_to='longitude',allow_none=True)
    status = fields.String(dump_to='status',allow_none=True)
    zone = fields.String(dump_to='zone',allow_none=True)
    #parentstructure = fields.String(dump_to='parent_structure',allow_none=True)
    #oldzone = fields.String(dump_to='old_zone',allow_none=True)
    all_groups = fields.String(dump_to='all_groups',allow_none=True) # Should this be JSON?
    guid = fields.String(dump_to='guid')
    #cost_per_hour = fields.Float(dump_to='cost_per_hour',allow_none=True)
    #rate_information = fields.String(dump_to='rate_information',allow_none=True)
    #restrictions = fields.String(dump_to='restrictions',allow_none=True)
    #created_utc = fields.DateTime(dump_to='created_utc',allow_none=True) # created_utc is not as good a match for the Install Date field from the mast list as in_service_utc.
    #active_utc = fields.DateTime(dump_to='active_utc',allow_none=True) # This works because Marshmallow can take an ISO-formatted datetime and turn it into a DateTime field.
    in_service_utc = fields.DateTime(dump_to='in_service_utc',allow_none=True) # Best matches the Install Date field of the master list.
    #inactive_utc = fields.DateTime(dump_to='inactive_utc',allow_none=True)
    #removed_utc = fields.DateTime(dump_to='removed_utc',allow_none=True) # removed_utc is not complete enough in the CALE API. 405024-BUTLER3602 was removed (with
    # a removal date given by the master list, but no removal date is present in the API.

    class Meta:
        ordered = True

class ExtendedMetersSchema(MetersSchema):
    rate = fields.String(allow_none=True)
    max_hours = fields.String(allow_none=True)
    hours = fields.String(allow_none=True)
    restrictions = fields.String(allow_none=True)
    special_events = fields.String(allow_none=True)
    rate_as_of = fields.Date(allow_none=False)

class CumulativeMetersSchema(MetersSchema):
    year_month = fields.String(dump_only=True,dump_to='year_month',default=datetime.datetime.now().strftime("%Y-%m")) 
    # The year and month for which the meters data was pulled.
    as_of = fields.DateTime(dump_only=True,dump_to='as_of',default=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
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

def main(keep_archive, mute_alerts):
    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    monthly_resource_name = 'Payment Points - {:02d}/{}'.format(yesterday.month, yesterday.year)
    current_resource_name = 'Current Payment Points'

    #server = "official-terminals" # Note that this is the package ID just for terminal information.
    server = "meters-etl" # The new repository, intended for publication

    # Combined Data 
    #   primary keys: Meter ID and/or GUID and the year/month 

    #pull_terminals(*args, **kwargs):
        # This function accepts keyword arguments use_cache (to
        # set whether cached data is used, for offline testing),
        # return_extra_zones (to set whether the sampling and parent
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
    unfixed_list_of_dicts, unfixed_keys = pull_terminals(output_to_csv=True, return_extra_zones=False, mute_alerts=mute_alerts)

    csv_path = csv_file_path()

    # Load CKAN parameters to get the package name.
    with open(SETTINGS_FILE,'r') as f:
        settings = json.load(f)
        site = settings['loader'][server]['ckan_root_url']
        API_key = settings['loader'][server]['ckan_api_key']
        package_id = settings['loader'][server]['package_id']

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

    # Set clear_first based on whether the resource is already there.
    resource_id = find_resource_id(site,package_id,current_resource_name,API_key)
    clear_first = (resource_id is not None)
    # This will clear the Current Meters resource so that no rogue meters
    # get left in that table.
    print("clear_first = {}".format(clear_first))

    schema = MetersSchema
    key_fields = ['id','guid']#['id']
    shoving_method = 'upsert'

    current_meters_pipeline = pl.Pipeline('current_meters_pipeline', 
                                          'Current Meters Pipeline', 
                                          settings_file=SETTINGS_FILE,
                                          log_status=False) \
        .connect(pl.FileConnector, csv_path) \
        .extract(pl.CSVExtractor) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=schema().serialize_to_ckan_fields(),
              key_fields=key_fields,
              clear_first=clear_first,
              package_id=package_id,
              resource_name=current_resource_name,
              method=shoving_method)

    check_and_run_pipeline(current_meters_pipeline,site,API_key,key_fields,schema,package_id,current_resource_name,shoving_method)

    # Will this script overwrite an existing CSV file (or just append to it)?
    ########### CUMULATIVE METERS ARCHIVE #############################
    cumulative_resource_name = 'Payment Points (Archives)'
    clear_first = False # Setting this variable here is just a precaution
    # since clear_first is not specified in the pipeline call below.
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

    check_and_run_pipeline(cumulative_meters_pipeline,site,API_key,key_fields,schema,package_id,cumulative_resource_name,shoving_method)

    ##################################################################
    if not keep_archive:
        os.remove(csv_path) # In any event, delete the CSV file.

if __name__ == '__main__':
    keep_archive = False
    mute_alerts = False
    if len(sys.argv) > 1:
        to_store = sys.argv[1]
        if to_store in ['keep', 'store', 'archive']:
            keep_archive = True
        if 'mute' in sys.argv[1:] or 'mute_alerts' in sys.argv[1:]:
            mute_alerts = True
    try:
        main(keep_archive=keep_archive, mute_alerts=mute_alerts)
    except:
        e = sys.exc_info()[0]
        print("Error: {} : ".format(e))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_msg = ''.join('!! ' + line for line in lines)
        print(traceback_msg)  # Log it or whatever here
        msg = "pipelines.py ran into an error: {}.\nHere's the traceback:\n{}".format(e,traceback_msg)
        if not mute_alerts:
            send_to_slack(msg,username='Leaky Pipe',channel='@david',icon=':droplet:')

