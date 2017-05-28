import os, sys
import re
import csv
from collections import OrderedDict
from json import loads, dumps
import json
import operator
import requests
import time
import urllib
try:
    from urlparse import urlparse # Python 2
except:
    from urllib.parse import urlparse # Python 3 renamed urlparse.

import pprint

try:
    import datapusher
except:
    from . import datapusher # Python 3/prime_ckan workaround

import ckanapi
#from ckanapi import ValidationError

import traceback

def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    # obtained from https://code.activestate.com/recipes/577058/
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

def dealias(site,pseudonym):
    # If a resource ID is an alias for the real resource ID, this function will
    # convert the pseudonym into the real resource ID and return it.
    ckan = ckanapi.RemoteCKAN(site)
    aliases = ckan.action.datastore_search(id='_table_metadata',filters={'name': pseudonym})
    resource_id = aliases['records'][0]['alias_of']
    return resource_id


def resource_show(ckan,resource_id):
    # A wrapper around resource_show (which could be expanded to any resource endpoint)
    # that tries the action, and if it fails, tries to dealias the resource ID and tries 
    # the action again.
    try:
        metadata = ckan.action.resource_show(id=resource_id)
    except ckanapi.errors.NotFound:
        # Maybe the resource_id is an alias for the real one.
        real_id = dealias(site,resource_id)
        metadata = ckan.action.resource_show(id=real_id)
    except:
        msg = "{} was not found on that CKAN instance".format(resource_id)
        print(msg)
        raise ckanapi.errors.NotFound(msg)
    
    return metadata

def initialize_datastore(resource_id, ordered_fields, keys=None, settings_file='ckan_settings.json', server='Live'):
    # For a CKAN resource that already exists (identified by resource_id)
    # on a CKAN instance specified by the settings in the JSON
    # settings_file and the specified server, reset the datastore
    # (deleting all stored data) and create a new datastore with the
    # field given by ordered_fields (giving the order, names, and types
    # of the fields). The primary key or keys are given in the keys
    # argument.
    with open(settings_file) as f:
        settings = json.load(f)
    dp = datapusher.Datapusher(settings, server=server)
    dp.delete_datastore(resource_id)
    # Example of ordered_fields and keys:
    #ordered_fields = [{"id": "Zone", "type": "text"}]
    #ordered_fields.append({"id": "Start", "type": "timestamp"})
    #ordered_fields.append({"id": "End", "type": "timestamp"})
    #keys = ["Zone", "UTC Start"]

    call_result = dp.create_datastore(resource_id, ordered_fields, keys=keys)
    print("Datastore creation result: {}".format(call_result))
    return call_result

def get_site(settings,server):
    # From the dictionary obtained from ckan_settings.json,
    # extract the URL for a particular CKAN server and return it.
    url = settings["URLs"][server]["CKAN"]
    scheme = urlparse(url).scheme
    hostname = urlparse(url).hostname
    return "{}://{}".format(scheme,hostname)

def execute_query(URL,query=None,API_key=None):
    # [ ] If the query might result in a response that is too large or
    # too burdensome for the CKAN instance to generate, paginate
    # this process somehow.

    # Information about better ways to handle requests exceptions:
    #http://stackoverflow.com/questions/16511337/correct-way-to-try-except-using-python-requests-module/16511493#16511493


    #To call the CKAN API, post a JSON dictionary in an HTTP POST
    # request to one of CKAN's API URLs.

    payload = {}
    # These attempts to add the Authorization field to the request
    # are failing, making it not yet possible for this function to work
    # with private repositories.
    #if API_key is not None:
    #    payload = {'Authorization': API_key}
    if query is not None:
        payload['sql'] = query
    try:
        #print("payload = {}, URL = {}".format(payload,URL))

        #head['Content-Type'] = 'application/x-www-form-urlencoded'
        #in_dict = urllib.quote(json.dumps(in_dict))
        #r = requests.post(url, data=in_dict, headers=head)

        #payload = urllib.quote(json.dumps(payload))

        r = requests.post(URL, payload)
    except requests.exceptions.Timeout:
        # Maybe set up for a retry, or continue in a retry loop
        r = requests.post(URL, payload)
    except requests.exceptions.TooManyRedirects:
        # Tell the user their URL was bad and try a different one
        print("This URL keeps redirecting. Maybe you should edit it.")
    except requests.exceptions.RequestException as e:
        # catastrophic error. bail.
        print(e)
        sys.exit(1)
    return r

def pull_and_verify_data(URL, site, failures=0):
    success = False
    try:
        r = execute_query(URL)
        result = r.json()["result"]
        records = result["records"]
        # You can just iterate through using the _links results in the
        # API response:
        #    "_links": {
        #  "start": "/api/action/datastore_search?limit=5&resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1",
        #  "next": "/api/action/datastore_search?offset=5&limit=5&resource_id=5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1"
        list_of_fields_dicts = result['fields']
        all_fields = [d['id'] for d in list_of_fields_dicts]
        if r.status_code != 200:
            failures += 1
        else:
            URL = site + result["_links"]["next"]
            success = True
    except:
        records = None
        all_fields = None
        #raise ValueError("Unable to obtain data from CKAN instance.")
    # Information about better ways to handle requests exceptions:
    #http://stackoverflow.com/questions/16511337/correct-way-to-try-except-using-python-requests-module/16511493#16511493

    return records, all_fields, URL, success

def get_number_of_rows(site,resource_id,API_key=None):
# This is pretty similar to get_fields and DRYer code might take
# advantage of that.

# On other/later versions of CKAN it would make sense to use
# the datastore_info API endpoint here, but that endpoint is
# broken on WPRDC.org.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=1) # The limit
        # must be greater than zero for this query to get the 'total' field to appear in
        # the API response.
        count = results_dict['total']
    except:
        return None

    return count

def get_fields(site,resource_id,API_key=None):
    # In principle, it should be possible to do this using the datastore_info 
    # endpoint instead and taking the 'schema' part of the result.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
        fields = [d['id'] for d in schema]
    except:
        return None

    return fields

def get_schema(site,resource_id,API_key=None):
    # In principle, it should be possible to do this using the datastore_info 
    # endpoint instead and taking the 'schema' part of the result.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
    except:
        return None

    return schema

def get_metadata(site,resource_id,API_key=None):
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = resource_show(ckan,resource_id)
    except:
        return None

    return metadata

def get_package_parameter(site,package_id,parameter,API_key=None):
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        desired_string = metadata[parameter]
        #print("The parameter {} for this package is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))
        
    return desired_string

def get_resource_parameter(site,resource_id,parameter,API_key=None):
    # Some resource parameters you can fetch with this function are
    # 'cache_last_updated', 'package_id', 'webstore_last_updated',
    # 'datastore_active', 'id', 'size', 'state', 'hash',
    # 'description', 'format', 'last_modified', 'url_type',
    # 'mimetype', 'cache_url', 'name', 'created', 'url',
    # 'webstore_url', 'mimetype_inner', 'position',
    # 'revision_id', 'resource_type'
    # Note that 'size' does not seem to be defined for tabular
    # data on WPRDC.org. (It's not the number of rows in the resource.)
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = resource_show(ckan,resource_id)
        desired_string = metadata[parameter]

        #print("The parameter {} for this resource is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain resource parameter '{}' for resource with ID {}".format(parameter,resource_id))

    return desired_string

def get_resource_name(site,resource_id,API_key=None):
    return get_resource_parameter(site,resource_id,'name',API_key)

def get_package_name_from_resource_id(site,resource_id,API_key=None):
    p_id = get_resource_parameter(site,resource_id,'package_id',API_key)
    return get_package_parameter(site,p_id,'title',API_key)

def find_resource_id(site,package_id,resource_name,API_key=None):
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

def query_resource(site,query,API_key=None):
    # Use the datastore_search_sql API endpoint to query a CKAN resource.
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search_sql(sql=query)
    # A typical response is a dictionary like this
    #{u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'_full_text', u'type': u'tsvector'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'records': [{u'_full_text': u"'0001b00010000000':1 '11':2 '13585.47':3",
    #               u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_full_text': u"'0001c00058000000':3 '2':2 '7827.64':1",
    #               u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_full_text': u"'0001c01661006700':3 '1':1 '3233.59':2",
    #               u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}]
    # u'sql': u'SELECT * FROM "d1e80180-5b2e-4dab-8ec3-be621628649e" LIMIT 3'}
    data = response['records']
    return data

def get_resource_data(site,resource_id,API_key=None,count=50,offset=0):
    # Use the datastore_search API endpoint to get <count> records from
    # a CKAN resource
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset)
    # A typical response is a dictionary like this
    #{u'_links': {u'next': u'/api/action/datastore_search?offset=3',
    #             u'start': u'/api/action/datastore_search'},
    # u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'limit': 3,
    # u'records': [{u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}],
    # u'resource_id': u'd1e80180-5b2e-4dab-8ec3-be621628649e',
    # u'total': 88232}
    data = response['records']
    return data

def get_all_records(site,resource_id,API_key=None,chunk_size=5000):
    all_records = []
    failures = 0
    k = 0
    offset = 0 # offset is almost k*chunk_size (but not quite)
    row_count = get_number_of_rows(site,resource_id,API_key)
    if row_count == 0: # or if the datastore is not active
       print("No data found in the datastore.")
       success = False
    while len(all_records) < row_count and failures < 5:
        time.sleep(0.1)
        try:
            records = get_resource_data(site,resource_id,API_key,chunk_size,offset)
            if records is not None:
                all_records += records
            failures = 0
            offset += chunk_size
        except:
            failures += 1

        # If the number of rows is a moving target, incorporate
        # this step:
        #row_count = get_number_of_rows(site,resource_id,API_key)
        k += 1
        print("{} iterations, {} failures, {} records, {} total records".format(k,failures,len(records),len(all_records)))

    return all_records


def get_resource(site,resource_id,chunk_size=500):
    # Phasing this one out to be replaced by get_all_records
    # since the latter supports private repositories.
    limit = chunk_size
    URL_template = "{}/api/3/action/datastore_search?resource_id={}&limit={}"

    URL = URL_template.format(site, resource_id, limit)

    all_records = []

    failures = 0
    records = [None, None, "Boojum"]
    k = 0
    while len(records) > 0 and failures < 5:
        time.sleep(0.1)
        records, fields, next_URL, success = pull_and_verify_data(URL,site,failures)
        if success:
            if records is not None:
                all_records += records
            URL = next_URL
            failures = 0
        else:
            failures += 1
        k += 1
        print("{} iterations, {} failures, {} records, {} total records".format(k,failures,len(records),len(all_records)))

    return all_records, fields, success


def retrieve_new_data(self):
    URL = "{}/api/3/action/datastore_search_sql".format(self.site)

    #query = "SELECT {} FROM \"{}\" WHERE \"{}\" > '{}';".format(self.field, self.resource_id, self.index_field, self.last_index_checked)
    query = "SELECT \"{}\",\"{}\" FROM \"{}\" WHERE \"{}\" > {};".format(self.field, self.index_field, self.resource_id, self.index_field, int(self.last_index_checked)-1)
    #query = "SELECT {} FROM \"{}\";".format(self.field, self.resource_id)

    print(query)

    r = execute_query(URL,query)

    print(r.status_code)
    if r.status_code != 200:
        r = requests.get(URL, {'sql': query})
    if r.status_code == 200:
        records = json.loads(r.text)["result"]["records"]
        last_index_checked = records[-1][self.index_field]
        return records, last_index_checked, datetime.now()
    else:
        raise ValueError("Unable to obtain data from CKAN instance.")
        # Information about better ways to handle requests exceptions:
        #http://stackoverflow.com/questions/16511337/correct-way-to-try-except-using-python-requests-module/16511493#16511493

def elicit_primary_key(site,resource_id,API_key):
    # This function uses a workaround to determine the primary keys of a resource
    # from a CKAN API call. 

    # Note that it has not been tested on primary-key-less resources and this represents
    # kind of a problem because, if used on such a resource, it will succeed in adding 
    # the duplicate row to the table.
    primary_keys = None
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        # Get the very last row of the resource.
        row_count = get_number_of_rows(site,resource_id,API_key)
        records = get_resource_data(site,resource_id,API_key,count=1)
        first_row = records[0]
        # Try to insert it into the database
        del first_row["_id"]
        results = ckan.action.datastore_upsert(resource_id=resource_id, method='insert', 
            records=[first_row], force=True)
        pprint.pprint(results)
    except ckanapi.ValidationError as exception:
        orig = exception.error_dict['info']['orig']
        print(orig)
        details = orig.split('\n')[1]

        string_of_keys = re.sub(r'\)=\(.*', '', re.sub(r'DETAIL:  Key \(','',details))
        primary_keys = string_of_keys.split(', ')

        # The above works if the keys are lowercased and have no spaces.
        # Otherwise, it seems that they are returned like this:
        # [u'"Key Number 1"', u'"Another Key that is Primary"']
        # so some extra processing is required.
        revised_primary_keys = []
        for pk in primary_keys: 
            if pk[0] == u'"' and pk[-1] == u'"':
                pk = pk[1:-1]
            revised_primary_keys.append(pk)
        primary_keys = revised_primary_keys
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Error: {}".format(exc_type))
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('!!! ' + line for line in lines))
    else:
        new_row_count = get_number_of_rows(site,resource_id,API_key)
        records = get_resource_data(site,resource_id,API_key,count=1,offset=new_row_count-1)
        last_row = records[0]
        value_of_id = int(last_row['_id'])
        msg = "This function was run on a resource that has no primary key"
        msg += " and therefore added a duplicate row that was never intended to be added."
        msg += " The correct thing to do here is to delete"  
        msg += " row with _id = {}".format(value_of_id)
        print(msg)
        
        if new_row_count == row_count+1:
            # Delete the last row (if it matches the one that was just added):
            del last_row['_id']
            if last_row == first_row:
                print("Deleting the last row...")
                deleted = delete_row_from_resource(site,resource_id,value_of_id,API_key)
            else:
                print("The last row doesn't match the added row, even though the number of rows",
                    "has increased")

        primary_keys = []

    return primary_keys

def set_resource_parameters_to_values(site,resource_id,parameters,new_values,API_key):
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        original_values = [get_resource_parameter(site,resource_id,p,API_key) for p in parameters]
        payload = {}
        payload['id'] = resource_id
        for parameter,new_value in zip(parameters,new_values):
            payload[parameter] = new_value
        #For example,
        #   results = ckan.action.resource_patch(id=resource_id, url='#', url_type='')
        results = ckan.action.resource_patch(**payload)
        print(results)
        print("Changed the parameters {} from {} to {} on resource {}".format(parameters, original_values, new_values, resource_id))
        success = True
    except:
        success = False
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Error: {}".format(exc_type))
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('!!! ' + line for line in lines))

    return success

# Comment out this function since it's not working as intended yet.
#def recast_field(site,resource_id,field,new_type,API_key):
#    # Experiments suggest that this function can be used to convert an integer field
#    # to a string field (text), but that if you try to convert back, the string 
#    # values in that field do not get converted back to integers (though the field
#    # itself does appear to have type numeric.
#
#    # Perhaps a proper recasting function would need to iterate through the data
#    # and fix the types (or possibly download everything, reset the datastore, and
#    # then upload it all with the proper types).
#
#    schema = get_schema(site,resource_id,API_key)
#    if schema[0]['id'] == '_id':
#        new_schema = schema[1:]
#        for d in new_schema:
#            if d['id'] == field:
#                d['type'] == new_type
#        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
#        outcome = ckan.action.datastore_create(resource_id=resource_id,fields=new_schema, force=True)
#        print("Verifying that the schema has changed...")
#        final_schema = get_schema(site,resource_id,API_key)
#        return final_schema
#    else:
#        print("Unable to eliminate the _id field from this schema")
#        return schema

def delete_row_from_resource(site,resource_id,_id,API_key):
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        response = ckan.action.datastore_delete(id=resource_id, filters={"_id":_id}, force=True)
        success = True
    except:
        success = False
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Error: {}".format(exc_type))
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('!!! ' + line for line in lines))
    return success


def disable_downloading(site,resource_id,API_key=None):
    # Under CKAN, if the user tries to download a huge table,
    # CKAN tries to generate a CSV (storing all the data in
    # memory), exhausts the server's memory supplies, and
    # causes the server to crash.

    # As part of a temporary workaround, we use this function
    # to change the URL parameter from a link that triggers a
    # dump from the datastore to a "#" symbol.
    return set_resource_parameters_to_values(site,resource_id,['url','url_type'],['#',''],API_key)

def to_dict(input_ordered_dict):
    return loads(dumps(input_ordered_dict))

def value_or_blank(key,d,subfields=[]):
    if key in d:
        if d[key] is None:
            return ''
        elif len(subfields) == 0:
            return d[key]
        else:
            return value_or_blank(subfields[0],d[key],subfields[1:])
    else:
        return ''

def write_or_append_to_csv(filename,list_of_dicts,keys):
    if not os.path.isfile(filename):
        with open(filename, 'wb') as g:
            g.write(','.join(keys)+'\n')
    with open(filename, 'ab') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        #dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)


def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'wb') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def unique_values(xs,field):
    return { x[field] if field in x else None for x in to_dict(xs) }

def char_delimit(xs,ch):
    return(ch.join(xs))

def sort_dict(d):
    return sorted(d.items(), key=operator.itemgetter(1))