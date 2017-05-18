# This is an older version of gadgets.py from utility-belt/
import os, sys
import re
import csv
from collections import OrderedDict
from json import loads, dumps
import operator
import requests
import time
import urllib
try:
    from urlparse import urlparse # Python 2
except:
    from urllib.parse import urlparse # Python 3

import ckanapi

import traceback

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

def get_fields(site,resource_id,API_key=None):
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id,limit=0)
        schema = results_dict['fields']
        fields = [d['id'] for d in schema]
    except:
        return None, False

    return fields, True

def get_package_parameter(site,package_id,parameter,API_key):
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
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        desired_string = metadata[parameter]
        #print("The parameter {} for this package is {}".format(parameter,metadata[parameter]))
        success = True
    except:
        success = False

    return desired_string, success

def get_resource_parameter(site,resource_id,parameter,API_key):
    # Some resource parameters you can fetch with this function are
    # 'cache_last_updated', 'package_id', 'webstore_last_updated',
    # 'datastore_active', 'id', 'size', 'state', 'hash',
    # 'description', 'format', 'last_modified', 'url_type',
    # 'mimetype', 'cache_url', 'name', 'created', 'url',
    # 'webstore_url', 'mimetype_inner', 'position',
    # 'revision_id', 'resource_type'
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.resource_show(id=resource_id)
        desired_string = metadata[parameter]
        #print("The parameter {} for this resource is {}".format(parameter,metadata[parameter]))
        success = True
    except:
        success = False

    return desired_string, success

def get_resource_name(site,resource_id,API_key):
    return get_resource_parameter(site,resource_id,'name',API_key)

def get_package_name_from_resource_id(site,resource_id,API_key):
    p_id, success = get_resource_parameter(site,resource_id,'package_id',API_key)
    if success:
        return get_package_parameter(site,p_id,'title',API_key)
    else:
        return None, False

def get_resource(site,resource_id,chunk_size=500):
    limit = chunk_size
    URL_template = "{}/api/3/action/datastore_search?resource_id={}&limit={}"

    URL = URL_template.format(site, resource_id, limit)

    all_records = []

    failures = 0
    records = [None, None, "Boojum"]
    k = 0
    while len(records) > 0 and failures < 5:
        time.sleep(0.3)
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


def set_resource_parameters_to_values(site,resource_id,parameters,new_values,API_key):
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        original_values = [get_resource_parameter(site,resource_id,p,API_key)[0] for p in parameters]
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
