from carto.auth import APIKeyAuthClient
from carto.datasets import DatasetManager

from pprint import pprint
import re, csv

import os
import tempfile
from contextlib import contextmanager

@contextmanager
def tempinput(list_of_records,keys):
    temp = tempfile.NamedTemporaryFile(delete=False,mode='w+t',prefix="estimated_occupancy_by_zone",suffix=".csv")
    #temp.write(data)
    dict_writer = csv.DictWriter(temp, keys, extrasaction='ignore', lineterminator='\n')
    dict_writer.writeheader()
    dict_writer.writerows(list_of_records)
    temp.close()
    try:
        yield temp.name
    finally:
        os.unlink(temp.name)

def authorize_carto():
    from prime_ckan.carto_credentials import ORGANIZATION, USERNAME, API_KEY
    BASE_URL = "https://{organization}.carto.com/user/{user}/". \
        format(organization=ORGANIZATION,
                       user=USERNAME)
    auth_client = APIKeyAuthClient(api_key=API_KEY, base_url=BASE_URL, organization=ORGANIZATION)
    return auth_client

def send_file_to_carto(filepath):
    auth_client = authorize_carto()
    dataset_manager = DatasetManager(auth_client)
    datasets = dataset_manager.all()

    filename = filepath.split('/')[-1] 
    carto_name = re.sub('.csv$','',filename)
    carto_name = re.sub('[\.\s]','_',carto_name)
    carto_name = carto_name.lower()
    print("Filename transformed to Carto-style table name is '{}'".format(carto_name))

    dataset_names = [d.name for d in datasets]
    for d in datasets:
        if d.name == carto_name:
            d.delete()

    dataset = dataset_manager.create(filepath)
    print("Carto dataset with name '{}' created.".format(carto_name))

# On moving from files to buffers (acting as virtual files):
    # "It should be noted that should you need to interface with code that needs 
    # filenames, then: If all your legacy code can take is a filename, then a 
    # StringIO instance is not the way to go. Use the tempfile module to generate 
    # a temporary filename instead:"
    #   https://stackoverflow.com/questions/11892623/python-stringio-and-compatibility-with-with-statement-context-manager/11892712#11892712
    #   "A StringIO instance is an open file already. The open command, on the 
    #   other hand, only takes filenames, to return an open file. A StringIO 
    #   instance is not suitable as a filename."


def update_map(inferred_occupancy_dict,zonelist,zone_info):
    for zone in sorted(zonelist):
        if zone not in inferred_occupancy_dict:
            inferred_occupancy_dict[zone] = 0

    list_of_records = [{'zone': k, 'inferred_occupancy': v} for k,v in inferred_occupancy_dict.items()]
    list_of_records.sort(key = lambda x: x['zone'])
    for record in list_of_records:
        zone = record['zone']
        if zone in zone_info:
           record['percent_occupied'] = round(10000*(record['inferred_occupancy'] + 0.0)/zone_info[zone]['spaces'])/100.0

    keys = ['zone','inferred_occupancy','percent_occupied']

    pprint(list_of_records)
    with tempinput(list_of_records,keys) as tempfilename:
        #processFile(tempfilename) 
        send_file_to_carto(tempfilename)
#send_file_to_carto('/Users/drw/test_carto.3.csv')
