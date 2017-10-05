from carto.auth import APIKeyAuthClient
from carto.datasets import DatasetManager

from pprint import pprint
from datetime import datetime
import re, csv, os

import itertools

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

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


def format_cell(r):
    if r is None:
        return ''
    percent = " {:>5.1f}%".format(r['percent_occupied']) if 'percent_occupied' in r else ''
    formatted = "{:31} {:4d}{:7}".format(r['zone'],r['inferred_occupancy'],percent)
    return formatted

def update_map(inferred_occupancy_dict,zonelist,zone_info):
    for zone in sorted(zonelist):
        if zone not in inferred_occupancy_dict:
            inferred_occupancy_dict[zone] = 0

    list_of_records = [{'zone': k, 'inferred_occupancy': v} for k,v in inferred_occupancy_dict.items()]
    list_of_records.sort(key = lambda x: x['zone'])
    lot_data = []
    street_data = []
    for record in list_of_records:
        zone = record['zone']
        if zone in zone_info:
            zone_data = zone_info[zone]
            if 'spaces' in zone_data:
                record['percent_occupied'] = round(10000*(record['inferred_occupancy'] + 0.0)/zone_data['spaces'])/100.0
                record['spaces'] = zone_data['spaces']
            # For now, add the centroids of the zones (where available).
            if 'latitude' in zone_data and 'longitude' in zone_data:
                record['latitude'] = zone_data['latitude']
                record['longitude'] = zone_data['longitude']
        if zone[0] == '3':
            lot_data.append(record)
        else:
            street_data.append(record)

    keys = ['zone','inferred_occupancy','percent_occupied','spaces','latitude','longitude']

    for l,s in itertools.zip_longest(lot_data, street_data, fillvalue=None):
        print("{} | {}".format(format_cell(l), format_cell(s)))

    filename = "estimated_occupancy_by_zone-{}.csv".format(datetime.now().strftime("%Y%m%d-%H%M"))
    with open(filename,'w') as f:
        dict_writer = csv.DictWriter(f, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_records)
    send_file_to_carto(filename)
    os.unlink(filename)

# [ ] Check how these occupancy metrics are affected by the meter/mobile payment dichotomy
#     and the limitations of imperfect session inference.
    # The meter/mobile payment dichotomy means that meter durations can't be compared to
    # mobile durations (since the latter may extend into hours that are free to park).
        # To correct this, either meter durations would have to be extended by 
        # careful use of timestamps or mobile durations would have to be truncated
        # by using the operation hours of the meter AND the timestamps of the 
        # payments.


#send_file_to_carto('/Users/drw/test_carto.3.csv')
