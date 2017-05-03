import requests
import xmltodict
import pprint

from collections import OrderedDict, defaultdict
import re
from util import to_dict, write_to_csv, value_or_blank, is_a_lot, is_a_virtual_lot, is_a_virtual_zone, corrected_zone_name, centroid_np, char_delimit, all_groups, lot_list, pure_zones_list, numbered_reporting_zones_list, zone_lookup, is_virtual, numbered_zone, censor
import numpy as np

from credentials_file import CALE_API_user, CALE_API_password

from local_parameters import path
# URL for accessing the Purchases listed under August 19, 2016 (seems to be based on UTC time when the purchase was made).
#url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/4/LiveDataExportService.svc/purchases/2016-08-19/2016-08-20'

# Add times after dates to narrow the query to a more precise time range:
#url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/4/LiveDataExportService.svc/purchases/2016-08-19/120000/2016-08-19/130000'
# samples only between 12pm and 1pm.

#r = requests.get(url, auth=(CALE_API_user, CALE_API_password))

# Convert Cale's XML into a Python dictionary
#doc = xmltodict.parse(r.text,encoding = r.encoding)

#f = open("whatever.xml", "wb")
#f.write(r.text)
#f.close()

#with open('whatever.xml') as fd:
#    doc = xmltodict.parse(fd.read())

#doc = xmltodict.parse(,encoding = 'utf-8')

#purchases = doc['Purchases']['Purchase'] # List of parking purchases

# Pretty-print the first entry in the purchases list with this command:
#pprint.pprint(dict(purchases[0].items()))

def pull_terminals(use_cache=False,return_extra_zones=True):
    if use_cache:
        if return_extra_zones:
            return ([u'CMU Study',
         u'East Liberty (On-street only)',
         u'Marathon/CMU',
         u'S. Craig',
         u'Southside Lots',
    #     u'TEST - South Craig - Reporting'
         ],
        {u'CMU Study': ['410 - Oakland 4'],
         u'East Liberty (On-street only)': ['412 - East Liberty'],
         u'Marathon/CMU': ['415 - SS & SSW',
                           '401 - Downtown 1',
                           '404 - Strip Disctrict',
                           '408 - Oakland 2',
                           '407 - Oakland 1',
                           '409 - Oakland 3',
                           '406 - Bloomfield (On-street)',
                           '410 - Oakland 4',
                           '402 - Downtown 2',
                           '422 - Northshore'],
         u'S. Craig': ['409 - Oakland 3'],
         u'Southside Lots': ['344 - 18th & Carson Lot',
                             '345 - 20th & Sidney Lot',
                             '343 - 19th & Carson Lot',
                             '342 - East Carson Lot',
                             '341 - 18th & Sidney Lot'],
    #     u'TEST - South Craig - Reporting': ['411 - Shadyside', '409 - Oakland 3']
         })
        else:
            return None, None

    url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/2/LiveDataExportService.svc/terminals'
    r = requests.get(url, auth=(CALE_API_user, CALE_API_password))

    # Convert Cale's XML into a Python dictionary
    doc = xmltodict.parse(r.text,encoding = r.encoding)
    terminals = doc['Terminals']['Terminal']
    f_terminals = path + "cached_terminals.xml"
    with open(f_terminals,'w') as g:
        g.write(r.text)

    attributes_url = 'http://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/1/LiveDataExportService.svc/customattributes'
    r = requests.get(attributes_url, auth=(CALE_API_user, CALE_API_password))
    doc = xmltodict.parse(r.text,encoding = r.encoding)
    attributes = doc['CustomAttributes']['Data']

    rates, restrictions = {}, {}
    for a in attributes:
        if a['@Attribute'] == 'Rate':
            rates[a['@Guid']] = a['@Value']
        elif a['@Attribute'] == 'Restrictions':
            restrictions[a['@Guid']] = a['@Value']

    points_in_zone = defaultdict(list)
    zone_type = {}
    list_of_dicts = []
    set_of_all_groups = set()
    for k,t in enumerate(terminals):
        new_entry = {}
        new_entry['GUID'] = t['@Guid']
        new_entry['ID'] = t['@Id']
        new_entry['Location'] = t['Location']
        new_entry['Latitude'] = value_or_blank('Latitude',t)
        new_entry['Longitude'] = value_or_blank('Longitude',t)
        #new_entry['Type'] = t['Type']['@Name'] # This is "External PBC" for Virtual
        # Terminals and "CWT" for all others, so it's kind of useless.
        new_entry['Status'] = t['Status']['@Name']
        new_entry['LocationType'] = value_or_blank('LocationType',t,['@Name'])

        # Convert the Location Type to "Lot" if the Parent Terminal Structure
        # ends in "-L". (Using the Parent Terminal Structure also snags
        # virtual terminals).
        if is_a_virtual_lot(t):
            new_entry['LocationType'] = "Virtual Lot"
        elif is_a_lot(t):
            new_entry['LocationType'] = "Lot"
        elif is_a_virtual_zone(t):
            new_entry['LocationType'] = "Virtual Zone"
        new_entry['Zone'] = numbered_zone(t['@Id'])
        #print('{}: ID = {}, new zone = {}'.format(k,t['@Id'],new_entry['Zone']))
        new_entry['AllGroups'] = char_delimit(all_groups(t),'|')

        set_of_all_groups.update(all_groups(t))

        new_entry['ParentStructure'] = t['ParentTerminalStructure']['@Name']
        new_entry['OldZone'] = corrected_zone_name(t)

        if new_entry['LocationType'] not in ['','Virtual Lot','Virtual Zone']:
            zone_type[new_entry['Zone']] = new_entry['LocationType']
            # This takes the most recent LocationType and assumes that it applies
            # to the entire zone. One bogus data point could throw this off,
            # and it would be more robust to keep a list of types and then
            # check that they are all the same before committing to that type.

        if value_or_blank('Latitude',t) != '' and t['ParentTerminalStructure']['@Name'] != 'Z - Inactive/Removed Terminals':
            lat = float(value_or_blank('Latitude',t))
            lon = float(value_or_blank('Longitude',t))
            points_in_zone[new_entry['Zone']].append([lat,lon])

        if t['@Guid'] in rates:
            new_entry['Rate information'] = rates[t['@Guid']]
            costs = re.findall(r'^\$([\d.]+)PH',rates[t['@Guid']])
            if len(costs) == 1:
                single_rate = costs[0]
                new_entry['Cost per hour'] = float(single_rate)
    #            new_entry['Rate'] = "$"+single_rate+"/hour"
    #        else: # Comment out these lines and just make the rate a float.
    #            new_entry['Cost per hour'] = "Multirate"

    #        if len(costs) == 0:
    #            if len(re.findall(r'(MULTIRATE)',r)) != 0:
    #                costs = "MULTIRATE"

        if t['@Guid'] in restrictions:
            new_entry['Restrictions'] = restrictions[t['@Guid']]

        list_of_dicts.append(new_entry)

    #keys = list_of_dicts[0].keys() # This does not set the correct order for the field names.
    keys = ['ID','Location','LocationType','Latitude','Longitude','Status', 'Zone','ParentStructure','OldZone','AllGroups','GUID','Cost per hour',#'Rate',
    'Rate information','Restrictions']

    write_to_csv('payment-points.csv',list_of_dicts,keys)
    ############
    list_of_zone_dicts = []
    zone_info = {}
    for zone in points_in_zone.keys():
        lat_lon_tuple = centroid_np(np.array(points_in_zone[zone]))
        # It would be nicer to throw out the outliers.
        zone_d = {'Zone': zone,
                'Latitude': lat_lon_tuple[0],
                'Longitude': lat_lon_tuple[1],
                'MeterCount': len(points_in_zone[zone]),
                'Type': zone_type[zone]
                }
        list_of_zone_dicts.append(zone_d)
        d_part = dict(zone_d)
        del d_part['Zone']
        zone_info[zone] = d_part
    sorted_zone_dicts = sorted(list_of_zone_dicts, key = lambda x: (x['Type'], x['Zone']))

    sorted_zone_keys = ['Zone','Latitude','Longitude','MeterCount','Type']

    pprint.pprint(zone_info)

    write_to_csv('zone-centroids.csv',sorted_zone_dicts,sorted_zone_keys)
    #print("Here is the list of all groups:")
    #pprint.pprint(set_of_all_groups)

    excluded_zones = ['TEST - South Craig - Reporting']
    excluded_zones = []
    print("Here is the list of all groups not already in lot_list or pure_zones_list or numbered_reporting_zones_list or exclude_zones (or those that start with 'TEST'):")
    maybe_special_zones = set_of_all_groups - set(lot_list) - set(pure_zones_list) - set(numbered_reporting_zones_list) - set(excluded_zones)
    special_zones = censor(maybe_special_zones)
    pprint.pprint(special_zones)

    parent_zones = {}
    for sz in special_zones:
        if sz not in parent_zones:
            parent_zones[sz] = []
        for t in terminals:
            if sz in all_groups(t):
                parent = numbered_zone(t['@Id'])
                if parent not in parent_zones[sz]:
                    parent_zones[sz].append(parent)

    pprint.pprint(parent_zones)

    if return_extra_zones:
        return list(special_zones), parent_zones
    else:
        return list_of_dicts, keys # The data that was previously written to the payment_points.csv file.
############

if __name__ == '__main__':
  pull_terminals()
