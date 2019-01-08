import os
import requests
import xmltodict
from datetime import datetime
from dateutil import parser
from pprint import pprint
import copy

from collections import OrderedDict, defaultdict
import re
from util.util import to_dict, write_to_csv, value_or_blank, is_a_lot, is_a_virtual_lot, is_a_virtual_zone, corrected_zone_name, char_delimit, all_groups, lot_list, pure_zones_list, numbered_reporting_zones_list, zone_lookup, is_virtual, numbered_zone, censor, get_more_minizones

from parameters.credentials_file import CALE_API_user, CALE_API_password

from parameters.local_parameters import path

calculate_zone_centroids = False
if calculate_zone_centroids:
    import numpy as np
    from util.hm_util import centroid_np

# URL for accessing the Purchases listed under August 19, 2016 (seems to
# be based on UTC time when the purchase was made).
#url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/4/LiveDataExportService.svc/purchases/2016-08-19/2016-08-20'

# Add times after dates to narrow the query to a more precise time range:
#url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/4/LiveDataExportService.svc/purchases/2016-08-19/120000/2016-08-19/130000'
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
#pprint(dict(purchases[0].items()))

def csv_file_path():
    path = os.path.dirname(os.path.abspath(__file__)) # The filepath of this script.
    filename = 'meters_etl/meters-{}.csv'.format(datetime.today().strftime("%Y-%m"))
    csv_path = path+'/'+filename
    return csv_path

def pull_terminals(*args, **kwargs):
    # This function accepts keyword arguments use_cache (to
    # set whether cached data is used, for offline testing),
    # return_extra_zones (to set whether the sampling and parent
    # zones are returned rather than the table of terminals),
    # and push_to_CKAN and output_to_csv (to control those output
    # channels).

    use_cache = kwargs.get('use_cache',False)
    return_extra_zones = kwargs.get('return_extra_zones',True)
    output_to_csv = kwargs.get('output_to_csv',False)
    push_to_CKAN = kwargs.get('push_to_CKAN',True)

    # [ ] Note that cached mode could break if a new parking zone is
    # created and is therefore a) not in the cached_terminals.xml
    # file (used by the get_terminals function) and b) not entered
    # into the hard-coded extra zones below.
    #
    # Indeed, no thought has been given yet to incorporating new
    # zones into corresponding extra zones.
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
         }, ['426 - Hill District'], ['Hill District'], {'426': '426 - Hill District'})
        else:
            return None, None

    url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/2/LiveDataExportService.svc/terminals'
    r = requests.get(url, auth=(CALE_API_user, CALE_API_password))

    # Convert Cale's XML into a Python dictionary
    doc = xmltodict.parse(r.text,encoding = r.encoding)
    terminals = doc['Terminals']['Terminal']
    f_terminals = path + "cached_terminals.xml"
    with open(f_terminals,'w+') as g:
        g.write(r.text)

    attributes_url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/1/LiveDataExportService.svc/customattributes'
    r = requests.get(attributes_url, auth=(CALE_API_user, CALE_API_password))
    doc = xmltodict.parse(r.text,encoding = r.encoding)
    attributes = doc['CustomAttributes']['Data']

    rates, restrictions, install_dates = {}, {}, {}
    for a in attributes:
        if a['@Attribute'] == 'Rate':
            rates[a['@Guid']] = a['@Value']
        elif a['@Attribute'] == 'Restrictions':
            restrictions[a['@Guid']] = a['@Value']
        elif a['@Attribute'] == 'Installation Date':
            install_dates[a['@Guid']] = a['@Value']
        # Note that some look like this 7/14/2013 and some are zero-padded, like this: 07/11/2013.

    points_in_zone = defaultdict(list)
    zone_type = {}
    list_of_dicts = []
    set_of_all_groups = set()
    uncharted_numbered_zones = []
    uncharted_enforcement_zones = []
    group_lookup_addendum = {}

    ids_to_ignore = ['Friendship Ave RPP']
    for k,t in enumerate(terminals):
        new_entry = {}
        new_entry['GUID'] = t['@Guid']
        new_entry['ID'] = t['@Id']
        new_entry['description'] = value_or_blank('@Description',t)
        new_entry['Location'] = t['Location']
        new_entry['Latitude'] = value_or_blank('Latitude',t)
        new_entry['Longitude'] = value_or_blank('Longitude',t)
        #new_entry['Type'] = t['Type']['@Name'] # This is "External PBC" for Virtual
        # Terminals and "CWT" for all others, so it's kind of useless.
        new_entry['Status'] = t['Status']['@Name']
        new_entry['LocationType'] = value_or_blank('LocationType',t,['@Name'])

        new_entry['created_utc'] = value_or_blank('DateCreatedUtc',t)
        new_entry['active_utc'] = value_or_blank('DateActiveUtc',t)
        new_entry['in_service_utc'] = value_or_blank('DateInServiceUtc',t)
        new_entry['inactive_utc'] = value_or_blank('DateInactiveUtc',t)
        new_entry['removed_utc'] = value_or_blank('DateRemovedUtc',t)


        if 'TariffPackages' in t:
            tariffs = t['TariffPackages']['TariffPackage']
            if type(tariffs) != list:
                tariffs = [tariffs]
            programs, program_descriptions = [], []
            for tariff in tariffs:
                if tariff['@Name'] != 'Test':
                    programs.append(tariff['@Name'])
                    program_descriptions.append(value_or_blank('@Description',tariff))
            new_entry['TariffPrograms'] = '|'.join(programs)
            new_entry['TariffDescriptions'] = '|'.join(program_descriptions)

        # Convert the Location Type to "Lot" if the Parent Terminal Structure
        # ends in "-L". (Using the Parent Terminal Structure also snags
        # virtual terminals).
        if is_a_virtual_lot(t):
            new_entry['LocationType'] = "Virtual Lot"
        elif is_a_lot(t):
            new_entry['LocationType'] = "Lot"
        elif is_a_virtual_zone(t):
            new_entry['LocationType'] = "Virtual Zone"
        new_entry['Zone'], new_numbered_zone, new_enforcement_zone  = numbered_zone(t['@Id'],t)
        if new_numbered_zone is not None:
            uncharted_numbered_zones.append(new_numbered_zone)
            if new_enforcement_zone is not None:
                uncharted_enforcement_zones.append(new_enforcement_zone)
            if t['@Id'][:3] == "PBP":
                # We found a new uncharted virtual zone, so let's create
                # the corresponding group_lookup key-value pair.

                # (Note that this could be done for each virtual zone and
                # then double-checked to make sure that the correspondences
                # are unambiguous to supply a freshly generated group_lookup
                # dict to group_by_code.)
                code = t['@Id'][3:]
                group_lookup_addendum[code] = new_numbered_zone
                print("      FOUND A NEW group_lookup PAIR: {}".format(group_lookup_addendum))

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
        if t['@Guid'] in install_dates:
            try:
                installed = parser.parse(install_dates[t['@Guid']])
                new_entry['InstallationDate'] = installed.date().isoformat() # This is generally consistent with the DateInServiceUtc field, but can come a day or two after that.
            except ValueError:
                print("Unable to parse install date ({}) for terminal with GUID {}".format(install_dates[t['@Guid']], t['@Guid']))
                new_entry['InstallationDate'] = ""

        if t['@Id'] not in ids_to_ignore:
            list_of_dicts.append(new_entry)

    #dkeys = list(list_of_dicts[0].keys()) # This does not set the correct order for the field names.
    dkeys = ['ID','Location','LocationType','Latitude','Longitude','Status', 'Zone','ParentStructure','OldZone','AllGroups','GUID','Cost per hour',#'Rate',
    'Rate information','Restrictions','description',
    'InstallationDate','TariffPrograms','TariffDescriptions',
    'created_utc','active_utc','in_service_utc','inactive_utc','removed_utc']

    if output_to_csv:
        csv_path = csv_file_path()
        # Rename 'ID' field to avoid possible problem stemming from Marshmallow's lowercasing of fields and
        # Python's treating 'id' as a reserved term.
        #csv_keys = list(dkeys)
        #old_key = 'ID'
        #new_key  = 'meter_id'
        #csv_keys[csv_keys.index(old_key)] = new_key
        #csv_list_of_dicts = list(list_of_dicts)
        #for d in csv_list_of_dicts:
        #    d[new_key] = copy.copy(d[old_key])
        #    del d[old_key]

        #write_to_csv(csv_path,csv_list_of_dicts,csv_keys)
        # Actually, this seems to not be strictly necessary.
        write_to_csv(csv_path,list_of_dicts,dkeys)
    ############
    list_of_zone_dicts = []
    zone_info = {}

    if calculate_zone_centroids:
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

        pprint(zone_info)

        if output_to_csv:
            write_to_csv('zone-centroids.csv',sorted_zone_dicts,sorted_zone_keys)


    excluded_zones = ['TEST - South Craig - Reporting']
    excluded_zones = []
    print("Here is the list of all groups not already in lot_list or pure_zones_list or numbered_reporting_zones_list or exclude_zones (or those that start with 'TEST') or newly discovered uncharted zones:")
    maybe_sampling_zones = set_of_all_groups - set(lot_list) - set(pure_zones_list) - set(numbered_reporting_zones_list) - set(excluded_zones) - set(uncharted_numbered_zones) - set(uncharted_enforcement_zones)
    more_minizones = get_more_minizones()
    for mz in more_minizones:
        maybe_sampling_zones.add(mz)
    sampling_zones = censor(maybe_sampling_zones)
    pprint(sampling_zones)

    parent_zones = {}
    for sz in sampling_zones:
        if sz not in parent_zones:
            parent_zones[sz] = []
        for t in terminals:
            if sz in all_groups(t):
                parent, _, _ = numbered_zone(t['@Id']) # Now that group_lookup_addendum has been determined, should it be used here? It seems like it should.
                if parent not in parent_zones[sz]:
                    parent_zones[sz].append(parent)

    pprint(parent_zones)

    if return_extra_zones:
        return list(sampling_zones), parent_zones, uncharted_numbered_zones, uncharted_enforcement_zones, group_lookup_addendum
    else:
        return list_of_dicts, dkeys # The data that was previously written to the payment_points.csv file.
############

# At present, the default when running this script (or the pull_terminals function) is not to output the results to
# a CSV file.
if __name__ == '__main__':
    pull_terminals(output_to_csv=True)
