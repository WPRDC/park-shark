import os
import re
import csv
from collections import OrderedDict
from json import loads, dumps
from pprint import pprint
import operator
import requests
import xmltodict
import time
from datetime import datetime

from parameters.credentials_file import CALE_API_user, CALE_API_password
from notify import send_to_slack

lot_list = ['18-CARSO-L', '18-SIDNE-L', '19-CARSO-L', '20-SIDNE-L', '42-BUTLE-L', '5224BUTL-L', 'ANSL-BEA-L', 'ASTE-WAR-L', 'BEAC-BAR-L', 'BEECHVIE-L', 'BROOKLIN-L', 'BROW-SAN-L', 'CENT-CRA-L', 'DOUG-PHI-L', 'EASTCARS-L', 'EASTOHIO-L', 'EVA-BEAT-L', 'FORB-MUR-L', 'FORB-SHA-L', 'FRIE-CED-L', 'HOME-ZEN-L', 'IVY-BELL-L', 'JCC-L', 'MAIN-ALE-L', 'OBSERHIL-L', 'PENNC.NW-L', 'SHER-HAR-L', 'SHER-KIR-L', 'SHILOH-L', 'TAME-BEA-L', 'TAYLOR-L', 'WALT-WAR-L'] # Harvard-Beatty-Inferred-L is missing from this list.

other_zones_list = ['ALLENTOWN', 'BAKERY-SQ', 'BEECHVIEW', 'BLOOMFIELD', 'BROOKLINE', 'CARRICK', 'DOWNTOWN1', 'DOWNTOWN2', 'EASTLIB', 'HILL-DIST', 'KNOXVILLE', 'LAWRENCEV', 'MELONPARK', 'MT.WASH', 'NORTHSHORE', 'NORTHSIDE', 'OAKLAND1', 'OAKLAND2', 'OAKLAND3', 'OAKLAND4', 'SOUTHSIDE', 'STRIPDIST', 'TECHNOLOGY', 'WEST END', 'Z - Inactive/Removed Terminals']

designated_minizones = ['SHADYSIDE1', 'SHADYSIDE2', 'SQ.HILL1', 'SQ.HILL2', 'UPTOWN1', 'UPTOWN2', 'W CIRC DR', 'Hill District']

# The reporting zones of the form '123 - Description'.
numbered_reporting_zones_list = ['301 - Sheridan Harvard Lot',
     '302 - Sheridan Kirkwood Lot',
     '304 - Tamello Beatty Lot',
     '307 - Eva Beatty Lot',
     '308 - Harvard Beatty Lot',
     '311 - Ansley Beatty Lot',
     '314 - Penn Circle NW Lot',
     '321 - Beacon Bartlett Lot',
     '322 - Forbes Shady Lot',
     '323 - Douglas Phillips Lot',
     '324 - Forbes Murray Lot',
     '325 - JCC/Forbes Lot',
     '328 - Ivy Bellefonte Lot',
     '329 - Centre Craig',
     '331 - Homewood Zenith Lot',
     '334 - Taylor Street Lot',
     '335 - Friendship Cedarville Lot',
     '337 - 52nd & Butler Lot',
     '338 - 42nd & Butler Lot',
     '341 - 18th & Sidney Lot',
     '342 - East Carson Lot',
     '343 - 19th & Carson Lot',
     '344 - 18th & Carson Lot',
     '345 - 20th & Sidney Lot',
     '351 - Brownsville & Sandkey Lot',
     '354 - Walter/Warrington Lot',
     '355 - Asteroid Warrington Lot',
     '357 - Shiloh Street Lot',
     '361 - Brookline Lot',
     '363 - Beechview Lot',
     '369 - Main/Alexander Lot',
     '371 - East Ohio Street Lot',
     '375 - Oberservatory Hill Lot',
     '401 - Downtown 1',
     '402 - Downtown 2',
     '403 - Uptown',
     '404 - Strip Disctrict',
     '405 - Lawrenceville',
     '406 - Bloomfield (On-street)',
     '407 - Oakland 1',
     '408 - Oakland 2',
     '409 - Oakland 3',
     '410 - Oakland 4',
     '411 - Shadyside',
     '412 - East Liberty',
     '413 - Squirrel Hill',
     '414 - Mellon Park',
     '415 - SS & SSW',
     '416 - Carrick',
     '417 - Allentown',
     '418 - Beechview',
     '419 - Brookline',
     '420 - Mt. Washington',
     '421 - NorthSide',
     '422 - Northshore',
     '423 - West End',
     '424 - Technology Drive',
     '425 - Bakery Sq',
     '426 - Hill District',
     '427 - Knoxville']

zone_lookup = OrderedDict([
    (u'301 - Sheridan Harvard Lot', u'SHER-HAR-L'),
    (u'302 - Sheridan Kirkwood Lot', u'SHER-KIR-L'),
    (u'304 - Tamello Beatty Lot', u'TAME-BEA-L'),
    (u'307 - Eva Beatty Lot', u'EVA-BEAT-L'),
    #(u'308 - Harvard Beatty Lot', u'Z - Inactive/Removed Terminals'),
    (u'311 - Ansley Beatty Lot', u'ANSL-BEA-L'),
    (u'314 - Penn Circle NW Lot', u'PENNC.NW-L'),
    (u'321 - Beacon Bartlett Lot', u'BEAC-BAR-L'),
    (u'322 - Forbes Shady Lot', u'FORB-SHA-L'),
    (u'323 - Douglas Phillips Lot', u'DOUG-PHI-L'),
    (u'324 - Forbes Murray Lot', u'FORB-MUR-L'),
    (u'325 - JCC/Forbes Lot', u'JCC-L'),
    (u'328 - Ivy Bellefonte Lot', u'IVY-BELL-L'),
    (u'331 - Homewood Zenith Lot', u'HOME-ZEN-L'),
    (u'334 - Taylor Street Lot', u'TAYLOR-L'),
    (u'335 - Friendship Cedarville Lot', u'FRIE-CED-L'),
    (u'337 - 52nd & Butler Lot', u'5224BUTL-L'),
    (u'338 - 42nd & Butler Lot', u'42-BUTLE-L'),
    (u'341 - 18th & Sidney Lot', u'18-SIDNE-L'),
    (u'342 - East Carson Lot', u'EASTCARS-L'),
    (u'343 - 19th & Carson Lot', u'19-CARSO-L'),
    (u'344 - 18th & Carson Lot', u'18-CARSO-L'),
    (u'345 - 20th & Sidney Lot', u'20-SIDNE-L'),
    (u'351 - Brownsville & Sandkey Lot', u'BROW-SAN-L'),
    (u'354 - Walter/Warrington Lot', u'WALT-WAR-L'),
    (u'355 - Asteroid Warrington Lot', u'ASTE-WAR-L'),
    (u'357 - Shiloh Street Lot', u'SHILOH-L'),
    (u'361 - Brookline Lot', u'BROOKLIN-L'),
    (u'363 - Beechview Lot', u'BEECHVIE-L'),
    (u'369 - Main/Alexander Lot', u'MAIN-ALE-L'),
    (u'371 - East Ohio Street Lot', u'EASTOHIO-L'),
    (u'375 - Oberservatory Hill Lot', u'OBSERHIL-L'),
    (u'401 - Downtown 1', u'DOWNTOWN1'),
    (u'402 - Downtown 2', u'DOWNTOWN2'),
    #(u'403 - Uptown', u'OAKLAND1'),
    #(u'403 - Uptown', u'UPTOWN1'), # It's ambiguous which of these two
    #(u'403 - Uptown', u'UPTOWN2'), # this group should map to.
    (u'404 - Strip Disctrict', u'STRIPDIST'),
    (u'405 - Lawrenceville', u'LAWRENCEV'),
    (u'406 - Bloomfield (On-street)', u'BLOOMFIELD'),
    (u'407 - Oakland 1', u'OAKLAND1'),
    (u'408 - Oakland 2', u'OAKLAND2'),
    (u'409 - Oakland 3', u'OAKLAND3'),
    (u'410 - Oakland 4', u'OAKLAND4'),
    #(u'410 - Oakland 4', u'W CIRC DR'),
    #(u'411 - Shadyside', u'SHADYSIDE1'),# It's ambiguous which of these two
    #(u'411 - Shadyside', u'SHADYSIDE2'),
    (u'412 - East Liberty', u'EASTLIB'),
    #(u'413 - Squirrel Hill', u'SQ.HILL1'),# It's ambiguous which of these two
    #(u'413 - Squirrel Hill', u'SQ.HILL2'),
    (u'414 - Mellon Park', u'MELONPARK'),
    (u'415 - SS & SSW', u'SOUTHSIDE'),
    (u'416 - Carrick', u'CARRICK'),
    (u'417 - Allentown', u'ALLENTOWN'),
    (u'418 - Beechview', u'BEECHVIEW'),
    (u'419 - Brookline', u'BROOKLINE'),
    (u'420 - Mt. Washington', u'MT.WASH'),
    (u'421 - NorthSide', u'NORTHSIDE'),
    (u'422 - Northshore', u'NORTHSHORE'),
    (u'423 - West End', u'WEST END'),
    (u'425 - Bakery Sq', u'BAKERY-SQ'),
    (u'426 - Hill District', u'HILL-DIST'),
    (u'427 - Knoxville', u'KNOXVILLE')
])

correction_lookup = {'427-Knoxville': '427 - Knoxville'}

def to_dict(input_ordered_dict):
    return loads(dumps(input_ordered_dict))

def add_if_new(xs,x):
    if x in xs:
        return xs
    else:
        return xs.append(x)

def replace_character(s,ch):
    return re.sub(ch,'/',s)

def add_element_to_set_string(x,set_string):
    # To avoid the same name appearing multiple times in such a string, we will
    # regard it as a set and use the add_if_new() function to enfore this property.
    if set_string == '' or set_string is None:
        return x
    if x is None:
        return set_string

    sep = '|' # character for delimiting sets
    xs = set_string.split(sep)
    x = replace_character(x,re.escape(sep))
    add_if_new(xs,x)
    return sep.join(xs)

def describe(t_id,terminals):
    t_ids = [t['@Id'] for t in terminals]
    pprint(to_dict(terminals[t_ids.index(t_id)]))

def round_to_cent(m):
    try:
        return round(m*100)/100.0
    except:
        return None

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

def write_or_append_to_csv(filename,list_of_dicts,keys,actually_overwrite=False):
    if not os.path.isfile(filename) or actually_overwrite:
        with open(filename, 'w') as g: # Does encoding="utf-8" need to be added, as elsewhere?
            g.write(','.join(keys)+'\n')
    with open(filename, 'a') as output_file: 
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        #dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)


def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file: 
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def build_keys(space_aggregation,time_aggregation,split_by_mode):
    """Based on the types of spatial and temporal aggregation (and now whether
    transactions should be split by payment mode into mobile and meter purchases),
    synthesize and return the dictionary keys (used for writing a bunch of
    dictionaries to a CSV file."""
    # Given that these fields appear elsewhere in the process_data.py code, it might 
    # be a good idea to refactor things some more so that there is one source for 
    # these field names.


    # I just added 'utc_start' to sampling_dkeys on April 25, 2017.
    if space_aggregation == 'zone':
        space_keys = ['zone']
    elif space_aggregation == 'meter':
        space_keys = ['Meter ID', 'Meter GUID', 'zone']

    if time_aggregation is None:
        time_keys = ['start', 'end', 'utc_start']
    elif time_aggregation == 'month':
        time_keys = ['Year', 'Month', 'Hour', 'UTC Hour']
    payment_keys = ['transactions', 'payments']
    if not split_by_mode:
        base = payment_keys
    else:
        base = ['meter_transactions', 'meter_payments', 'mobile_transactions', 'mobile_payments']
    linked_keys = ['car_minutes', 'durations'] # These keys are valid when
    # the transactions have been linked and durations can be correctly
    # calculated.
    extras = ['space_count', 'zone_type', 'inferred_occupancy']

    dkeys = space_keys + time_keys + base
    sampling_dkeys = space_keys + ['parent_zone'] + time_keys + base
    occ_dkeys = space_keys + time_keys + payment_keys + linked_keys + extras
    return dkeys, sampling_dkeys, occ_dkeys


def unique_values(xs,field):
    return { x[field] if field in x else None for x in to_dict(xs) }
    # But sometimes we would like to do this
    #{ x['ParentTerminalStructure']['@Name'] if 'ParentTerminalStructure' in x else None for x in to_dict(xs) }
    # This would require subfields to be specifiable.

#>>> unique_values(purchases,'@PurchaseStateName')
#set([u'Completed', u'Ongoing'])

def censored(x):
    return re.search("^TEST",x) is not None

def censor(xs):
    # Eliminate all strings in the list xs that contain forbidden
    # patterns (e.g., those that start with "TEST").
    ys = []
    for x in xs:
        if not censored(x):
            ys.append(x)
    return ys

def is_a_lot(t):
    return t['ParentTerminalStructure']['@Name'][-2:] == "-L"

def is_virtual(t):
    virtual = False
    if 'Location' in t:
        if t['Location'] == 'Virtual Terminal for ParkMobile':
            virtual = True
        if t['Location'] == 'Virtual terminal for ParkMobile':
            virtual = True
    elif '@Id' in t:
        if t['@Id'][:3] == "PBP":
            virtual = True
    else:
        print("Here is a terminal with no @Id field!: {}".format(t))
    return virtual

def is_a_virtual_lot(t):
    return (is_a_lot(t) and is_virtual(t))

def is_a_virtual_zone(t):
    return (is_virtual(t) and not is_a_lot(t) and t['@Id'][:3] == "PBP")

def convert_group_to_zone(t,group):
    if group in zone_lookup:
        return zone_lookup[group]
    else:
        return None

def char_delimit(xs,ch):
    return(ch.join(xs))

def standardize_group_name(name):
    if name in correction_lookup.keys():
        return correction_lookup[name]
    return name

def all_groups(t):
    all_group_names = []
    if 'TerminalGroups' in t:
        if 'TerminalGroup' in t['TerminalGroups']:
            list_of_groups = t['TerminalGroups']['TerminalGroup']
            if type(list_of_groups) == type(OrderedDict()):
                all_group_names += [standardize_group_name(list_of_groups['@TerminalGroupName'])]
            else:
                all_group_names += [standardize_group_name(g['@TerminalGroupName']) for g in list_of_groups]
    if 'ParentTerminalStructure' in t:
        if '@Name' in t['ParentTerminalStructure']:
            pts = t['ParentTerminalStructure']['@Name']
            if pts not in all_group_names:
                all_group_names.append(pts)
    return all_group_names

def is_three_digits(s):
    return (re.match("\d\d\d", s) is not None)

def groups_of_type(group_type,t):
    """A general function that can either retrieve all TerminalGroups associated
    with a meter that are of type 'Reporting' or all of type 'Enforcement',
    depending on the passed group_type parameter."""
    if 'TerminalGroups' in t:
        if 'TerminalGroup' in t['TerminalGroups']:
            group_list = t['TerminalGroups']['TerminalGroup']
            if type(group_list) == type(OrderedDict()):
                group_list = [group_list]
            reporting_group_names = [standardize_group_name(g['@TerminalGroupName']) for g in group_list if g['@TerminalGroupTypeName'] == group_type]
            return reporting_group_names
    return []

def numbered_reporting_groups(t):
    # This function could be refactored to get groups_of_type('Reporting',t)
    # and then filter out the ones that don't start with three digits.
    if 'TerminalGroups' in t:
        if 'TerminalGroup' in t['TerminalGroups']:
            group_list = t['TerminalGroups']['TerminalGroup']
            if type(group_list) == type(OrderedDict()) and group_list['@TerminalGroupTypeName'] == 'Reporting' and is_three_digits(group_list['@TerminalGroupName'][:3]):
                reporting_group_names = [standardize_group_name(group_list['@TerminalGroupName'])]
            else:
                reporting_group_names = [standardize_group_name(g['@TerminalGroupName']) for g in group_list if g['@TerminalGroupTypeName'] == 'Reporting' and is_three_digits(g['@TerminalGroupName'][:3])]
            return reporting_group_names
    return []

def get_more_minizones():
    # "Mini-zones" is another designation for sampling groups, the distinction being that sampling groups/zones
    # had been inferred from zones that were not numbered reporting zones or enforcement zones, while
    # mini-zones are those being explicitly requested.
    #more_minizones = ['SHADYSIDE1', 'SHADYSIDE2', 'SQ.HILL1', 'SQ.HILL2'] # Add these back in since they are used as mini-zones/sampling zones.
    more_minizones = [] # Since we're switching to incorporating ParentTerminalStructure groups into all_groups/AllGroups,
    # we no longer need to add these in... or subtract them from non_sampling_zones in sampling_groups.
    return more_minizones

def sampling_groups(t,uncharted_numbered_zones,uncharted_enforcement_zones):
    # This just returns a list of strings, each of which is the designation for what is believed to be
    # a sampling zone.
    non_sampling_zones = lot_list + other_zones_list + numbered_reporting_zones_list + uncharted_numbered_zones + uncharted_enforcement_zones
    more_minizones = get_more_minizones() # These are zones that were previous considered to be pure zones,
    # but which have been changed to mini-zones.
    improved_non_sampling_zones = [z for z in non_sampling_zones if z not in more_minizones]
    all_group_names = all_groups(t)
    sgs = [name for name in all_group_names if name not in improved_non_sampling_zones]
    return sgs

def group_by_code(code,t=None,group_lookup_addendum={}):
    # Here 'code' is a string which may look like '310' or '402' or
    # (in the worst case scenario) '355-2'.

    # [ ] How about automating the generation of this lookup or (at least)
    # creating a fallback lookup, by taking the list of terminals, crossing
    # over from virtual terminal (PBP402) to the corresponding code and
    # zone (402 and DOWNTOWN2), then iterating through terminals to find
    # a matching reporting zone/group (402 - Downtown 2) that unambiguously
    # works for all known terminals?

    # It's actually pretty feasible and could be done as part of the
    # fetch_terminals code.

    # Maybe then an automated table should be created that looks like this
    #
    # CODE  GROUP               ZONE(S)         Number of physical, active meters
    # 401   401 - Downtown 1    DOWNTOWN1
    #
    # What's missing from this is the space count for that group, the rate,
    # and the lease count.
    # If those numbers could be integrated with this in an ETL process, one
    # table could be used to get all the information needed for the reports.
    # (Maybe a human-readable name would be in there as well.)
    mute_alerts = False

    group_lookup_base = {'301': '301 - Sheridan Harvard Lot',
                        '302': '302 - Sheridan Kirkwood Lot',
                        '304': '304 - Tamello Beatty Lot',
                        '307': '307 - Eva Beatty Lot',
                        '308': '308 - Harvard Beatty Lot',
                        '311': '311 - Ansley Beatty Lot',
                        '314': '314 - Penn Circle NW Lot',
                        '321': '321 - Beacon Bartlett Lot',
                        '322': '322 - Forbes Shady Lot',
                        '323': '323 - Douglas Phillips Lot',
                        '324': '324 - Forbes Murray Lot',
                        '325': '325 - JCC/Forbes Lot',
                        '328': '328 - Ivy Bellefonte Lot',
                        '329': '329 - Centre Craig', # The process for finding this was
                        # to look up PBP329, find its zone, and then find other terminals
                        # with the same zone and skim off the appropriate terminal group.
                        '331': '331 - Homewood Zenith Lot',
                        '334': '334 - Taylor Street Lot',
                        '335': '335 - Friendship Cedarville Lot',
                        '337': '337 - 52nd & Butler Lot',
                        '338': '338 - 42nd & Butler Lot',
                        '341': '341 - 18th & Sidney Lot',
                        '342': '342 - East Carson Lot',
                        '343': '343 - 19th & Carson Lot',
                        '344': '344 - 18th & Carson Lot',
                        '345': '345 - 20th & Sidney Lot',
                        '351': '351 - Brownsville & Sandkey Lot',
                        '354': '354 - Walter/Warrington Lot',
                        '355': '355 - Asteroid Warrington Lot',
                        '357': '357 - Shiloh Street Lot',
                        '361': '361 - Brookline Lot',
                        '363': '363 - Beechview Lot',
                        '369': '369 - Main/Alexander Lot',
                        '371': '371 - East Ohio Street Lot',
                        '375': '375 - Oberservatory Hill Lot',
                        '401': '401 - Downtown 1',
                        '402': '402 - Downtown 2',
                        '403-1': '403 - Uptown',
                        '403-2': '403 - Uptown',
                        '403-3': '403 - Uptown',
                        '404': '404 - Strip Disctrict',
                        '405': '405 - Lawrenceville',
                        '406': '406 - Bloomfield (On-street)',
                        '407': '407 - Oakland 1',
                        '408': '408 - Oakland 2',
                        '409': '409 - Oakland 3',
                        '409-2': '409 - Oakland 3', # This is apparently an inactive virtual zone.
                        '410': '410 - Oakland 4',
                        '410-1': '410 - Oakland 4', #W.CIRC.DR
                        '411-1': '411 - Shadyside', #SHADYSIDE1
                        '411-2': '411 - Shadyside', #SHADYSIDE2
                        '412': '412 - East Liberty',
                        '413-1': '413 - Squirrel Hill', #SQ.HILL1
                        '413-2': '413 - Squirrel Hill', #SQ.HILL2
                        '414': '414 - Mellon Park',
                        '415': '415 - SS & SSW',
                        '416': '416 - Carrick',
                        '355-2': '417 - Allentown', ### Really this is the biggest of anomalies.
                        '417': '417 - Allentown', ## This seems like a
                                                #### totally reasonable interpolation.
                        '418': '418 - Beechview',
                        '419': '419 - Brookline',
                        '420': '420 - Mt. Washington',
                        '421': '421 - NorthSide',
                        '422': '422 - Northshore',
                        '423': '423 - West End',
                        '424': '424 - Technology Drive',
                        '425': '425 - Bakery Sq',
                        '426': '426 - Hill District',
                        '427': '427 - Knoxville'
    }
    group_lookup_base['Fri'] = None # Workaround for oddball 'Friendship Ave RPP' ID.

    group_lookup = {**group_lookup_addendum, **group_lookup_base} # This works in Python 3.5 and higher.
    # Is the group already hard-coded in? If so, return it.
    if code in group_lookup:
        return group_lookup[code], True, None, None

    # Handle cases where something like a 403-4 code has been invented.
    candidate_groups = set()
    # Maybe we can guess the group based on a very similar group code.
    for keycode in group_lookup.keys():
        if keycode[:3] == code[:3]:
            candidate_groups.add(group_lookup[keycode])
    if len(set(candidate_groups)) == 1:
        return list(candidate_groups)[0], True, None, None

    # OK, that didn't work. Let's start looking through the numbered
    # reporting groups for a match that we can identify as a new 
    # numbered reporting group.
    if len(candidate_groups) == 0:
        if t is None:
            msg = 'No group found for code {} and full terminal description {}'.format(code,to_dict(t))
            code = None
        else:
            # An option at this point would be to use other information
            # in the terminal record to infer the group, matching the code
            # to the numbered reporting zone, but this approach is not 
            # futureproof and could break.

            pprint(t)
            nrgs = numbered_reporting_groups(t)
            print(nrgs)
            if len(nrgs) == 0:
                msg = 'No group found for code {} and full terminal description {}'.format(code,to_dict(t))
                code = None
            else:
                # Winnow by the code
                matches = [nrg for nrg in nrgs if nrg[:3] == code[:3]]
                if len(matches) > 1:
                    msg = "Too many numbered zones to discern the true/best one. No group found for code {} and full terminal description {}".format(code,to_dict(t))
                    code = None
                elif len(matches) == 0:
                    msg = "Unable to identify a valid numbered zone. No group found for code {} and full terminal description {}".format(code,to_dict(t))
                    code = None
                else:
                    new_numbered_zone = matches[0]
                    if not mute_alerts:
                        msg = "Found a new group ({}) for code {} and full terminal description {}".format(new_numbered_zone,code,to_dict(t))
                        send_to_slack(msg,username='park-shark',channel='@david',icon=':shark:')

                    new_parent_terminal_structure = t['ParentTerminalStructure']['@Name']
                    t_groups = t['TerminalGroups']['TerminalGroup']
                    if type(t_groups) != list:
                        t_groups = [t_groups]
                    new_enforcement_zones = list({standardize_group_name(g['@TerminalGroupName']) for g in t_groups if g['@TerminalGroupTypeName'] == 'Enforcement'})
                    print("new_enforcement_zones = {}".format(new_enforcement_zones))
                    if len(new_enforcement_zones) == 0:
                        msg = "No enforcement zone found for the new numbered zone {} for code {} and full terminal description {}".format(new_numbered_zone,code,to_dict(t))
                        new_enforcement_zone = None
                    elif len(new_enforcement_zones) > 1:
                        msg = "More than one enforcement zone found for the new numbered zone {} for code {} and full terminal description {}".format(new_numbered_zone,code,to_dict(t))
                        new_enforcement_zone = None
                    else:
                        new_enforcement_zone = new_enforcement_zones[0]

                    if new_enforcement_zone is None and not mute_alerts:
                        send_to_slack(msg,username='park-shark',channel='@david',icon=':shark:')
                    return matches[0], True, new_numbered_zone, new_enforcement_zone

    if code is not None:
        msg = 'Too many candidate groups found for code {}'.format(code)
        code = None

    print(msg)
    if not mute_alerts:
        send_to_slack(msg,username='park-shark',channel='@david',icon=':shark:')
    return None, False, None, None # There's no group matching the code in the list, the match boolean is False, there's no new numbered zone, and no new enforcement zone (because there's no groups to pull this stuff from).

    #return code, False, None, None

def infer_group(t=None,t_id=None,group_lookup_addendum={}):
    # This function only works for virtual groups.
    if t_id is None:
        t_id = t['@Id']
    if t_id[:3] != 'PBP':
        return None
    code = t_id[3:] # Split off the part after 'PBP' (Pay By Phone?).
    # The above code could be factored out into part of an infer_code() function.
    group, matched, new_numbered_zone, new_old_zone = group_by_code(code,t,group_lookup_addendum)
    return group, new_numbered_zone, new_old_zone

def numbered_zone(t_id,t=None,group_lookup_addendum={}):
    # This is the new slimmed-down approach to determining the numbered
    # reporting zone for the meter purely from the meter ID.
    num_zone = None
    if t_id[:3] == 'PBP': #is_virtual(t)
        num_zone, new_num_zone, new_old_zone = infer_group(t,t_id,group_lookup_addendum)
    else:
        try:
            zone_in_ID, matched, new_num_zone, new_old_zone = group_by_code(t_id[:3],t,group_lookup_addendum)
        except:
            if t_id == "B0010X00786401372-STANWX0601":
                # Workaround for weird terminal ID spotted in
                # November 8th, 2012 data.
                zone_in_ID, matched, new_num_zone, new_old_zone = '401 - Downtown 1', True, None, None
            else:
                if not mute_alerts:
                    msg = "Unable to find a numbered zone for terminal ID {} where terminal = {}".format(t_id,t)
                    send_to_slack(msg,username='park-shark',channel='@david',icon=':shark:')
        if matched:
            num_zone = zone_in_ID
    return num_zone, new_num_zone, new_old_zone

def sort_dict(d):
    return sorted(d.items(), key=operator.itemgetter(1))

def zone_name(t):
    # For now, use t['ParentTerminalStructure']['@Name'] as the zone
    # identifier, knowing that this may be wrong for W.CIRC.DR.

    # One other problem with this: Inactive or removed terminals
    # will have a t['ParentTerminalStructure']['@Name']
    # value of "Z - Inactive/Removed Terminals", but if we're
    # using this to report on months-old transactions, we may
    # want to fish out the original zone. This is exactly what
    # the corrected_zone_name function is for, so perhaps this
    # should be more widely used in these scripts.

    # The old way:
    #code = t['ParentTerminalStructure']['@Name'] 

    # This scheme maps virtual terminals that correspond to zones to the correct zones, though it also keeps lots and virtual lots separate:
    # UPTOWN2 			403372-5THAVE1402
    # UPTOWN2 			PBP403-3
    # 42-BUTLE-L 		PBP338
    # 18-CARSO-L 		344339-18CARSN0001

    # Lots and zones may be being regarded as separate, based on existing
    # reports. Still, we might want to combine all lots in a zone with
    # the zone itself to form a superzone to give a better picture of
    # total available parking.
    # Superzone? Zone w/lots? (This still ignores garages.)


    # One problem with this is that the new Hill District meter has an Enforcement Zone name 
    # (Hill District) that differs from its Parent Terminal Structure (HILL-DIST).
    # The new way:
    enforcement_zones = groups_of_type('Enforcement',t)
    if len(enforcement_zones) == 0:
        return t['ParentTerminalStructure']['@Name'] # Fall back to this value for consistency
        # since it seems like a really, really good way of recovering the enforcement zone
        # when the meter has been removed (except in the new case of the Hill District and
        # maybe a few others (I haven't checked for others that I can specifically recall)).

        #raise ValueError("No enforcement zones found for terminal {}".format(t['@Id']))
    elif len(enforcement_zones) > 1:
        raise ValueError("{} enforcement zones found for terminal {}".format(len(enforcement_zones),t['@Id']))
    code = enforcement_zones[0]
    return code

def corrected_zone_name(t,t_ids=[],t_id=None,group_lookup_addendum={}):
    lost_zone_names = {'401354-OLIVER0301': 'DOWNTOWN1',# 401354-OLIVER0301
    # has no TerminalGroups, but clearly it should be in DOWNTOWN1
    '402384-1STAVE0404': 'DOWNTOWN2',
    '403359-5THAVE1301': 'UPTOWN2',
    '421083-ARCHST0802': 'NORTHSIDE',
    '421081-ARCHST1301': 'NORTHSIDE',
    '421079-ARCHST1302': 'NORTHSIDE',
    '421080-ARCHST1304': 'NORTHSIDE',
    #'409342-BIGELO3801': 'OAKLAND3', # This is a best guess based on location and meter ID.
    #'409344-BIGELO3804': 'OAKLAND3', # This is a best guess based on location and meter ID.
    #'409346-BIGELO3902': 'OAKLAND3', # This is a best guess based on location and meter ID.
    '419019-BROOKL0702': 'BROOKLINE',
    '403321-CENTRE3002': 'OAKLAND1',
    '403322-CENTRE3101': 'OAKLAND1',
    '403323-CENTRE3102': 'OAKLAND1',
    '403324-CENTRE3103': 'OAKLAND1', #From the map it's clear this should be OAKLAND1 (and NOT UPTOWN[1|2]).
    '403325-CENTRE3104': 'OAKLAND1',
    '409336-CENTRE4604': 'OAKLAND3', #4602 is OAKLAND3, 4701 is SHADYSIDE2
    '411582-CENTRE4903': 'SHADYSIDE2',
    '411599-CENTRE5501': 'SHADYSIDE2',
    '411600-CENTRE5502': 'SHADYSIDE2',
    '403329-CRAWFD0101': 'UPTOWN1',
    '403327-CRAWFD0104': 'UPTOWN1',
    '407136-DARAGH0304': 'OAKLAND1',
    '407137-DARAGH0306': 'OAKLAND1',
    '415052-ECARSN2404': 'SOUTHSIDE',
    '371565-EOHIO-0001': 'EASTOHIO-L',
    '371566-EOHIO-0002': 'EASTOHIO-L',
    '371571-EOHIO-0003': 'EASTOHIO-L',
    #'401376-FORBES0201': 'DOWNTOWN1', # This should probably not need an exception.
    '401377-FORBES0202': 'DOWNTOWN1',
    '401380-FORBES0303': 'DOWNTOWN1',
    '413004-FORBES5802': 'SQ.HILL1',
    '413010-FORBES5803': 'SQ.HILL1',
    '413008-FORBES5810': 'SQ.HILL1', # 413004-FORBES5810 is currently a valid terminal.
    '308529-HARVBT0001': 'Harvard-Beatty-Inferred-L', #(inferred zone name)
    '308530-HARVBT0002': 'Harvard-Beatty-Inferred-L', #(inferred zone name)
    '411576-IVYST-0702': 'SHADYSIDE1', # This is closer to SHADYSIDE1 than to SHADYSIDE2.
    '325570-JCCLOT0003': 'JCC-L',
    '406637-LIBRTY5102': 'BLOOMFIELD',
    '401373-MARKET0001': 'DOWNTOWN2',
    '409236-NDTHRG0302': 'OAKLAND3',
    '409237-NDTHRG0304': 'OAKLAND3',
    '403319-RBSTEX0409': 'OAKLAND1',
    '403320-RBSTEX0411': 'OAKLAND1',
    '411617-SGHRAM0401': 'SHADYSIDE2',
    '411617-SGHRAM0401': 'SHADYSIDE2',
    '357561-SHILOH0001': 'SHILOH-L',
    '357568-SHILOH0002': 'SHILOH-L',
    '357569-SHILOH0003': 'SHILOH-L',
    '420004-SHILOH0206': 'MT.WASH',
    '404429-SMALLM1401': 'STRIPDIST',
    '304525-TAMELO0002': 'TAME-BEA-L',
    '304526-TAMELO0003': 'TAME-BEA-L',
    '408201-UNIVER0103': 'OAKLAND2',
    #'401363-WILLPN0602': 'DOWNTOWN1' # This should probably not need an exception.
    '401510-WOODST0302': 'DOWNTOWN2',
    '401511-WOODST0402': 'DOWNTOWN2',
    }
    # While these were manually found by grepping payment-points.csv for
    # ID substrings (like TAMELO), this process could be automated by
    # finding the most similar IDs and using their zone names.
    if t is None:
        if t_id in lost_zone_names:
            return lost_zone_names[t_id]
        else:
            six_digit_ids = {id[:6]: id for id in t_ids}
            # Try to use the initial 6-digit code as the basis for the
            # terminal ID (sort of another GUID).
            if id[:6] in six_digit_ids:
                t = terminals[t_ids.index(six_digit_ids[id[:6]])]

    zn = zone_name(t)
    if zn == "Z - Inactive/Removed Terminals":
        if 'TerminalGroups' in t and 'TerminalGroup' in t['TerminalGroups']:
            # The next three lines should be converted into a generic
            # cast_to(type_i, thing)
            glist = t['TerminalGroups']['TerminalGroup']
            if type(t['TerminalGroups']['TerminalGroup']) != list:
                glist = [t['TerminalGroups']['TerminalGroup']]
            for terminalgroup in glist:
                if terminalgroup['@TerminalGroupTypeName'] == 'Enforcement':
                    zn = standardize_group_name(terminalgroup['@TerminalGroupName'])
            if zn == "Z - Inactive/Removed Terminals":
                #print("\nGrody terminal listed with zone_name 'Z - Inactive/Removed Terminals':")
                #pprint(to_dict(t))
                num_zone, new_num_zone, new_old_zone = numbered_zone(t['@Id'],t,group_lookup_addendum)
                zn = convert_group_to_zone(t,num_zone)
                if zn is None:
                    if new_old_zone is not None:
                        zn = new_old_zone
                # [ ] Handle cases where there's a Reporting group but no
                # Enforcement group.
        if zn is None or zn == "Z - Inactive/Removed Terminals":
            if t['@Id'] in lost_zone_names.keys():
                zn = lost_zone_names[t['@Id']]
            else:
                print("corrected_zone_name: No zone name found or inferred for {} (which may mean that it is inactive/removed).".format(t['@Id']))
                # I think it doesn't matter if we can't map inactive meters back to enforcement zones.
                # It might even be OK to remove the "lost_zone_names" corrections.
    return zn


def lot_code(t):
    return t['ParentTerminalStructure']['@Name']

def is_timezoneless(d):
    return d.tzinfo is None or d.tzinfo.utcoffset(d) is None

def get_terminals(use_cache = False):
    if not use_cache:
        url = 'https://webservice.mdc.dmz.caleaccess.com/cwo2exportservice/LiveDataExport/2/LiveDataExportService.svc/terminals'
        r = requests.get(url, auth=(CALE_API_user, CALE_API_password))

        # Convert Cale's XML into a Python dictionary
        doc = xmltodict.parse(r.text,encoding = r.encoding)
    else:
        with open("cached_terminals.xml",'r') as f:
            text = f.read()
        doc = xmltodict.parse(text,encoding = 'utf-8')

    terminals = doc['Terminals']['Terminal']
    return terminals

def cast_fields(original_dicts,ordered_fields):
    # This can become pre_load functions in a Marshmallow schema.
    data = []
    for d_original in original_dicts:
        d = dict(d_original) # Clone the dict to prevent changing the original
        d['durations'] = loads(d['durations'])
        d['payments'] = float(d['payments'])
        # This may not be necessary, but ensuring that datetimes are in
        # ISO format is the best way of preparing timestamps to be
        # sent to CKAN.
        d['start'] = datetime.strptime(d['start'],"%Y-%m-%d %H:%M:%S").isoformat()
        d['end'] = datetime.strptime(d['end'],"%Y-%m-%d %H:%M:%S").isoformat()
        d['utc_start'] = datetime.strptime(d['utc_start'],"%Y-%m-%d %H:%M:%S").isoformat()

        ordered_row = OrderedDict([(fi['id'],d[fi['id']]) for fi in ordered_fields])
        data.append(ordered_row)
    return data

def remove_field(dicts,field,superfield = None):
    # Remove the given field from every dict in dicts (a list of dicts).
    for d in dicts:
        if superfield is None:
            if field in d:
                del d[field]
        else:
            if superfield in d:
                if type(d[superfield]) == list:
                    for subd in d[superfield]:
                        if field in subd:
                            del subd[field]
                else:
                    if field in d[superfield]:
                        del d[superfield][field]
    return dicts

def pull_from_url(url):
    r = requests.get(url, auth=(CALE_API_user, CALE_API_password))
    retries = 0
    retry_limit = 5
    while r.status_code != 200 and retries < retry_limit:
        r = requests.get(url, auth=(CALE_API_user, CALE_API_password))
        retries += 1
        time.sleep(retries*5)
        if retries  % 5 == 0:
            print("|", end="", flush=True)
        else:
            print(".", end="", flush=True)

    if retries == retry_limit:
        print("Retry limit hit when pulling from {}".format(url))
        print("dir(r) = {}".format(dir(r)))
        print("r.status_code = {}".format(r.status_code))
        print("r.reason = {}".format(r.reason))

    return r
