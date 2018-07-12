import re, json
from collections import defaultdict
from parameters.local_parameters import path
from pprint import pprint

from os import listdir
from os.path import isfile, join

json_path = "{}{}/".format(path,"utc_json/")
only_json_files = sorted([f for f in listdir(json_path) if (isfile(join(json_path, f)) and re.search('.json$',f) is not None)])

pprint(only_json_files)

meters_to_check = ['324541-FMURRY0001', '324542-FMURRY0002', '325543-JCCLOT0001']

counts = defaultdict(int)
meter_payments = 0

def is_meter(p):
    if 'PurchasePayUnit' not in p:
        terminal_id = p['@TerminalID']
        if terminal_id[:3] == 'PBP':
            return False
        elif terminal_id[0] in ['3','4']:
            return True
        else:
            raise ValueError("Unable to categorize terminal with ID {} as a meter or non-meter.".format(terminal_id))
    else:
        if type(p['PurchasePayUnit']) == list: # It's a list of Coin and Card payments.
            return True
        elif p['PurchasePayUnit']['@PayUnitName'] == 'Mobile Payment':
            return False
        else:
            return True

for filename in only_json_files:
    filepath = json_path + filename
    with open(filepath, 'r') as f:
        ps = json.load(f) 
    for p in ps:
        if is_meter(p):
            meter_payments += 1
            
        for m in meters_to_check:
            if p['@TerminalID'] == m:
                counts[m] += 1

    if meter_payments > 0:
        for m in meters_to_check:
            print("{},{},{},{},{}".format(filename.split('.')[0], m, counts[m]/meter_payments, counts[m], meter_payments))

