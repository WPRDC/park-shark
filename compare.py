"""This script compares the Purchase GUIDs in a (single) locally 
stored SQLite table (currently assuming all transactions are under 
the same date) with those in a reference file downloaded as a CSV 
from CALE Web Office."""
from dateutil import parser
import os, sys, csv, dataset
from pprint import pprint

from util.sqlite_util import is_date_cached, get_sqlite_table
from parameters.local_parameters import path

from collections import defaultdict

def check_guids(*args, **kwargs):
    fn = kwargs.get('fn',"cwo_refs/Purchases-2017-06-16-1200-1300-Eastern-Historical.csv")
    with open(fn,'r') as f:
        reader = csv.DictReader(f)
        pdls = []
        guids = []
        ds = []
        for d in reader:
            # Check whether GUID is in corresponding sqlite database.
            pdl = parser.parse(d['Purchase Date Local'])
            guid = d['Purchase Guid']
            pdls.append(pdl)
            guids.append(guid)
            ds.append(d)
        reference_time = 'purchase_time'
        date_i = pdl.date()
        if is_date_cached(path,reference_time,date_i):
            table, db = get_sqlite_table(path,date_i,reference_time)
            sqlite_guids = []
            for row in table.all():
                sqlite_guids.append(row['@PurchaseGuid'])
            k = 0
            count = 0
            counts_by_meter = defaultdict(int)
            for d,pdl,guid in zip(ds,pdls,guids):
                if guid.upper() not in sqlite_guids:
                    print("Unable to find {} (k={}):".format(guid,k))
                    pprint(d)
                    counts_by_meter[d['Terminal - Terminal ID']] += 1
                    count += 1
                k += 1
            print("total missing = {}".format(count))
            pprint(counts_by_meter) 

if __name__ == '__main__':
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        copy_of_args = list(args)

        list_of_servers = ["meters-etl", #"official-terminals", "aws-test"
                "transactions-production",
                "transactions-payment-time-of",
                "transactions-prototype",
                "transactions-by-pdl",
                "split-transactions-by-pdl",
                "debug",
                "testbed",
                "sandbox",
                ] # This list could be automatically harvested from SETTINGS_FILE.

        kwparams = {}
        # This is a new way of parsing command-line arguments that cares less about position
        # and just does its best to identify the user's intent.
        for k,arg in enumerate(copy_of_args):
            # Try to use arg as filename that can be opened.
            if os.path.isfile(arg):
                fn = arg
                args.remove(arg)
            else:
                print("I have no idea what do with args[{}] = {}.".format(k,arg))

        kwparams['fn'] = fn
        pprint(kwparams)
        check_guids(**kwparams)
    else:
        raise ValueError("Please specify some command-line parameters")

