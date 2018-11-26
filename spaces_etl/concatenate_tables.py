"""This script is used to concatenate together all CSV files in the default directory that start with 'space-counts', as a means of compiling several files containing records of available parking spaces by zone."""

import os, sys, re, csv
from pprint import pprint

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def extract_data(fn):
    list_of_dicts = list(csv.DictReader(open(fn)))

    # Get the headers
    with open(fn) as f:
        reader = csv.reader(f)
        keys = next(reader)
    return list_of_dicts, keys

def strip_out_date(fn):
    ds = re.sub('space-counts-','',fn)
    ds = re.sub('-on-street.*','',ds)
    ds = re.sub('-off-street.*','',ds)
    return ds

def get_filenames_with_prefix(file_prefix):
    path = os.getcwd()
    all_filenames = os.listdir(path)
    return [fn for fn in all_filenames if re.match(file_prefix,fn) is not None and os.path.isfile(path+'/'+fn)]

def main(file_prefix):
    if re.search('-on-street',input_filename_1) is not None:
        output_filename = re.sub('-on-street','',input_filename_1)
    elif re.search('-off-street',input_filename_1) is not None:
        output_filename = re.sub('-off-street','',input_filename_1)
    else:
        output_filename = 'output.csv'

    list_of_lists = []
    keyring = []
    for f in files:
        list_of_dicts_1, keys_1 = extract_data(input_filename_1)
        list_of_lists.append(list_of_dicts_1)
        if len(keyring) > 0:
            assert set(keys_1) == set(keyring[0])
        keyring.append(keys_1)

    keys = keyring[0]
    if list_of_dicts_1[0]['type'] == 'on-street':
        on_street = list_of_dicts_1
        off_street = list_of_dicts_2
    elif list_of_dicts_1[0]['type'] == 'off-street':
        on_street = list_of_dicts_2
        off_street = list_of_dicts_1
    else:
        raise ValueError("{} is an unexpected value".format(list_of_dicts_1[0]['type']))

    list_of_dicts = off_street + on_street
    for d in list_of_dicts:
        d['rate'] = float(rate)

    print("Writing processed data to {}".format(output_filename))
    write_to_csv(output_filename,list_of_dicts,keys)

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print("Example of how to run this script:")
        print("    > python concatenate_tables.py space-counts")
        print("This will concatenate together all CSV files in the default directory that start with 'space-counts'.")
        raise ValueError("More command-line parameters are needed.")

    pattern = sys.argv[1]
    main(file_prefix = pattern)
