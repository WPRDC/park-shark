import os, sys, re, csv
from pprint import pprint

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def main(input_filename,as_of):
    #input_filename = 'Lease_counts_2018-unprocessed.csv'
    if re.search('-unprocessed',input_filename) is not None:
        output_filename = re.sub('-unprocessed','',input_filename)
    else:
        output_filename = 'output.csv'

    list_of_dicts = list(csv.DictReader(open(input_filename)))

    # Get the headers
    with open(input_filename) as f:
        reader = csv.reader(f)
        keys = next(reader)


    # Add fields by looking up zones.
    zone_by_location = {
            'SHERIDAN HARVARD': "301 - Sheridan Harvard Lot",
            'SHERIDAN KIRKWOOD': "302 - Sheridan Kirkwood Lot",
            'TAMELLO BEATTY': "304 - Tamello Beatty Lot",
            'EVA BEATTY': "307 - Eva Beatty Lot",
            'HARVARD BEATTY': "308 - Harvard Beatty Lot",
            'ANSLEY BEATTY': "311 - Ansley Beatty Lot",
            'PENN CIRCLE N.W.': "314 - Penn Circle NW Lot",
            'DOUGLAS PHILLIPS': "323 - Douglas Phillips Lot",
            'CENTRE CRAIG': "329 - Centre Craig",
            '52ND & BUTLER': "337 - 52nd & Butler Lot",
            '42ND AND BUTLER': "338 - 42nd & Butler Lot",
            #'STANTON McCANDLESS,0,2016-11-21
            '18TH AND SIDNEY': "341 - 18th & Sidney Lot",
            'EAST CARSON': "342 - East Carson Lot",
            '19TH AND CARSON': "343 - 19th & Carson Lot",
            '20TH & SIDNEY PLAZA': "345 - 20th & Sidney Lot", # THESE TWO ARE DIFFERENT
            '20TH AND SIDNEY': "345 - 20th & Sidney Lot",     # NAMES FOR THE SAME LOT/ZONE.
            'BROWNSVILLE SANKEY': "351 - Brownsville & Sandkey Lot",
            'WALTER WARRINGTON': "354 - Walter/Warrington Lot",
            'SHILOH': "357 - Shiloh Street Lot",
            'BROOKLINE BLVD': "361 - Brookline Lot", 
            'BEECHVIEW': "363 - Beechview Lot", 
            'MAIN ALEXANDER': "369 - Main/Alexander Lot",
            'EAST OHIO STREET': "371 - East Ohio Street Lot",
            'OBSERVATORY HILL': "375 - Oberservatory Hill Lot"
            }

    for d in list_of_dicts:
        if d['location'] not in zone_by_location:
            raise ValueError("Unable to find lot with designation {} in zone_by_location".format(d['location']))
        zone = zone_by_location[d['location']]
        d['zone'] = zone
        d['as_of'] = as_of

    keys += ['zone','as_of']

    print("Writing processed data to {}".format(output_filename))
    write_to_csv(output_filename,list_of_dicts,keys)

if __name__ == '__main__':
    if len(sys.argv) <= 2:
        print("Example of how to run this script:")
        print("    > python add_fields.py Lease_counts_2018-unprocessed.csv 2018-07-09")
        raise ValueError("as_of value must be specified (in the form '2018-07-04')")
    input_filename = sys.argv[1]
    as_of = sys.argv[2]
    assert as_of[:2] == '20'
    assert as_of[4] == '-'
    assert as_of[7] == '-'
    main(input_filename=input_filename, as_of=as_of)
