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

def main(input_filename_1,input_filename_2,as_of):
    if re.search('-on-street',input_filename_1) is not None:
        output_filename = re.sub('-on-street','',input_filename_1)
    elif re.search('-off-street',input_filename_1) is not None:
        output_filename = re.sub('-off-street','',input_filename_1)
    else:
        output_filename = 'output.csv'

    list_of_dicts_1, keys_1 = extract_data(input_filename_1)
    list_of_dicts_2, keys_2 = extract_data(input_filename_2)

    assert set(keys_1) == set(keys_2)
    keys = keys_1
    if list_of_dicts_1[0]['type'] == 'on-street':
        on_street = list_of_dicts_1
        off_street = list_of_dicts_2
    elif list_of_dicts_1[0]['type'] == 'off-street':
        on_street = list_of_dicts_2
        off_street = list_of_dicts_1
    else:
        raise ValueError("{} is an unexpected value".format(list_of_dicts_1[0]['type']))

    # Add fields by looking up zones.
    zone_by_location = {
            'SHERIDAN HARVARD': "301 - Sheridan Harvard Lot", # the location name in the lease-counts file
            'SHERIDAN/HARVARD': "301 - Sheridan Harvard Lot", # another alias for space counts
            'SHERIDAN KIRKWOOD': "302 - Sheridan Kirkwood Lot",
            'SHERIDAN/KIRKWOOD': "302 - Sheridan Kirkwood Lot", # another alias
            'TAMELLO BEATTY': "304 - Tamello Beatty Lot",
            'TAMELLO/BEATTY': "304 - Tamello Beatty Lot", # another alias
            'EVA BEATTY': "307 - Eva Beatty Lot",
            'EVA/BEATTY': "307 - Eva Beatty Lot", # another alias
            'HARVARD BEATTY': "308 - Harvard Beatty Lot",
            'ANSLEY BEATTY': "311 - Ansley Beatty Lot",
            'ANSLEY/BEATTY': "311 - Ansley Beatty Lot", # another alias
            'PENN CIRCLE N.W.': "314 - Penn Circle NW Lot",
            'PENN CIRCLE NW': "314 - Penn Circle NW Lot", # another alias
            'BEACON/BARTLETT' : "321 - Beacon Bartlett Lot",
            'FORBES/SHADY': "322 - Forbes Shady Lot",
            'DOUGLAS PHILLIPS': "323 - Douglas Phillips Lot",
            'DOUGLAS/PHILLIPS': "323 - Douglas Phillips Lot", # another alias
            'FORBES/MURRAY': "324 - Forbes Murray Lot",
            'TAYLOR STREET': "334 - Taylor Street Lot",
            'JCC': "325 - JCC/Forbes Lot",
            'IVY/BELLEFONTE': "328 - Ivy Bellefonte Lot",
            'CENTRE CRAIG': "329 - Centre Craig",
            'HOMEWOOD/ZENITH': "331 - Homewood Zenith Lot",
            'FRIENDSHIP/CEDARVILLE': "335 - Friendship Cedarville Lot",
            '52ND & BUTLER': "337 - 52nd & Butler Lot",
            '5224 BUTLER STREET': "337 - 52nd & Butler Lot", # another alias
            '42ND AND BUTLER': "338 - 42nd & Butler Lot",
            '42ND /BUTLER': "338 - 42nd & Butler Lot", # yet another alias
            #'STANTON McCANDLESS,0,2016-11-21
            '18TH AND SIDNEY': "341 - 18th & Sidney Lot",
            '18TH/SIDNEY': "341 - 18th & Sidney Lot", # another alias
            'EAST CARSON': "342 - East Carson Lot",
            '19TH AND CARSON': "343 - 19th & Carson Lot",
            '19TH/CARSON': "343 - 19th & Carson Lot", # another alias
            '18TH/CARSON': "344 - 18th & Carson Lot", 
            '20TH & SIDNEY PLAZA': "345 - 20th & Sidney Lot", # THESE TWO ARE DIFFERENT
            '20TH AND SIDNEY': "345 - 20th & Sidney Lot",     # NAMES FOR THE SAME LOT/ZONE.
            '20TH/SIDNEY': "345 - 20th & Sidney Lot",           # AND HERE'S ANOTHER.
            'BROWNSVILLE SANKEY': "351 - Brownsville & Sandkey Lot",
            'BROWNSVILLE/SANKEY': "351 - Brownsville & Sandkey Lot", # another alias
            'WALTER WARRINGTON': "354 - Walter/Warrington Lot",
            'WALTER/WARRINGTON': "354 - Walter/Warrington Lot", # another alias
            'ASTEROID/WARRINGTON': "355 - Asteroid Warrington Lot",
            'SHILOH': "357 - Shiloh Street Lot",
            'BROOKLINE BLVD': "361 - Brookline Lot",
            'BEECHVIEW': "363 - Beechview Lot",
            'BEECHVIEW BLVD': "363 - Beechview Lot", # another alias
            'MAIN ALEXANDER': "369 - Main/Alexander Lot",
            'MAIN/ALEXANDER': "369 - Main/Alexander Lot", # another alias
            'EAST OHIO STREET': "371 - East Ohio Street Lot",
            'EAST OHIO': "371 - East Ohio Street Lot", # another alias
            'OBSERVATORY HILL': "375 - Oberservatory Hill Lot",
            'DOWNTOWN 1': "401 - Downtown 1",
            'DOWNTOWN 2': "402 - Downtown 2",
            'UPTOWN': "403 - Uptown",
            'STRIP DISTRICT': "404 - Strip Disctrict",
            'LAWRENCEVILLE': "405 - Lawrenceville",
            'BLOOMFIELD/GARFIELD': "406 - Bloomfield (On-street)",
            'OAKLAND 1': "407 - Oakland 1",
            'OAKLAND 2': "408 - Oakland 2",
            'OAKLAND 3': "409 - Oakland 3",
            'OAKLAND 4': "410 - Oakland 4",
            'SHADYSIDE': "411 - Shadyside",
            'EAST LIBERTY': "412 - East Liberty",
            'SQUIRREL HILL/GREENFIELD': "413 - Squirrel Hill",
            'MELLON PARK AREA': "414 - Mellon Park",
            'SOUTHSIDE': "415 - SS & SSW",
            'CARRICK': "416 - Carrick",
            'ALLENTOWN': "417 - Allentown",
            'BEECHVIEW': "418 - Beechview",
            'BROOKLINE': "419 - Brookline",
            'MT. WASHINGTON': "420 - Mt. Washington",
            'NORTH SIDE': "421 - NorthSide",
            'NORTH SHORE': "422 - Northshore",
            'WEST END': "423 - West End",
            'TECHNOLOGY DRIVE': "424 - Technology Drive",
            'BAKERY SQUARE': "425 - Bakery Sq",
            'HILL DISTRICT': "426 - Hill District"
            }

    list_of_dicts = off_street + on_street
    for d in list_of_dicts:
        if d['location'] not in zone_by_location:
            raise ValueError("Unable to find zone with designation {} in zone_by_location".format(d['location']))
        zone = zone_by_location[d['location']]
        d['zone'] = zone
        d['as_of'] = as_of
        rate_text = d['rate_description']
        if re.match('\$', rate_text) is not None and re.search('/HR', rate_text) is not None:
            rate = re.sub('/HR','',re.sub('\$','',rate_text))
            print(rate)
            d['rate'] = float(rate)

    keys = ['zone','as_of'] + keys
    keys.insert(keys.index('rate_description'), 'rate')
    keys.remove('location')

    print("Writing processed data to {}".format(output_filename))
    write_to_csv(output_filename,list_of_dicts,keys)

if __name__ == '__main__':
    if len(sys.argv) <= 2:
        print("Example of how to run this script:")
        print("    > python process_spaces.py space-counts-2018-01-19-on-street.csv space-counts-2018-01-19-off-street.csv")
        raise ValueError("More command-line parameters are needed.")
    input_filename_1 = sys.argv[1]
    input_filename_2 = sys.argv[2]
    if (re.search("space-counts-",input_filename_1) is not None) and re.search("space-counts-",input_filename_2) is not None:
        as_of = strip_out_date(input_filename_1)
        as_of_2 = strip_out_date(input_filename_2)
        assert as_of == as_of_2
        assert as_of[:2] == '20'
        assert as_of[4] == '-'
        assert as_of[7] == '-'
        for num in as_of.split('-'):
            try:
                int(num)
            except:
                print("{} contains a non-integer".format(as_of))
    else:
        print("Example of how to run this script:")
        print("    > python process_spaces.py space-counts-2018-01-19-on-street.csv space-counts-2018-01-19-off-street.csv")
        raise ValueError("Correctly formatted filenames are needed.")
    main(input_filename_1=input_filename_1, input_filename_2=input_filename_2, as_of=as_of)
