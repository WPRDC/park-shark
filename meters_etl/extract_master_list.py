import os, csv, re
from xlrd import open_workbook

from pprint import pprint
from collections import defaultdict
import string
from dateutil import parser

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def value_or_none(record,field):
    if field in record:
        return record[field]
    return None

def printable(s):
    if s is None:
        return s
    printable_chars = set(string.printable)
    return ''.join(list(filter(lambda x: x in printable_chars, s)))

def main():
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    path = dname + "/master_list/"

    wb = open_workbook(path+'PPA-master-list.xlsx')

    zones = {}
    zones['Allentown'] = '417 - Allentown'
    zones['Bakery Sq'] = '425 - Bakery Sq'
    zones['Beechview'] = '418 - Beechview'
    zones['Bloomfield'] = '406 - Bloomfield (On-street)'
    zones['Brookline'] = '419 - Brookline'
    zones['Carrick'] = '416 - Carrick'
    zones['Downtown 1'] = '401 - Downtown 1'
    zones['Downtown 2'] = '402 - Downtown 2'
    zones['East Liberty'] = '412 - East Liberty'
    zones['Hill District'] = '426 - Hill District' # These are mapping two minizones to one regular zone
    zones['Hill District 2'] = '426 - Hill District' # These are mapping two minizones to one regular zone
    zones['Knoxville'] = '427 - Knoxville'
    zones['Lawrenceville'] = '405 - Lawrenceville'
    zones['Mellon Park'] = '414 - Mellon Park'
    zones['Mt Washington'] = '420 - Mt. Washington'
    zones['Northshore'] = '422 - Northshore'
    zones['Northside'] = '421 - NorthSide'
    zones['Oakland 1'] = '407 - Oakland 1'
    zones['Oakland 2'] = '408 - Oakland 2'
    zones['Oakland 3'] = '409 - Oakland 3'
    zones['Oakland 4'] = '410 - Oakland 4'
    zones['Shadyside 1'] = '411 - Shadyside'# These are mapping two minizones to one regular zone
    zones['Shadyside 2'] = '411 - Shadyside'# These are mapping two minizones to one regular zone
    zones['Southside'] = '415 - SS & SSW'
    zones['Sq Hill 1'] = '413 - Squirrel Hill'# These are mapping two minizones to one regular zone
    zones['Sq Hill 2'] = '413 - Squirrel Hill'# These are mapping two minizones to one regular zone
    zones['Strip District'] = '404 - Strip Disctrict'
    zones['Technology'] = '424 - Technology Drive'
    zones['Uptown 1'] = '403 - Uptown'
    zones['Uptown 2'] = '403 - Uptown'
    zones['West Circuit'] = '410 - Oakland 4'# This is mapping two minizones to one regular zone
    zones['West End'] = '423 - West End'
    zones['Inactive Meters'] = 'Z - Inactive/Removed Terminals'


    bigboard = []
    meter_ids = []
    for s in wb.sheets():
        #print('Sheet:',s.name)
        sheet_zone = s.name
        print("="*10 + sheet_zone + "="*10)
        if sheet_zone in zones:
            zone = zones[sheet_zone]
        else:
            zone = None
        records = []
        for k,row in enumerate(range(s.nrows)):
            if k == 0:
                headers = []
                for col in range(s.ncols):
                    headers.append(s.cell(row,col).value)
            else:
                values = []
                record = {}
                for j,col in enumerate(range(s.ncols)):
                    cell_value = s.cell(row,col).value
                    record[headers[j]] = cell_value
                    values.append(cell_value)
                records.append(record)
                #try:
                #    str_values = [printable(v) for v in values]
                #except:
                #    pprint(values)
                #    print(values[0])
                #    print(len(values[0]))
                #    print("{} ({})".format(printable(values[0]),len(printable(values[0]))))

                    #str_values = [str(v) for v in values]
                #print(','.join(str_values))

                #if sheet_zone != 'Inactive Meters'
                if True:
                    # Inactive Meters don't have most of these fields.
                    bigboard_record = {}
                    meter_id = record['Meter ID']
                    bigboard_record['meter_id'] = printable(meter_id)
                    bigboard_record['zone'] = printable(zone)
                    bigboard_record['max_hours'] = printable(value_or_none(record, 'Max Hours'))
                    bigboard_record['hours'] = printable(value_or_none(record, 'Hours'))
                    bigboard_record['rate'] = printable(value_or_none(record, 'Rate'))
                    bigboard_record['restrictions'] = printable(value_or_none(record, 'Restrictions'))
                    bigboard_record['special_events'] = printable(value_or_none(record, 'Special Events'))

                    if meter_id not in meter_ids: # Make sure that this is not a duplicate Inactive Meters record for a meter
                        bigboard.append(bigboard_record) # that's already been recorded (otherwise, it winds up getting
                        # overwritten and rate information does not show up in the extracted data).
                        meter_ids.append(meter_id)
                    pprint(bigboard_record)

    keys = ['meter_id', 'zone', 'max_hours', 'hours', 'rate', 'restrictions', 'special_events']
    write_to_csv(path+'meters_master_list.csv',bigboard,keys)

    # read in meters file extracted from CALE API
    meter_data = {}
    for r in bigboard:
        meter_data[r['meter_id']] = r
    joined = []
    rate_list_by_tariff_program = defaultdict(list)
    rate_list_by_meter_id = defaultdict(list)

    filename = 'meters-2019-01-29.csv'
    filename_w_o_extension, extension = filename.split('.')

    date_from_filename = parser.parse(re.sub("^meters-","",filename_w_o_extension)).date()

    with open(path+filename,'r') as g:
        list_of_ds = csv.DictReader(g)

        for d in list_of_ds:
            if d['ID'] in meter_data:
                meter = meter_data[d['ID']]
                meter_rate = meter['rate']
                if meter_rate is not None:
                    meter_rate = re.sub("\.00","",meter_rate)
                    meter_rate = re.sub("HR","hr",re.sub("Hr","hr",meter_rate))
                    if meter_rate not in rate_list_by_tariff_program[d['TariffPrograms']]:
                        rate_list_by_tariff_program[d['TariffPrograms']].append(meter_rate)
                    if meter_rate not in rate_list_by_meter_id[d['ID']]:
                        rate_list_by_meter_id[d['ID']].append(meter_rate)
                d['rate'] = meter_rate
                d['max_hours'] = meter['max_hours']
                d['hours'] = meter['hours']
                d['restrictions'] = meter['restrictions']
                d['special_events'] = meter['special_events']
            else:
                print("{} is missing from the master list.".format(d['ID']))
            d['rate_as_of'] = date_from_filename.isoformat()

            joined.append(d)

    keys = ['ID', 'Zone', 'Location', 'location_type', 'Latitude', 'Longitude', 'Status', 'ParentStructure', 'all_groups', 'GUID', 'Rate information', 'rate', 'TariffPrograms', 'TariffDescriptions', 'max_hours', 'hours', 'restrictions', 'special_events', 'created_utc', 'in_service_utc', 'removed_utc', 'rate_as_of']
    write_to_csv(path+'joined.csv',joined,keys)

    rate_by_tariff = {}
    for tariff_program,rate_list in rate_list_by_tariff_program.items():
        print("{:<5} {} {}".format(tariff_program, len(rate_list), rate_list))
        assert len(rate_list) == 1
        tariff_id = re.sub("Pgm","",tariff_program)
        rate_by_tariff[tariff_id] = rate_list[0]
        # The results of this originally had the following different forms:
        # Pgm43 1 ['$1.75/hr']
        # Pgm73 1 [None] # <== This is an odd one.
        #       1 [None]
        # Pgm8  3 [None, '$2/hr', '$2.00/hr'] # <== These can be collapsed to $2/hour
        # Pgm68 2 [None, '$3/hr']
        # Eliminating None values and combining $2/hr with $2.00/hr gives a nice lookup table.


    print("*"*40)
    rate_by_meter = {}
    for meter_id,rate_list in rate_list_by_meter_id.items():
        if len(rate_list) != 1:
            print("{:<5} {} {}".format(meter_id, len(rate_list), rate_list))
        assert len(rate_list) == 1
        rate_by_meter[meter_id] = rate_list[0]

    return rate_by_tariff, rate_by_meter

if __name__ == '__main__':
    main()
