import re, csv, copy, requests, time
from pprint import pprint
from icecream import ic
from collections import defaultdict

def write_to_csv(filename, list_of_dicts, keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def standardize_string(x):
    return re.sub('\s+', ' ', x).upper()

def standardize_address(x):
    x = re.sub('\.', '', x)
    abbreviation_by_street_designator = {'Avenue': 'Ave',
            'Court': 'Ct',
            'Drive': 'Dr',
            'Road': 'Rd',
            'Street': 'St',
            'Way': 'Wy',
            ' Eighth ': ' 8th ',
            ' N SECOND': ' NORTH SECOND',
            'CRUCIBLE CT': 'CRUCIBLE ST',
            '5 PALISADES PLZ': '5 PALISADES PLAZA, SUITE A-3',
            }
    for designator, abbreviation in abbreviation_by_street_designator.items():
        x = re.sub(designator, abbreviation, x, flags=re.IGNORECASE)
    return standardize_string(x)

def standardize_name(x):
    x = re.sub('\.$', '', x)
    abbreviation_by_designator = {'Apartments': 'Apts',
            'APARTMNENTS': 'Apts',
            'Avenue': 'Ave',
            ' & ': ' AND ',
            ' PHSE ': ' PHASE ',
            ' PH ': ' PHASE ',
            'PHASE 1': 'PHASE I',
            'PHASE 2': 'PHASE II',
            'PHASE 3': 'PHASE III',
            'PHASE 4': 'PHASE IV',
            ' HTS ': ' HEIGHTS ',
            'BEDFORD IB': 'BEDFORD PHASE IB',
            'NORTH HILLS HIGHLANDS II': 'NORTH HILLS HIGHLANDS PHASE II',
            'OAK HILL APT PHASE II': 'OAK HILL PHASE II',
            'HOMESTEAD APTS II': 'HOMESTEAD APTS PHASE II',
            }
    for designator, abbreviation in abbreviation_by_designator.items():
        x = re.sub(designator, abbreviation, x, flags=re.IGNORECASE)
    return standardize_string(x)

def standardize_city(x):
    x = re.sub('\.$', '', x)
    substitutions = {'UPPER SAINTE CLAIR': 'UPPER SAINT CLAIR',
            'MC KEES ROCKS': 'MCKEES ROCKS',
            'MC KEESPORT': 'MCKEESPORT',
            }
    for target, replacement in substitutions.items():
        x = re.sub(target, replacement, x, flags=re.IGNORECASE)
    return standardize_string(x)

def standardize_field(x, fieldname):
    if fieldname == 'source_file':
        return x
    if fieldname == 'hud_property_name':
        return standardize_name(x)
    if fieldname == 'property_street_address':
        return standardize_address(x)
    if fieldname in ['municipality_name', 'city']:
        return standardize_city(x)
    return standardize_string(x)

def geocode_address_with_geomancer(address):
    url = "https://tools.wprdc.org/geo/geocode?addr={}".format(address)
    import requests
    sess = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries = 20)
    sess.mount('http://', adapter)

    #r = requests.get(url) # Switched to sess.get because of a
    # "EOF occurred in violation of protocol" error.
    r = sess.get(url)
    result = r.json()
    time.sleep(0.1)
    if result['data']['status'] == "OK":
        longitude, latitude = result['data']['geom']['coordinates']
        return longitude, latitude
    print(f"Unable to geocode {address}, failing with status code '{result['data']['status']}'")
    return None, None

def try_to_geocode(merged_record):
    if merged_record['latitude'] == '':
        if merged_record.get('property_street_address', None) in ['', None, 'SCATTERED SITES',
                'SCATTERED SITES IN GARFIELD', 'MULTIPLE PARCELS (19)']:
            return merged_record
        address = merged_record['property_street_address']
        if '|' in address: # Pick the first of several addresses to geocode
            address = address.split('|')[0]

        if merged_record.get('city', None) in [None, '']:
            return merged_record
        if 'municipality' in merged_record and merged_record['municipality'] not in [None, '']:
            alternate_address = f"{address}, {merged_record['municipality']}, PA "
            no_municipality = False
        else:
            no_municipality = True
        address += f", {merged_record['city']}, PA "
        if merged_record.get('zip_code', '') not in ['']:
            address += merged_record['zip_code']
            if not no_municipality:
                alternate_address += merged_record['zip_code']
        print(f"Trying to geocode {address}.")
        longitude, latitude = geocode_address_with_geomancer(address)
        if latitude is None and not no_municipality:
            longitude, latitude = geocode_address_with_geomancer(alternate_address)
        if latitude is not None:
            merged_record['latitude'] = latitude
            merged_record['longitude'] = longitude
            print(f"   Changed coordinates to ({latitude}, {longitude}).")

    return merged_record

def fix_some_records(record_1, record_2, merged_record, verbose):
    if '800030192' in [record_1['property_id'], record_2['property_id']]:
        merged_record['property_street_address'] = '301 S HIGHLAND AVE'
        merged_record['city'] = 'PITTSBURGH'
        merged_record['zip_code'] = '15206'
        merged_record['census_tract'] = '42003070600' # HUD_Insured_Multifamily_Properties.csv had the correct values.
        # LIHTCPUB.csv was totally wrong. Maybe because it had 301 Highland Ave instead of 301 S Highland Ave.
        assert merged_record['census_tract'] in record_1['census_tract'].split('|') + record_2['census_tract'].split('|') # Guard against Census tract drift.
        merged_record['latitude'] = '40.457363999' # It should be fine to set these as strings.
        merged_record['longitude'] = '-79.924964'
    elif '800238769' in [record_1['property_id'], record_2['property_id']]:
        merged_record['property_street_address'] = '130 7TH ST'
        merged_record['city'] = 'PITTSBURGH'
        merged_record['zip_code'] = '15222'
        merged_record['census_tract'] = '42003020100' # HUD_Insured_Multifamily_Properties.csv had the correct values.
        # LIHTCPUB.csv was totally wrong (again).
        assert merged_record['census_tract'] in record_1['census_tract'].split('|') + record_2['census_tract'].split('|') # Guard against Census tract drift.
        merged_record['latitude'] = '40.443638999'
        merged_record['longitude'] = '-80.000669'
    elif 'PA001000095' in [record_1['development_code'], record_2['development_code']]:
        merged_record['property_street_address'] = '5171 COLUMBO ST'
        merged_record['city'] = 'PITTSBURGH'
        merged_record['census_tract'] = '42003101600' # This is the 2010 tract (which I found somewhere in the data).
        merged_record['census_tract'] = '42003101900' # This is the 2020 tract (which I looked up:
        # https://geocoding.geo.census.gov/geocoder/geographies/address?street=5171+COLUMBO+ST&city=Pittsburgh&state=PA&zip=15212&benchmark=4&vintage=4
        # housing_inspections has the right street/city/latitude/longitude (but no tract)
        # public_housing_projects has everything right (including the census_tract). It also has a geocoding_accuracy field!
        merged_record['latitude'] = '40.469373999'
        merged_record['longitude'] = '-79.9393939999999'
    elif '800240610' in [record_1['property_id'], record_2['property_id']]:
        # The proposed 25-acre Larimer/East Liberty Park spans from Station Street and Larimer Avenue to the northern corner of the Larimer Neighborhood at Orphan Street and Larimer Avenue. The park will serve 4,177 residents who live within a five-minute walk.
        merged_record['zip_code'] = '15206'
        merged_record['census_tract'] = '42003120900' # 2020 tract (Looked up on census.gov). The HUD MF data had the correct 2010 tract.
        merged_record['latitude'] = '40.465034999' # Using HUD MF numbers.
        merged_record['longitude'] = '-79.915303' # Using HUD MF numbers.
    elif '800246671' in [record_1['property_id'], record_2['property_id']]:
        merged_record['property_street_address'] = '537 Oak Hill Dr' # Just choosing the HUD MF address
        merged_record['city'] = 'PITTSBURGH'
        merged_record['zip_code'] = '15213'
        merged_record['census_tract'] = '42003051000'  # 2020-verified Census tract matched one from another file.
        assert merged_record['census_tract'] in record_1['census_tract'].split('|') + record_2['census_tract'].split('|') # Guard against Census tract drift.
        merged_record['latitude'] = '40.4438084'
        merged_record['longitude'] = '-79.970124'
    elif 'PA006000824' in [record_1['development_code'], record_2['development_code']]:
        merged_record['property_street_address'] = '5171 COLUMBO ST'
        merged_record['city'] = 'PITTSBURGH'
        merged_record['census_tract'] = '42003101900' # This is the 2020 tract (which I looked up).

        merged_record['latitude'] = '40.469373999'
        merged_record['longitude'] = '-79.9393939999999'

    elif 'TC1990-0139' in [record_1['normalized_state_id'], record_2['normalized_state_id']]:
        merged_record['hud_property_name'] = 'MELVIN COURT/CHURCHILL APTS'
        #merged_record['property_street_address'] = ''
        merged_record['municipality_name'] = 'PENN HILLS'
        merged_record['city'] = 'PITTSBURGH'
        merged_record['zip_code'] = '15235'
        merged_record['census_tract'] = '42003523200' # This is the 2020 tract (which I looked up and which matches one of the existing values).
        assert merged_record['census_tract'] in record_1['census_tract'].split('|') + record_2['census_tract'].split('|') # Guard against Census tract drift.

        merged_record['latitude'] = '40.4587898254395' # Closest to Census geocoder location
        merged_record['longitude'] = '-79.8440475463867'

        merged_record['status'] = 'Closed' # According to web searches

    elif 'PA006000301' in [record_1['development_code'], record_2['development_code']]:
        merged_record['property_street_address'] = '209 LOCUST ST'
        merged_record['city'] = 'PITTSBURGH'
        merged_record['census_tract'] = '42003481000' # The 2020 tract does not match the previous values.
        merged_record['latitude'] = '40.4666786193848'
        merged_record['longitude'] = '-80.0561752319336'

    elif '800018237' in [record_1['property_id'], record_2['property_id']]:
        merged_record['property_street_address'] = '7021 Kelly Street'
        merged_record['zip_code'] = '15208'
        merged_record['census_tract'] = '42003130800' # Added from census.gov (2020 tract)
        merged_record['latitude'] = '40.45648' # Added from census.gov.
        merged_record['longitude'] = '-79.90119' # Added from census.gov.
    elif '800018276' in [record_1['property_id'], record_2['property_id']]:
        merged_record['property_street_address'] = '120 CAMBRIDGE SQUARE DR'
        merged_record['census_tract'] = '42003521301' # Added from census.gov (2020 tract)
        merged_record['latitude'] = '40.422333' # Added from census.gov.
        merged_record['longitude'] = '-79.76022' # Added from census.gov.
    elif '800018357' in [record_1['property_id'], record_2['property_id']]:
        merged_record['property_street_address'] = '2017-2031 DE RUAD ST' # I'm deliberately keeping this as a range of addresses, for now.
        merged_record['zip_code'] = '15208'
        merged_record['census_tract'] = '42003040200' # Added from census.gov (2020 tract)
        merged_record['latitude'] = '40.438619368902245' # Added manually
        merged_record['longitude'] = '-79.97572065933687' # Added manually
    elif '800018531' in [record_1['property_id'], record_2['property_id']]:
        merged_record['property_street_address'] = '7130 FRANKSTOWN AVE'
        merged_record['census_tract'] = '42003130800' # Added from census.gov (2020 tract)
        merged_record['latitude'] = '40.45773' # Added manually
        merged_record['longitude'] = '-79.89778' # Added manually
    elif '800018532' in [record_1['property_id'], record_2['property_id']]:
        merged_record['property_street_address'] = '1020 BRUSHTON AVE'
        merged_record['census_tract'] = '42003130700' # Added from census.gov (2020 tract)
        merged_record['latitude'] = '40.457275' # Added manually
        merged_record['longitude'] = '-79.88773' # Added manually
        merged_record['units'] = 126
        assert str(merged_record['units']) in record_1['units'].split('|') + record_2['units'].split('|') # Guard against units drift.
#    elif '800018175' in [record_1['property_id'], record_2['property_id']]:
#        #merged_record['units'] = 126 # [ ] Need to distinguish between 130 and 136 units.
#        #assert str(merged_record['units']) in record_1['units'].split('|') + record_2['units'].split('|') # Guard against units drift.
    elif '800246670' in [record_1['property_id'], record_2['property_id']]:
#        merged_record['census_tract'] = '42003130700' # Added from census.gov (2020 tract)
#        merged_record['latitude'] = '40.4528969990001' # Chosen from two very close sets
        assert 'OAK HILL PHASE IC' in [record_1['hud_property_name'], record_2['hud_property_name']]
        merged_record['hud_property_name'] = 'OAK HILL PHASE 1C'
        merged_record['property_street_address'] = '537 Oak Hill Drive'
        merged_record['latitude'] = '40.4437629990001'
        merged_record['longitude'] = '-79.970113'
        merged_record['scattered_sites'] = "TRUE" # This may not be necessary, but setting property_street_address is
        # obscuring the fact that one table has SCATTERED SITES as the address.

    elif 'TC1994-0155' in [record_1['normalized_state_id'], record_2['normalized_state_id']]:
        merged_record['property_street_address'] = '2253-2263 HAWTHORNE AVE'
        merged_record['municipality_name'] = 'SWISSVALE'
        merged_record['city'] = 'PITTSBURGH'
        merged_record['latitude'] = '40.42121887' # The coordinates of 2263 HAWTHORNE
        merged_record['longitude'] = '-79.8805542'
        merged_record['status'] = 'Closed' # According to web searches
    elif 'PA006000812' in [record_1['development_code'], record_2['development_code']]:
        merged_record['property_street_address'] = '200 TREFOIL CT'
        merged_record['latitude'] = '40.4252243' # The coordinates of 2263 HAWTHORNE
        merged_record['longitude'] = '-79.75964355'
    elif 'PA006000303' in [record_1['development_code'], record_2['development_code']]:
        merged_record['property_street_address'] = '507 GROVETON DR'
        merged_record['latitude'] = '40.50457382'
        merged_record['longitude'] = '-80.14112854'
    elif 'PA001000096' in [record_1['development_code'], record_2['development_code']]:
        merged_record['property_street_address'] = '242 FERN ST'
        merged_record['latitude'] = '40.47097'
        merged_record['longitude'] = '-79.9348'
    elif '800018474' in [record_1['property_id'], record_2['property_id']]:
        # Choose an address for geooding
        if merged_record['property_street_address'] == '1513-1521 CRUCIBLE ST':
            merged_record['property_street_address'] = '1513 CRUCIBLE ST'
    elif '800018716' in [record_1['property_id'], record_2['property_id']]:
        # Choose an address for geooding
        if merged_record['property_street_address'] == '1300 BRIGHTON RD|1414 BRIGHTON RD':
            merged_record['property_street_address'] = '1414 BRIGHTON RD'
    elif '800018393' in [record_1['property_id'], record_2['property_id']]:
        # Choose an address for geooding
        if merged_record['property_street_address'] == '7021 KELLY ST|7057 FLETCHER WY': # 7021 KELLY is the AHRCO office
            # (as is clear from Google Street View). Two other projects are associated with
            # the same address, so I'm switching to alternate addresses for the map marker.
            merged_record['property_street_address'] = '7057 FLETCHER WY|7021 KELLY ST'

    return merged_record

def fix_single_record(record):
    if '800018680' == record['property_id']:
        record['property_street_address'] = re.sub('MIDWY', 'MIDWAY', record['property_street_address'])
    elif '10630' == record['pmindx']:
        record['zip_code'] == re.sub('^18212', '15212', record['zip_code'])
    elif '10186' == record['pmindx']:
        # Choose an address for geooding
        #if merged_record['hud_property_name'] == 'MIDDLE HILL':
        record['hud_property_name'] = 'MIDDLE HILL (ADDISON TERRACE PHASE 3)'
        if record['property_street_address'] == 'LOTS BEDFORD ERIN TRENT':
            record['property_street_address'] = 'Lots: Bedford, Erin, Trent, Wooster and Webster Aves'
            # Obtained from https://www.phfa.org/forms/multifamily_news/awards/2015/2015_app_log_1.pdf
    elif '800018530' == record['property_id']:
        # Choose an address for geooding
        if record['property_street_address'] == '331 EAST 9TH AND 339 EAST 12TH AVE':
            record['property_street_address'] = '331 EAST 9TH AVE'
    elif '10223' == record['pmindx']:
        assert record['property_street_address'] == 'SCATTERED SITES'
        record['property_street_address'] = '2520 Wadsworth St|Allequippa Place and Wadsworth Street (Scattered Sites?)'
        # Maybe this is not scattered sites? Oakland Affordable Living appears to be an apartment building.
        record['zip_code'] = '15213'
        record['city'] = 'Pittsburgh'
        record['latitude'] = '40.4428'
        record['longitude'] = '-79.9675'
        record['census_tract'] = '42003040200' # 2020 Census tract - looked up through geo.census.gov
    elif '10390' == record['pmindx']:
        assert record['property_street_address'] == '3300-3350 PENN AVE'
        record['property_street_address'] = '3350 PENN AVE'
        record['latitude'] = '40.4623'
        record['longitude'] = '-79.9672'
        #record['census_tract'] = '' # 2020 Census tract - looked up through geo.census.gov
        # Lookup is failing

    elif '10504' == record['pmindx']:
        assert record['property_street_address'] == 'STOEBNER WY, WINSLOW ST,'
        record['hud_property_name'] = "LARIMER / EAST LIBERTY CHOICE NEIGHBORHOODS INITIATIVE"
        # https://www.ura.org/pages/larimer-east-liberty-choice-neighborhood-initiative
        # East Liberty and Larimer Neighborhoods near the intersection of Larimer Avenue and East Liberty Blvd
        record['property_street_address'] = 'East Liberty and Larimer Neighborhoods near the intersection of Larimer Avenue and East Liberty Blvd'
        # The proposed 25-acre Larimer/East Liberty Park spans from Station Street and Larimer Avenue to the northern corner of the Larimer Neighborhood at Orphan Street and Larimer Avenue. The park will serve 4,177 residents who live within a five-minute walk.
        #    "334 units of high quality, mixed-income housing built in four phases over the next four years.
        #Phase I was completed in 2016 and consisted of 85 units in 18 buildings
        #Phase II began in October 2017 and consists of 150 units
        #Phases III & IV completes the final 99 units by 2021"
        record['latitude'] = '40.4634'
        record['longitude'] = '-79.9175'
        record['census_tract'] = '42003111500' # 2020 Census tract - looked up through geo.census.gov
    elif '800112244' == record['property_id']:
        assert record['property_street_address'] == '5653 BRD ST'
        record['property_street_address'] = '5653 BROAD ST'

    if False:
        record = try_to_geocode(record)
    return record


def compare_decimal_strings(value_1, value_2, digits):
    if '|' in value_1 or '|' in value_2: # Handle lists.
        return False, None
    if '.' not in value_1 or '.' not in value_2:
        return False, None
    integer_part_1, decimal_part_1 = value_1.split('.')
    integer_part_2, decimal_part_2 = value_2.split('.')
    if integer_part_1 == integer_part_2:
        if decimal_part_1[:digits] == decimal_part_2[:digits]:
            return True, f'{integer_part_1}.{decimal_part_1[:digits]}'
    return False, None

def merge(record_1, record_2, verbose):
    DIGITS_OF_PRECISION = 4
    merged_record = {}
    for key, value in record_1.items():
        other_value = record_2.get(key, None)
        # Remove extra spaces
        if type(value) == str:
            value = standardize_field(value, key)
        if type(other_value) == str:
            other_value = standardize_field(other_value, key)

        if key == 'index':
            merged_record[key] = min(value, other_value)
        elif other_value in [None, '']:
            merged_record[key] = value
        elif value in [None, '']:
            merged_record[key] = other_value
        elif key == 'scattered_sites':
            if value == other_value:
                merged_record[key] = value
            elif set([value, other_value]) == set(["TRUE", "FALSE"]):
                merged_record[key] = "TRUE"
            else:
                ic(value, other_value)
                ic(record_1)
                ic(record_2)
                assert 0
        elif value.upper() == other_value.upper():
            merged_record[key] = value
        elif key == 'source_file':
            source_files = sorted(value.split('|') + other_value.split('|'))
            merged_record[key] = '|'.join(source_files)
        elif key == 'census_tract':
            if value in ['42XXXXXXXXX', '']:
                merged_record[key] = other_value
            elif other_value in ['42XXXXXXXXX', '']:
                merged_record[key] = value
            else:
                #print(f"Since this code doesn't know how to merge key = {key}, value = {value}, other value = {record_2[key]}, it's just going to list both.")
                merged_record[key] = '|'.join(sorted([value, other_value]))

        elif key in ['latitude', 'longitude'] and compare_decimal_strings(value, other_value, DIGITS_OF_PRECISION)[0]:
            # Sometimes the geocoordinates from different sources differ by something like a floating
            # point error (or certainly an insignificant amount).
            merged_record[key] = compare_decimal_strings(value, other_value, DIGITS_OF_PRECISION)[1]
            # 4 decimal points is enough, according to XKCD:
            # https://xkcd.com/2170/
        elif re.match(value.upper(), other_value.upper()) is not None: # other_value starts with value
            merged_record[key] = other_value # Go with the longer version
        elif re.match(other_value.upper(), value.upper()) is not None:
            merged_record[key] = value
        else:
            if verbose:
                print(f"Since this code doesn't know how to merge key = {key}, value = {value}, other value = {record_2[key]}, it's just going to list both.")
            merged_record[key] = '|'.join(sorted([value, other_value]))

            #raise ValueError(f'What should we do with key = {key}, value = {value}, other value = {dups[1][key]}?')

    for key, value in record_2.items():
        if key not in merged_record or merged_record[key] == '':
            merged_record[key] = value

    merged_record = fix_some_records(record_1, record_2, merged_record, verbose)

    return merged_record

fields_to_get = ['hud_property_name',
        'property_street_address', 'municipality_name', 'city', 'zip_code',
        'contract_id', # mf_subsidy_ac
        'fha_loan_id',
        'normalized_state_id',
        'pmindx',
        'units',
        'scattered_sites',
        'latitude', 'longitude',
        'census_tract',
        'status'
        ]

possible_keys = ['property_id', 'lihtc_project_id', 'development_code', 'fha_loan_id', 'normalized_state_id', 'contract_id', 'pmindx', 'crowdsourced_id'] # 'inspection_property_id_multiformat']

index_filename = 'master_list.csv'
deduplicated_index_filename = 'deduplicated_index.csv'

def deduplicate_records(deduplicated_index_filepath, verbose=False):
    fields_to_write = fields_to_get
    for f in possible_keys:
        if f not in fields_to_write:
            fields_to_write.append(f)
    #########################
    # Load master list from file
    with open(index_filename, 'r') as f:
        reader = csv.DictReader(f)
        master_list = list(reader)

    for k, record in enumerate(master_list):
        record['index'] = k

    house_cat_id_name = 'house_cat_id'

    eliminated_indices = []

    # Index all records by all key fields
    master_by = defaultdict(lambda: defaultdict(int)) # master_by['property_id']['80000000'] = index of some record in master_list
    for n, record in enumerate(master_list):
        for key in possible_keys:
            if key in record:
                if record[key] not in ['', None]:
                    if master_by[key][record[key]] != 0: # Sound COLLISION.
                        if verbose:
                            print(f'Found another instance of key = {key}, value = {record[key]} already in the master list.')
                        if True: # merging routine
                            if verbose:
                                print(f'Attempting to merge these two records:')
                            already_indexed_n = master_by[key][record[key]]
                            record_1 = master_list[already_indexed_n]
                            #pprint(record_1)
                            #pprint(record)
                            merged_record = merge(record_1, record, verbose)

                            if verbose:
                                ic(already_indexed_n)
                                #ic(master_list[already_indexed_n])
                            master_list[already_indexed_n] = merged_record
                            eliminated_indices.append(n)
                            #ic(master_list[already_indexed_n])
                            #ic(merged_record)
                        else:
                            already_indexed_n = master_by[key][record[key]]
                            record_1 = master_list[already_indexed_n]
                            if verbose:
                                pprint(record_1)
                                pprint(record)
                        #assert False
                            raise ValueError("DO SOMETHING ABOUT THIS ONE! "*8)
                    #assert master_by[key][record[key]] == 0 # I think this might be fairly critical actually.
                    else:
                        master_by[key][record[key]] = n # This is the row number in the master list.
                    #if record['fha_loan_id'] == '03332013' and record['zip_code'] == '51212':
                    #    print("JUST SKIPPING THIS COLLISION FOR NOW!!!")
                    #    pass
                    #else:
                    #    assert master_by[key][record[key]] == 0 # I think this might be fairly critical actually.
                    #    master_by[key][record[key]] = n # This is the row number in the master list.

    # Load file that gives instructions for linking records based on IDs
    with open(f'unidirectional_links.csv', 'r') as g:
        reader = csv.DictReader(g)
        for row in reader:
            source_field = row['source_field']
            source_value = row['source_value']
            target_field = row['target_field']
            target_value = row['target_value']
            if source_field not in possible_keys + [house_cat_id_name]:
                if verbose:
                    ic(source_field)
                    ic(possible_keys + [house_cat_id_name])
            assert source_field in possible_keys + [house_cat_id_name]
            if target_field not in possible_keys + [house_cat_id_name]:
                if verbose:
                    ic(target_field)
            assert target_field in possible_keys + [house_cat_id_name]

            assert source_value != ''
            assert target_value != ''

            if row['relationship'] == 'needs': # target_field == house_cat_id_name:
                index = master_by[source_field][source_value]
                source_record = master_list[index]
                source_record[target_field] = target_value
            else:
                # Merge these two records
                index_1 = master_by[source_field][source_value]
                index_2 = master_by[target_field][target_value]
                if index_1 in eliminated_indices:
                    if verbose:
                        ic(index_1, source_field, source_value)
                        ic(row)
                #assert index_1 not in eliminated_indices # This seems like it's necessary because ic(master_by['lihtc_project_id']['PAA20133006'])
                #assert index_2 not in eliminated_indices # does not have the same information as ic(master_by['state_id']['TC20110313'])
                                                         # after the merging, though it should.

                # Just try skipping these:
                if index_1 in eliminated_indices:
                    if verbose:
                        print(f'index_1 = {index_1} has already been taken care of.')
                if index_2 in eliminated_indices:
                    if verbose:
                        print(f'index_2 = {index_2} has already been taken care of.')
                if index_1 in eliminated_indices or index_2 in eliminated_indices:
                    if verbose:
                        ic(row)
                else:
                    record_1 = master_list[index_1]
                    record_2 = master_list[index_2]

                    merged_record = merge(record_1, record_2, verbose)

                    master_list[index_1] = master_list[index_2] = merged_record
                    master_by[source_field][source_value] = min(index_1, index_2)
                    master_by[target_field][target_value] = min(index_1, index_2)
                    eliminated_indices.append(max(index_1, index_2))
                if verbose:
                    print("============")


    deduplicated_master_list = []
    added = []
    for field, remainder in master_by.items():
        for value, index in remainder.items():
            if index not in added and index not in eliminated_indices:
                added.append(index)
                deduplicated_master_list.append(fix_single_record(master_list[index]))

    write_to_csv(deduplicated_index_filepath, deduplicated_master_list, fields_to_write + [house_cat_id_name, 'source_file', 'index'])
    return master_list, deduplicated_master_list

if __name__ == '__main__':
    verbose = True
    index_list, deduplicated_index = deduplicate_records(deduplicated_index_filename, verbose)
    ic(len(index_list))
    ic(len(deduplicated_index))

# Bellefield Dwellings has one HUD Property ID 800018223, but two
# LIHTC Project ID values, one from 1988 and one from 2011.
# It also has two different state IDs and two different federal
# IDs. Thus, LIHTC Project IDs are just records of funding
# (as maybe most of these are in some way).

# Each of the LIHTC records gives some information (such as number of units).

# [X] Add these to unidirectional_links.csv when the code below knows how to deal
# with one-to-many links:
#lihtc_project_id,PAA19880440,funded,property_id,800018223,Bellefield Dwellings,This is the first LIHTC project funding link.
#lihtc_project_id,PAA20133006,funded,property_id,800018223,Bellefield Dwellings,This is the second LIHTC project funding link.

# [X] development_code,PA006000811,is another ID for,property_id,800237651,LAVENDER HEIGHTS I/Lavender Heights

# [X] PA006000822 (Carnegie Apartments Additions is a second LIHTC project that should be linked to the original 13-unit Carnegie Apartments)

# [X] PAA19890620 and PAA19910435 are two different LIHTC Project IDs for the same location (HIGHLAND AVE APARTMENTS).
# The funding was for 3 units and then 1 unit, respectively. But it's not clear if the 1 is part of the 3.

#lihtc_project_id,PAA19890620,funded,house_cat_id,15145-highland-ave-apartments,Highland Ave Apartments
#lihtc_project_id,PAA19910435,funded,house_cat_id,15145-highland-ave-apartments,Highland Ave Apartments

# Buildings and projects should be treated distinctly. Anything else?


###############################################################3
# Two records with fha_loan_id == '03335292' were found, one from mf_subsidy_loans_ac.csv|mf_loans.csv and one from mf_mortgages_pa.csv (based on the linking attempt that just added separate records rather than linking).
# The first two files both had a correct street address but the wrong ZIP code, while the second had the right ZIP code but a seemingly very wrong street address.
#hud_property_name      property_street_address         city        zip_code    fha_loan_id property_id source_file
#Oak Hill Brackenridge  537 Oak Hill Drive              Pittsburgh  15213       03335292    800244735   mf_subsidy_loans_ac.csv|mf_loans_ac.csv
#Oak Hill Brackenridge  120  209 & 239 Blakey Court  1  Pittsburgh  15219       03335292                mf_mortgages_pa.csv
# This will present a linking problem.
# One option: Make lists of all the conflicting address information.
# Another option: Use the (seemingly accurate) geocoordinates in mf_loans.

