import re, csv, copy, requests, time, json
from datetime import datetime
from pprint import pprint
from icecream import ic
from collections import defaultdict

GEOCACHE_FILE = 'geocache.json'
def load_geocache():
    with open(GEOCACHE_FILE, 'r') as f:
        global cached_geocodings
        cached_geocodings = json.load(f)

def save_geocache():
    with open(GEOCACHE_FILE, 'w') as g:
        global cached_geocodings
        g.write(json.dumps(cached_geocodings))

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

def geocode(address):
    global cached_geocodings
    if address in cached_geocodings:
        return cached_geocodings[address]

    geocoordinates = geocode_address_with_geomancer(address)
    if geocoordinates != (None, None):
        cached_geocodings[address] = geocoordinates
    return geocoordinates

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
        longitude, latitude = geocode(address)
        if latitude is None and not no_municipality:
            longitude, latitude = geocode(alternate_address)
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
            record['latitude'] = '40.446'
            record['longitude'] = '-79.979'
            record['census_tract'] = '42003050100'
            record['census_tract_2010'] = '42003050100'
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
        if re.search('5653 BRD ST', record['property_street_address']):
            record['property_street_address'] = re.sub('5653 BRD ST', '5653 BROAD ST', record['property_street_address'])
    elif 'TC1989-0042' == record['normalized_state_id']:
        record['status'] = 'Closed'
        record['property_street_address'] = '2561 Allequippa St'
        record['city'] = 'Pittsburgh'
        record['zip_code'] = '15213'
        # Was the address missing because this is Closed or just because the location is old?
    elif 'TC1989-0248' == record['normalized_state_id']:
        record['property_street_address'] = '1717 WEILER ST'
        record['city'] = 'NORTH BRADDOCK'
        record['latitude'] = '40.3999'
        record['longitude'] = '-79.8430'
        record['census_tract'] = '42003512000' # 2020 Census tract - looked up through geo.census.gov
        record['census_tract_2020'] = '42003512000'
    elif 'TC1989-0248' == record['normalized_state_id']:
        #record['property_street_address'] = '701 Swissvale Ave'
        record['scattered_sites'] = 'TRUE'
    elif 'TC1991-0100' == record['normalized_state_id']:
        record['municipality'] = 'SWISSVALE'
        record['city'] = 'PITTSBURGH'
        record['zip_code'] = '15218'
    elif '6428' == record['pmindx']:
        record['property_street_address'] = '1220 SHEFFIELD ST'
    elif '8892' == record['pmindx']:
        # "Negley Neighbors is located in the East Liberty area of Pittsburgh, providing affordable housing in 11 buildings scattered throughout the neighborhood. All apartments have been renovated and provide a full appliance package, blinds, and carpeting, while maintaining the old world charm with 1, 2 and 3 bedrooms available. Income Restricted."
        # https://ndcassetmanagement.com/property/negley-neighbors/
        record['property_street_address'] = '744 NORTH NEGLEY AVE'
        record['hud_property_name'] = 'NEGLEY NEIGHBORHOOD APTS (a.k.a. NEGLEY NEIGHBORS)'
    elif 'TC2006-0428' == record['normalized_state_id']:
        # https://ndcassetmanagement.com/property/east-braddock-apartments-mvi/
        # "A 17 unit affordable apartment community located in Braddock and East Pittsburgh. The Braddock area is undergoing a major revitalization and most of our units are within walking distance of all the exciting changes. This property includes 1 and 2 bedroom apartments and 3 bedroom townhouses."
        record['property_street_address'] = '633 COREY AVE'
        record['zip_code'] = '15104'
        record['latitude'] = '40.4042'
        record['longitude'] = '-79.8683'

    elif '9941' == record['pmindx']:
        # The real cordinates are not known yet.
        record['latitude'] = '40.462'
        record['longitude'] = '-79.920'
    elif '800018254' == record['property_id']:
        record['property_street_address'] = '479 BANK ST'
        record['latitude'] = '40.3572'
        record['longitude'] = '-80.1120'
        record['census_tract'] = '42003457100' # 2020 Census tract from gecoding.geo.census.gov
        record['census_tract_2010'] = '42003457100'
    elif '800018792' == record['property_id']:
        record['property_street_address'] = '111 RIDGEMEAD FIELDS DR'
    elif '10541' == record['pmindx']:
        record['property_street_address'] = '2007 CENTER AVE' # This is an approximate address, as the construction is not done yet.
        record['latitude'] = '40.444'
        record['longitude'] = '-79.980'
    elif '7076' == record['pmindx']:
        record['latitude'] = '40.4642'
        record['longitude'] = '-79.8972'

    elif '10523' == record['pmindx']:
        record['latitude'] = '40.4636'
        record['longitude'] = '-79.9262'
        # Coordinates estimated from this planning PDF: https://www.eastliberty.org/wp-content/uploads/2019/12/2019.10.04-HBH.Draft-Plans.pdf
        # [ ] This project has applied for LIHTC funding, but it's not known whether it was awarded as of 2021-09-18.
    elif '10423' == record['pmindx']:
        record['latitude'] = '40.4641'
        record['longitude'] = '-79.9262'
        # Coordinates estimated from this PDF: https://www.eastliberty.org/wp-content/uploads/2019/12/Mellons-Orchard-Investor-Set_Plans-pages-deleted_compressed-1.pdf
        #There are 5 buildings, the eastmost two of which are labelled as "live/work apartments".
        # [ ] This project has received LIHTC funding, but there is no record of it in our LIHTC data as of 2021-09-18.
    elif '10363' == record['pmindx']:
        record['latitude'] = '40.4564'
        record['longitude'] = '-79.9010'
        record['census_tract'] = '42003130800' #2020 Census tract from gecoding.geo.census.gov
        record['census_tract_2010'] = '42003130300'
    elif '10300' == record['pmindx']:
        record['latitude'] = '40.4295'
        record['longitude'] = '-79.9235'
        record['census_tract'] = '42003141400'  #2020 Census tract from gecoding.geo.census.gov
        record['census_tract_2010'] = '42003141400'
    elif '800018210' == record['property_id']:
        record['latitude'] = '40.3728'
        record['longitude'] = '-80.0329'
        record['census_tract'] = '42003473300' #2020 Census tract from gecoding.geo.census.gov
        record['census_tract_2010'] = '42003473300'
    elif '800018375' == record['property_id']:
        #record['property_street_address'] = '
        record['status'] = 'Either closed or renamed/moved'
        record['latitude'] = '40.461'
        record['longitude'] = '-79.920'
        record['census_tract'] = '42003111500' # 2020 Census tract from gecoding.geo.census.gov
        record['census_tract_2010'] = '42003111500'
    elif '800018381' == record['property_id']:
        record['latitude'] = '40.3821'
        record['longitude'] = '-79.8277'
        record['census_tract'] = '42003504100' # 2020 Census tract from gecoding.geo.census.gov
        record['census_tract_2010'] = '42003504100'
    elif '800018470' == record['property_id']:
        record['latitude'] = '40.4269'
        record['longitude'] = '-79.9430'
        record['census_tract'] = '42003151600' # 2020 Census tract from gecoding.geo.census.gov
        record['census_tract_2010'] = '42003151600'
    elif '800018496' == record['property_id']:
        record['property_street_address'] = '2006 BROADVIEW BLVD'
        if record['hud_property_name'] == 'HARRISON HIRISE':
            record['hud_property_name'] == 'HARRISON HI-RISE'
    elif '800018540' == record['property_id']:
        record['latitude'] = '40.4758'
        record['longitude'] = '-79.9599'
        record['census_tract'] = '42003090100' # 2020 Census tract from gecoding.geo.census.gov
        record['census_tract_2010'] = '42003090100'
    elif '800018669' == record['property_id']:
        record['latitude'] = '40.3507'
        record['longitude'] = '-79.8620'
        record['census_tract'] = '42003551900'
        record['census_tract_2010'] = '42003551900'
    elif '800018944' == record['property_id']:
        record['latitude'] = '40.46713'
        record['longitude'] = '-79.9936'
        record['census_tract'] = '42003262000'
        record['census_tract_2010'] = '42003262000'
    elif '800214916' == record['property_id']: # Miller Ave (Senior) Apartments
        record['hud_property_name'] = 'MILLER AVENUE SENIOR APARTMENTS' # Not to be confused with Miller Ave Apts at 15 Miller Ave.
        record['property_street_address'] = '16 MILLER AVE' # Changed from "1600 MILLER AVE" based on Google Maps + Street View
        record['latitude'] = '40.3834'
        record['longitude'] = '-79.8599'
        record['census_tract'] = '42003486700'
        record['census_tract_2010'] = '42003486700'
    elif '800214963' == record['property_id']:
        record['latitude'] = '40.39121'
        record['longitude'] = '-80.0151'
        record['census_tract'] = '42003191800'
        record['census_tract_2010'] = '42003191800'
    elif '800217778' == record['property_id']:
        record['latitude'] = '40.4143'
        record['longitude'] = '-79.9919'
        record['census_tract'] = '42003300100'
        record['census_tract_2010'] = '42003300100'
    elif '800219089' == record['property_id']:
        record['latitude'] = '40.4644'
        record['longitude'] = '-79.9301'
        record['census_tract'] = '42003111500'
        record['census_tract_2010'] = '42003111500'
    elif '800222535' == record['property_id']:
        record['latitude'] = '40.6451'
        record['longitude'] = '-80.0824'
        record['census_tract'] = '42003410000'
        record['census_tract_2010'] = '42003410000'
    elif '800224938' == record['property_id']:
        record['hud_property_name'] = 'VANTAGE COURT SENIOR HOUSING'
        record['latitude'] = '40.4075'
        record['longitude'] = '-79.9090'
        record['census_tract'] = '42003483800'
        record['census_tract_2010'] = '42003483800'
    elif 'TC1990-0181' == record['normalized_state_id']:
        record['scattered_sites'] = 'TRUE'
        # Kenyon Properties II appears to be a LIHTC-funded, 28-unit
        # housing project in Wilkinsburg.
        # Some web sites say that it scattered-site:
        # https://affordablehousingonline.com/housing-search/Pennsylvania/Wilkinsburg/Kenyon-Properties-II/10075716

        # Kenyon Properties I has the same LIHTC configuration but has
        # a property_street_address of 701 Swissvale Ave, Wilkinsburg.
        # That building does not look like it can hold 28 apartments, so
        # I suspect that this is also a scattered-site project.
        # I'm just going to put Kenyon Properties II near Kenyon Properties I
        # with large error bars (low precision).
        record['latitude'] = '40.44'
        record['longitude'] = '-79.88'
        record['census_tract'] = '42003564700'
        record['census_tract_2010'] = '42003561000'

    elif 'TC1990-0180' == record['normalized_state_id']:
        record['scattered_sites'] = 'TRUE'
    elif '10285' == record['pmindx']:
        record['latitude'] = '40.4406'
        record['longitude'] = '-79.9831'
        record['census_tract'] = '42003030500'
        record['census_tract_2010'] = '42003030500'
    elif '10517' == record['pmindx']:
        # https://bloomfield-garfield.org/wp-content/uploads/2021/01/BGC-Affordable-Housing-Initiatives.pdf
        # "Garfield Highlands – Proposed new construction of 25 scattered-site homes in 5300 block of
        # Kincaid St., 5300 and 5400 blocks of Rosetta St., 200, 300 and 400 blocks of N. Aiken Avenue,
        # 5300 block of Hillcrest St. Would be a mix of two- and three-bedroom homes, affordable to
        # families earning less than $48,000/year. Awaiting a decision from PA Housing Finance Agency
        # on allocation of federal tax credits for the project. Will need U.R.A. to furnish a loan for $1
        # million+. Construction could be underway by summer 2021; "
        if datetime.now().year == '2021':
            record['status'] = 'Under construction'
        record['scattered_sites'] = 'TRUE'
        if 'units' not in record or record['units'] in [None, '']:
            record['units'] = '25'
        record['latitude'] = '40.4670' # This is just the interscetion of N. Aiken and Rosetta, which
        record['longitude'] = '-79.9350' # seems like a decent centroid.
        record['census_tract'] = '42003111400'
        record['census_tract_2010'] = '42003111400'
    elif '9573' == record['pmindx']: # Garfield Glen
        # https://bloomfield-garfield.org/wp-content/uploads/2021/01/BGC-Affordable-Housing-Initiatives.pdf
        # BGC carries out the role of owner or partner in the following developments:
        # Garfield Glen I and II – 64 rental homes built between N. Mathilda and N. Pacific Aves
        # ● Partnered with S & A Homes on both developments – Managed by NDC Real Estate
        # ● BGC reviews major management decisions around rental policies, evictions, and
        # additional investments in the homes.
        # ● BGC is the first point of contact for tenants or neighbors wishing to speak with the
        # owners.
        # ● At the end of initial 15-year leasing period, BGC will control 51% of the ownership
        # interest in the developments, and S & A 49%. Tenants will have the option to buy
        # their homes outright in year 16.
        record['scattered_sites'] = 'TRUE'
        record['hud_property_name'] = 'SCATTERED SITES IN GARFIELD BETWEEN N. MATHILDA AND N. PACIFIC AVES'
        record['latitude'] = '40.46683'
        record['longitude'] = '-79.94242'
        record['census_tract'] = '42003101900'
        record['census_tract_2010'] = '42003101700'
    elif '9324' == record['pmindx']:
        record['property_street_address'] = re.sub('\|SCHENLEYFERN & COLUMBO STS', '', record['property_street_address'])
    elif '800233043' == record['property_id']:
        record['property_street_address'] = '6201 BROAD ST'
        record['hud_property_name'] = 'FAIRFIELD APTS (PREVIOUSLY LIBERTY PARK)'
    elif '800241361' == record['property_id']:
        record['property_street_address'] = re.sub('BRDVIEW', 'BROADVIEW', record['property_street_address'])
    elif '800018460' == record['property_id']:
        record['hud_property_name'] = 'HILLSBORO PLAZA (PREVIOUSLY GOODWILL PLAZA)'
        # https://st-residential.com/hillsboro-plaza
        # Hillsboro Plaza is a 62 and over, income based independent living facility located in the Sheraden neighborhood.
        # The eight-story building consists of 72, efficiency and one bedroom apartments, a community room and laundry room.
        # 72 Senior Apartment Homes with Section 8 rental assistance
    elif '800018889' == record['property_id']:
        record['hud_property_name'] = 'ST AUGUSTINE PLAZA'
    elif '800018984' == record['property_id']:
        record['property_street_address'] = re.sub('RAILRD', 'RAILROAD', record['property_street_address'])
    elif '9946' == record['pmindx']:
        assert record['property_street_address'] == 'SCATTERED SITES'
        record['property_street_address'] = re.sub('SCATTERED SITES', 'SCATTERED SITES (DINWIDDIE AND MILLER STREETS)', record['property_street_address'])
        # From https://www.phfa.org/forms/multifamily_news/awards/2015/2015_app_log_1.pdf
    elif '10178' == record['pmindx']: # MORNINGSIDE CROSSING
        record['units'] = '46' # https://www.phfa.org/forms/multifamily_news/awards/2015/2015_app_log_1.pdf
    elif '7089' == record['pmindx']: # LAMBETH APTS which closed, but has been replaced by YORK COMMONS.
        record['status'] = 'Closed'
    elif '800244735' == record['property_id']: # OAK HILL BRACKENRIDGE
        # looks to be still under construction based on
        # page 2 of this PDF (a map)
        # and Google Maps aerial photos (circa 2021)
        # AND the fact that the HUD data says that it has zero units.
        if record['units'] in ['0', 0]:
            record['status'] = 'Under construction'

    elif '10506' == record['pmindx']:
        record['hud_property_name'] = re.sub(' CNI ', ' CHOICE NEIGHBORHOODS INITIATIVE ', record['hud_property_name'])
    elif '03335246' in record['fha_loan_id']:
        record['hud_property_name'] = re.sub('H.J. HEINZ LOFTS|H.J.HEINZ LOFTS', 'H.J. HEINZ LOFTS', record['hud_property_name'])
        record['property_street_address'] = '300 HEINZ ST'
        record['zip_code'] = '15212'
        record['latitude'] = '40.4540'
        record['longitude'] = '-79.9910'
        record['census_tract'] = record['census_tract_2020'] = '42003241300'
    elif '800030516' == record['property_id']:
        record['hud_property_name'] = "HARMAR VILLAGE CARE CENTER"
    elif '03311141' == record['fha_loan_id']:
        record['property_street_address'] = '1001 PARKVIEW BLVD'
        record['zip_code'] = '15217'
        record['municipality_name'] = 'PITTSBURGH'
        record['latitude'] = '40.4247'
        record['longitude'] = '-79.9099'
        record['census_tract'] = record['census_tract_2020'] = '42003141400'
    elif '03322094' == record['fha_loan_id']:
        record['property_street_address'] = '5347 BRIGHTWOOD RD'
        record['zip_code'] = '15102'
        record['status'] = 'Closed'
    elif '03335264' in record['fha_loan_id']:
        record['property_street_address'] = '7237 BEACON HILL DR'
        record['municipality_name'] = 'WILKINSBURG'
        record['zip_code'] = '15221'
        record['latitude'] = '40.4432'
        record['longitude'] = '-79.8644'
        record['census_tract'] = record['census_tract_2020'] = '42003561500'
    elif '03335240' in record['fha_loan_id']: # DAVISON SQUARE APTS
        record['property_street_address'] = '265 46TH ST'
        record['zip_code'] = '15201'
        record['latitude'] = '40.4713'
        record['longitude'] = '-79.9567'
        record['census_tract'] = record['census_tract_2020'] = '42003090200'
    elif '03311111' in record['fha_loan_id']: # HERITAGE HILLS
        record['property_street_address'] = '1250 VILLAGE GREEN DR'
        record['zip_code'] = '15025'
        record['latitude'] = '40.3196'
        record['longitude'] = '-79.9520'
        record['census_tract'] = record['census_tract_2020'] = '420034911011'
    elif '03335282' in record['fha_loan_id']: # WATERFORD LANDING APTS # This is a neighborhood.
        record['property_street_address'] = '1200 LANDING LN'
        record['zip_code'] = '15108'
        record['latitude'] = '40.525'
        record['longitude'] = '-80.226'
    elif '03310014' in record['fha_loan_id']: # HIGHLAND PARK CARE CENTER
        record['property_street_address'] = '745 N HIGHLAND AVE'
        record['zip_code'] = '15206'
        record['latitude'] = '40.4691'
        record['longitude'] = '-79.9213'
        record['census_tract'] = record['census_tract_2020'] = '42003111300'
    elif '03322037' in record['fha_loan_id']: # WOODCLIFFE MANOR ASSISTED LIVING FACILITY
        record['property_street_address'] = '5347 BRIGHTWOOD RD'
        record['zip_code'] = '15102'
        record['status'] = 'Closed'
    elif '03322033' in record['fha_loan_id']: # LOCUST GROVE
        record['property_street_address'] = '4043 IRENE ST'
        record['zip_code'] = '15122'
        record['latitude'] = '40.3911'
        record['longitude'] = '-79.8800'
        record['census_tract'] = record['census_tract_2020'] = '42003488200'
    elif '03311050' in record['fha_loan_id']: # MT VERNON APARTMENTS
        record['property_street_address'] = '2300 SURREY LN'
        record['zip_code'] = '15135'
        record['latitude'] = '40.2916'
        record['longitude'] = '-79.8256'
        record['census_tract'] = record['census_tract_2020'] = '42003496200'
    elif '03311038' in record['fha_loan_id']: # The Flats at Fox Hill Apartments (4 buildings)
        record['hud_property_name'] = 'THE FLATS AT FOX HILL APARTMENTS (4 BUILDINGS)'
        record['property_street_address'] = '1120 FOX HILL DR'
        record['zip_code'] = '15146'
        record['latitude'] = '40.4404'
        record['longitude'] = '-79.7830'
        record['census_tract'] = record['census_tract_2020'] = record['census_tract_2010'] = '42003521200'

        record['building_count'] = 4
    elif '03335257' in record['fha_loan_id']: # HEP PARK VILLAGE APTS - close to LGAR
        record['property_street_address'] = '342 KENYON STREET'
        record['zip_code'] = '15145'
        record['latitude'] = '40.4106'
        record['longitude'] = '-79.8231'
        record['census_tract'] = record['census_tract_2020'] = record['census_tract_2010'] = '42003509400'
    elif '03335251' in record['fha_loan_id']: # WESTPOINTE APTS I AND II
        record['property_street_address'] = '2000 WESTPOINTE DR'
        record['zip_code'] = '15205'
        record['latitude'] = '40.4518'
        record['longitude'] = '-80.1532'
        record['census_tract'] = record['census_tract_2020'] = record['census_tract_2010'] = '42003459201'
    elif '03335253' in record['fha_loan_id']: # SOUTHPOINTE TOWERS (constructed in 1976 and has 157 units)
        record['property_street_address'] = '100 CERASI DR'
        record['municipality_name'] = 'WEST MIFFLIN'
        record['zip_code'] = '15122'
        record['latitude'] = '40.3310'
        record['longitude'] = '-79.9404'
        record['census_tract'] = record['census_tract_2020'] = '42003489001'
    elif '03322029' in record['fha_loan_id']: # LEMINGTON HOME FOR THE AGED (Closed in 2005. Under redevelopment as a 54 one-bedroom units for senior citizens.)
        record['property_street_address'] = '1625 LINCOLN AVE'
        record['zip_code'] = '15206'
        record['latitude'] = '40.4721'
        record['longitude'] = '-79.8933'
        record['census_tract'] = record['census_tract_2020'] = '42003561900'
        record['status'] = 'Closed (but being redeveloped as affordable housing)'
    elif '03343110' == record['fha_loan_id']: # AMBER HOLLOW
        record['status'] = 'Unfindable' # Is this the same as AMBER GLEN?
    elif '03335252' == record['fha_loan_id']: # SOUTH PARK APTS
        record['status'] = 'Unfindable' # It's not clear where this is.
    elif '03310010' == record['fha_loan_id']:  # AMBER WOODS AT HARMARVILLAGE
        record['property_street_address'] = '715 FREEPORT RD'
        record['city'] = 'CHESWICK'
        record['municipality_name'] = 'CHESWICK'
        record['zip_code'] = '15024'
        record['latitude'] = '40.5427'
        record['longitude'] = '-79.8191'
        record['census_tract'] = record['census_tract_2020'] = '42003419000'
    elif '800018743' == record['property_id']: # ST. CLAIR WOODS APTS
        record['city'] = 'PITTSBURGH'
        record['latitude'] = '40.3608'
        record['longitude'] = '-80.0678'
        record['census_tract'] = record['census_tract_2020'] = '42003473602'
    elif '800224287' == record['property_id']:
        record['hud_property_name'] = 'INDEPENDENCE COURT OF MONROEVILLE'
    elif '800030495' == record['property_id']:
        record['hud_property_name'] = 'PRESBYTERIAN MEDICAL CENTER OF OAKMONT'
#    elif '800018175' in [record_1['property_id'], record_2['property_id']]:
#        #merged_record['units'] = 126 # [ ] Need to distinguish between 130 and 136 units.
#        #assert str(merged_record['units']) in record_1['units'].split('|') + record_2['units'].split('|') # Guard against units drift.
    elif '800018175' == record['property_id']:
        record['hud_property_name'] = 'ALLEGHENY COMMONS EAST'
    elif '800244929' == record['property_id']:
        record['hud_property_name'] = 'LARIMER / EAST LIBERTY PHASE II'
    elif '800018598' == record['property_id']:
        record['hud_property_name'] = 'LEETSDALE HI-RISE'
    elif '800018903' == record['property_id']:
        record['hud_property_name'] = 'ST. THERESE PLAZA'
    elif '800240553' == record['property_id']:
        record['hud_property_name'] = 'THE OAKS RETIREMENT RESIDENCE'
    elif '800030501' == record['property_id']:
        record['hud_property_name'] = 'HIGHLAND PARK CARE CENTER'

# Brighton Place (TC1991-0087)
# It's completely unclear where this project is/was; somewhere in Pittsburgh is all the data tells us.
# Since there's a separate, seemingly newer project called "NORTHSIDE COALITION" which is located at 1500 Brighton Place,
# there's reason to suspect that the first became the second at some point.


# Allequippa Terrace Phase 1B
# PAA00000014
# The 36 acres in dispute are where the dilapidated Allequippa Terrace public housing complex stood. It was torn down in the late 1990s and replaced by what is called Oak Hill phase one -- 632 rental units, some with rent subsidized for lower-income people and some at market rate, along with seven for-sale houses.
# https://old.post-gazette.com/neigh_city/20030528oakhillc2.asp

# Hawthorne Place Apartments (TC1994-0155)
# should be changed to this range:
# 2253-2271 1/2 Hawthorne Ave.
# "Hawthorne Place is a 20 unit community that is all 2 bedroom units. Located in Swissvale this property is within walking distance of the busway and the near by Edgewood Town Center. This property is also located near the parkway east."

# Grant Street Renaissance (TC1994-0075)
# should be changed to this range:
# 221, 227, 301, & 302 Grant St.
# 307 Sarah Street
#https://ndcassetmanagement.com/property/grant-street-renaissance-mvi/

# Falconhurst Restoration
# TC2015-0408
# c/o Cresent Apartments
#736-738 Rebecca St.
#
#724 Kelly Ave.
#
#Mulberry St.
#
#Wilkinsburg, PA 15221
# Falconhurst is a multi-family Tax Credit scattered Community. We have 6 buildings that make of Falconhurst all located in Wilkinsburg. We offer 1, 2 and 3 bedrooms


# Crescent Apartments
# 736 & 738 Rebecca St.
# The restoration of these two buildings brought 27 units of affordable housing back to Wilkinsburg, with 23 units in the Crescent Apartments and 4 units in the Wilson House.
# https://ndcassetmanagement.com/property/crescent-apartments/

    if True:
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
            merged_record['all_keys'] = '|'.join(str(value).split('|') + str(other_value).split('|'))
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
    load_geocache()
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
    default_index = None
    master_by = defaultdict(lambda: defaultdict(lambda: default_index)) # master_by['property_id']['80000000'] = index of some record in master_list
    for n, record in enumerate(master_list):
        for key in possible_keys:
            if key in record:
                if record[key] not in ['', None]:
                    if master_by[key][record[key]] != default_index: # Sound COLLISION.
                        if verbose:
                            print(f'Found another instance of key = {key}, value = {record[key]} already in the master list.')
                        already_indexed_n = master_by[key][record[key]]
                        assert already_indexed_n not in eliminated_indices
                        if already_indexed_n != n: # merging routine
                            record_1 = master_list[already_indexed_n]
                            merged_record = merge(record_1, record, verbose)

                            # When this merge happens, the code should compensate by reviewing the master_by mapping for any
                            # other fields in the records TO BE merged, and ensuring that they point to the new index,
                            # being careful to think through what happens when field values from two records get listed
                            # in the merged record.
                            for key_i in possible_keys:
                                if record_1[key_i] != '': # Don't index null IDs.
                                    index_n_i = master_by[key_i][record_1[key_i]]
                                    if index_n_i not in [default_index, n]: # We found a different
                                        # field that has already been indexed.
                                        # That index pointer needs to be adjusted
                                        # to point to the merged record:
                                        id_string = record_1[key_i]
                                        for id_j in id_string.split('|'): # Deserialize IDs.
                                            master_by[key_i][id_j] = n # Repoint all these references to position n
                                            # in the master_list since that's where the merged record is going to go.

                            master_list[already_indexed_n] = None
                            master_list[n] = record = merged_record
                            eliminated_indices.append(already_indexed_n)
                        else:
                            pass # If already_indexed_n == n, this record has already been handled.

                    else:
                        id_string = record[key]
                        if id_string != '':
                            for id_j in id_string.split('|'): # Deserialize IDs.
                                master_by[key][id_j] = n  # This is the row number in the master list.

    with open(f'unidirectional_links.csv', 'r') as g:
        reader = csv.DictReader(g)
        for row in reader:
            source_field = row['source_field']
            if source_field[0] == '#': # Treat these lines as
                continue               # commented out.
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
                if index_1 != index_2: # Since we don't need to merge a record with itself.

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
                        master_by[source_field][source_value] = max(index_1, index_2)
                        master_by[target_field][target_value] = max(index_1, index_2)
                        eliminated_indices.append(min(index_1, index_2))
                    if verbose:
                        print("============")


    deduplicated_master_list = []
    added = []
    for field, remainder in master_by.items():
        for value, index in remainder.items():
            if index not in added and index not in eliminated_indices and index != default_index:
                added.append(index)
                deduplicated_master_list.append(fix_single_record(master_list[index]))

    write_to_csv(deduplicated_index_filepath, deduplicated_master_list, fields_to_write + [house_cat_id_name, 'source_file', 'index'])
    save_geocache()
    return master_list, deduplicated_master_list

if __name__ == '__main__':
    verbose = False
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

