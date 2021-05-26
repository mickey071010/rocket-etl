import re, csv, copy
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
            ' HTS ': ' HEIGHTS',
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

def merge(record_1, record_2):
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
        elif other_value is None:
            merged_record[key] = value
        elif value.upper() == other_value.upper():
            merged_record[key] = value
        elif key == 'source_file':
            source_files = sorted(value.split('|') + other_value.split('|'))
            merged_record[key] = '|'.join(source_files)
        elif re.match(value.upper(), other_value.upper()) is not None: # other_value starts with value
            merged_record[key] = other_value # Go with the longer version
        elif re.match(other_value.upper(), value.upper()) is not None:
            merged_record[key] = value
        else:
            print(f"Since this code doesn't know how to merge key = {key}, value = {value}, other value = {record_2[key]}, it's just going to list both.")
            merged_record[key] = f'{value}|{other_value}'

            #raise ValueError(f'What should we do with key = {key}, value = {value}, other value = {dups[1][key]}?')

    for key, value in record_2.items():
        if key not in merged_record or merged_record[key] == '':
            merged_record[key] = value

    return merged_record

fields_to_get = ['hud_property_name',
        'property_street_address', 'municipality_name', 'city', 'zip_code',
        'contract_id', # mf_subsidy_ac
        'fha_loan_id',
        'normalized_state_id',
        'pmindx',
        'units',
        'latitude', 'longitude',
        ]

possible_keys = ['property_id', 'lihtc_project_id', 'development_code', 'fha_loan_id', 'normalized_state_id', 'contract_id', 'pmindx'] # 'inspection_property_id_multiformat']

########################
fields_to_write = fields_to_get
for f in possible_keys:
    if f not in fields_to_write:
        fields_to_write.append(f)
#########################
# Load master list from file
with open('master_list.csv', 'r') as f:
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
                    print(f'Found another instance of key = {key}, value = {record[key]} already in the master list.')
                    if True: # merging routine
                        print(f'Attempting to merge these two records:')
                        already_indexed_n = master_by[key][record[key]]
                        record_1 = master_list[already_indexed_n]
                        #pprint(record_1)
                        #pprint(record)
                        merged_record = merge(record_1, record)

                        ic(already_indexed_n)
                        ic(master_list[already_indexed_n])
                        master_list[already_indexed_n] = merged_record
                        eliminated_indices.append(n)
                        #ic(master_list[already_indexed_n])
                        #ic(merged_record)
                    else:
                        already_indexed_n = master_by[key][record[key]]
                        record_1 = master_list[already_indexed_n]
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
            ic(source_field)
            ic(possible_keys + [house_cat_id_name])
        assert source_field in possible_keys + [house_cat_id_name]
        if target_field not in possible_keys + [house_cat_id_name]:
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
                ic(index_1, source_field, source_value)
                ic(row)
            #assert index_1 not in eliminated_indices # This seems like it's necessary because ic(master_by['lihtc_project_id']['PAA20133006'])
            #assert index_2 not in eliminated_indices # does not have the same information as ic(master_by['state_id']['TC20110313'])
                                                     # after the merging, though it should.

            # Just try skipping these:
            if index_1 in eliminated_indices:
                print(f'index_1 = {index_1} has already been taken care of.')
            if index_2 in eliminated_indices:
                print(f'index_2 = {index_2} has already been taken care of.')
            if index_1 in eliminated_indices or index_2 in eliminated_indices:
                ic(row)
            else:
                record_1 = master_list[index_1]
                record_2 = master_list[index_2]

                merged_record = merge(record_1, record_2)

                master_list[index_1] = master_list[index_2] = merged_record
                master_by[source_field][source_value] = min(index_1, index_2)
                master_by[target_field][target_value] = min(index_1, index_2)
                eliminated_indices.append(max(index_1, index_2))
            print("============")


deduplicated_master_list = []
added = []
for field, remainder in master_by.items():
    for value, index in remainder.items():
        if index not in added and index not in eliminated_indices:
            added.append(index)
            deduplicated_master_list.append(master_list[index])

write_to_csv('deduplicated_master_list.csv', deduplicated_master_list, fields_to_write + [house_cat_id_name, 'source_file', 'index'])
ic(len(master_list))
ic(len(deduplicated_master_list))
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

