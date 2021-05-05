import re, csv, copy
from pprint import pprint
from icecream import ic
from collections import defaultdict


def write_to_csv(filename, list_of_dicts, keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

fields_to_get = ['hud_property_name',
        'property_street_address', 'municipality_name', 'city', 'zip_code',
        'contract_id', # mf_subsidy_ac
        'fha_loan_id',
        'state_id',
        'units',
        'latitude', 'longitude',
        ]

possible_keys = ['property_id', 'lihtc_project_id', 'development_code', 'fha_loan_id', 'state_id'] # 'inspection_property_id_multiformat']

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

# Index all records by all key fields
master_by = defaultdict(lambda: defaultdict(int)) # master_by['property_id']['80000000'] = index of some record in master_list
for n, record in enumerate(master_list):
    for key in possible_keys:
        if key in record:
            if record[key] not in ['', None]:
#                if master_by[key][record[key]] != 0: # Sound COLLISION.
#                    print(f'Found another instance of key = {key}, value = {record[key]} already in the master list.')
#                    ic(master_by[key][record[key]])
#                    ic(record)

               
                assert master_by[key][record[key]] == 0
                master_by[key][record[key]] = n

eliminated_indices = []
# Load file that gives instructions for linking records based on IDs
with open(f'unidirectional_links.csv', 'r') as g:
    reader = csv.DictReader(g)
    for row in reader:
        source_field = row['source_field']
        source_value = row['source_value']
        target_field = row['target_field']
        target_value = row['target_value']
        assert source_field in possible_keys + [house_cat_id_name]
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
            assert index_1 not in eliminated_indices # This seems like it's necessary because ic(master_by['lihtc_project_id']['PAA20133006'])
            assert index_2 not in eliminated_indices # does not have the same information as ic(master_by['state_id']['TC20110313'])
                                                     # after the merging, though it should.
            merged_record = {}
            record_1 = master_list[index_1]
            record_2 = master_list[index_2]


            #merged_record = merge(record_1, record_2)
            for key, value in record_1.items():
                other_value = record_2.get(key, None)
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
                    ic(record_1)
                    ic(record_2)
                    print(f"Since this code doesn't know how to merge key = {key}, value = {value}, other value = {record_2[key]}, it's just going to list both.")
                    merged_record[key] = f'{value}|{other_value}'

                    #raise ValueError(f'What should we do with key = {key}, value = {value}, other value = {dups[1][key]}?')

            for key, value in record_2.items():
                if key not in merged_record or merged_record[key] == '':
                    merged_record[key] = value

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

