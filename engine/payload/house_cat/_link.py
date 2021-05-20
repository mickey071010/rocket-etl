import re, csv, copy
from pprint import pprint
from icecream import ic
from collections import defaultdict

#from parameters.local_parameters import DESTINATION_DIR
DESTINATION_DIR = "/Users/drw/WPRDC/etl/rocket-etl/output_files/"

path = DESTINATION_DIR + "house_cat"

possible_keys = ['property_id', 'lihtc_project_id', 'development_code', 'fha_loan_id', 'state_id'] # 'inspection_property_id_multiformat']

def write_to_csv(filename, list_of_dicts, keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def get_files_in_folder(path):
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
    return onlyfiles

def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))

def add_row_to_linking_dict(f, row, id_field, fields_to_get, ac_by_id):
    try:
        ac_by_id[row[id_field]][id_field] = row[id_field]
    except:
        ic(ac_by_id)
        ic(row)
        ic(id_field, row[id_field])
        raise
    if 'source_file' not in ac_by_id[row[id_field]]:
        ac_by_id[row[id_field]]['source_file'] = f
    else:
        xs = ac_by_id[row[id_field]]['source_file'].split('|')
        ac_by_id[row[id_field]]['source_file'] = '|'.join(list(set(xs + [f])))

    for field in fields_to_get:
        if field in row and row[field] not in [None, '']:
            ac_by_id[row[id_field]][field] = row[field]

all_files = get_files_in_folder(path)
files = [f for f in all_files if f[-3:].lower() == 'csv']
keys_by_file = defaultdict(list)
files_by_key = defaultdict(list)

files_by_property_id = defaultdict(list)
files_by_development_code = defaultdict(list)
city_by_property_id = defaultdict(str)
city_by_fha_loan_id = defaultdict(str)
city_by_dev_code = defaultdict(str)

files_by = defaultdict(lambda: defaultdict(list))

for f in files:
    with open(f'{path}/{f}', 'r') as g:
        reader = csv.DictReader(g)
        columns = reader.fieldnames
        available_keys = intersection(possible_keys, columns)
        keys_by_file[f] = available_keys
        for key in available_keys:
            files_by_key[key].append(f)

        for row in reader:
            fields = ['property_id', 'development_code', 'fha_loan_id']
            for field in fields:
                if field in available_keys:
                    if field == 'property_id':
                        if 'city' in row and row['city'] not in [None, '']:
                            city_by_property_id[row[field]] = row['city']
                        files_by_property_id[row[field]].append(f)
                    elif field == 'development_code':
                        if 'city' in row and row['city'] not in [None, '']:
                            city_by_dev_code[row[field]] = row['city']
                        files_by_development_code[row[field]].append(f)
                        if 'city' in row and row['city'] not in [None, '']:
                            city_by_dev_code[row[field]] = row['city']
                    elif field == 'fha_loan_id':
                        if 'city' in row and row['city'] not in [None, '']:
                            city_by_fha_loan_id[row[field]] = row['city']
                    files_by[field][row[field]].append(f)

# Examine the breakdown of records containing property_id values based on which files each property_id appears in.
pa_files_by_property_id = defaultdict(list)
for property_id, file_list in files_by_property_id.items():
#    if file_list[0] == 'mf_8_contracts_us.csv' and len(set(file_list)) == 1:
#        pass
#    else:
        pa_files_by_property_id[property_id] = '|'.join(sorted(list(set(file_list))))
# Some files like mf_8_contracts and mf_inspections can have multiple records for a single property_id

prop_id_files_list = [{'property_id': p_id, 'file_list': v} for p_id, v in pa_files_by_property_id.items()]
for d in prop_id_files_list:
    d['city'] = city_by_property_id[d['property_id']]

write_to_csv('files_by_property_id.csv', prop_id_files_list, ['property_id', 'file_list', 'city'])

# property_id
# Out of the 794 property_id values in mf_inspections_pa that are not in any of the 3 mf*ac.csv files,
# only one is in Pittsburgh (Lemington Heights apartments) with a 2005 inspection, and it's no longer
# in existence. None of the others appear to be in Allegheny County (based on city name and my untrained eye).

# Thus, joining mf_*_ac and then adding in only those property_id values from mf_contracts and mf_inspections
# seems legitimate, yielding 250 property_id values (143 in Pittsburgh).


# [ ] Combine mf_subsidy_ac, mf_subsidy_loans_ac, and mf_loans_ac, join with mf_8_contracts_us

fields_to_get = ['hud_property_name',
        'property_street_address', 'municipality_name', 'city', 'zip_code',
        'contract_id', # mf_subsidy_ac
        'fha_loan_id',
        'state_id',
        ]

mf_ac_by_property_id = defaultdict(dict)
id_field = 'property_id'
ac_fha_loan_ids = []

ac_property_id_files = ['mf_subsidy_8_ac.csv', 'mf_subsidy_loans_ac.csv', 'mf_loans_ac.csv']
for a in ac_property_id_files:
    assert a in files

for f in files:
    if f in ac_property_id_files:# mf_inspections_pa.csv and mf_8_contracts_us COULD
    # BE added here if there are other fields we want to extract (that is, beyond constructing a master list).
        with open(f'{path}/{f}', 'r') as g:
            reader = csv.DictReader(g)
            for row in reader:
                add_row_to_linking_dict(f, row, id_field, fields_to_get, mf_ac_by_property_id)
                if f == 'mf_loans_ac.csv':
                    if 'fha_loan_id' in row and row['fha_loan_id'] not in [None, '']:
                        ac_fha_loan_ids.append(row['fha_loan_id'])


master_list = [v for k, v in mf_ac_by_property_id.items()]

#########################
# Examine the breakdown of records containing fha_loan_id values based on which files each fha_loan_id appears in.
id_field = 'fha_loan_id'
pa_files_by_fha_loan_id = defaultdict(list)
for fha_loan_id, file_list in files_by[id_field].items():
    pa_files_by_fha_loan_id[fha_loan_id] = '|'.join(sorted(list(set(file_list))))

fha_loan_id_files_list = [{'fha_loan_id': fha_loan_id, 'file_list': v} for fha_loan_id, v in pa_files_by_fha_loan_id.items()]
for d in fha_loan_id_files_list:
    d['city'] = city_by_fha_loan_id[d[id_field]]

write_to_csv('files_by_fha_loan_id.csv', fha_loan_id_files_list, [id_field, 'file_list', 'city'])

# The results look like this:
#file_list                                  count   percent
#mf_mortgages_pa.csv                        311     75.12
#mf_loans_ac.csv|mf_mortgages_pa.csv        85      20.53
#mf_loans_ac.csv                            10      2.42
#mf_init_commit_pa.csv|mf_mortgages_pa.csv  8       1.93

# I can't find any Allegheny County cities in the first grouping (mf_mortgages_pa records
# that are not in mf_loans_ac), so basically we can use mf_loans_ac to join to the other
# files. This is great because mf_loans_ac already has the property_id value.
# The only exception was one of the eight records in the intersection of mf_init_commit_pa
# (which itself only has 8 records): River Vue Apartments, which is listed in both files
# as having the city name "Pittsburg" and the ZIP code "51212".

# IMPORTANT NOTE: In 91% of records, fha_loan_id == associated_fha_laon_id

# Add fha_loan_id-based properties to master list.

ac_by_id = defaultdict(dict)
id_field = 'fha_loan_id'

fha_files = ['mf_init_commit_pa.csv', 'mf_mortgages_pa.csv']
for f in fha_files:
    assert f in files

for f in fha_files:
    with open(f'{path}/{f}', 'r') as g:
        reader = csv.DictReader(g)
        for row in reader:
            if row[id_field] == '03332013':
                add_row_to_linking_dict(f, row, id_field, fields_to_get, ac_by_id)

master_list += [v for k, v in ac_by_id.items()]
#########################
# Add LIHTC to master_list

# Some locations in Allegheny County have a county_fips_code of '42XXX'. Most of those
# have a fips2000[:5] == '42XXX'.
# The only properties which have county_fips_code == '42XXX' and fips2000[:5] == '42XXX',
# AND are in Allgeheny County have lihtc_property_id values of
# PAA19890800 ("NORTH BRADDOCK")
# PAA19900328 ("WILKINSBURG")
# PAA19910120 ("SWISSV[D]ALE")
# These particular records also lack geocoordinates, so they could not be pulled out of
# lihtc_projects_pa.csv based on latitude and longitude either.

f = 'lihtc_projects_pa.csv'
assert f in files

id_field = 'lihtc_project_id'
ac_by_id = defaultdict(dict)
with open(f'{path}/{f}', 'r') as g:
    reader = csv.DictReader(g)
    for row in reader:
        in_allegheny_county = row['county_fips_code'] == '42003' or row['fips2000'][:5] == '42003'
        # Whitelist known exceptions
        in_allegheny_county = in_allegheny_county or (row[id_field] in ['PAA19890800', 'PAA19900328', 'PAA19910120'])
        # Additional inclusions could be made based on latitude+longitude or city or zip_code
        if in_allegheny_county:
            add_row_to_linking_dict(f, row, id_field, fields_to_get, ac_by_id)

lihtc_projects = [v for k, v in ac_by_id.items()]
master_list += lihtc_projects
ic(len(lihtc_projects))
write_to_csv('lihtc_projects_ac.csv', lihtc_projects, fields_to_get + possible_keys + ['source_file'])
# LIHTC collisions:
# LIHTC has about 4 collisions, like the two records at the property_street_address == '110 MCINTYRE RD'

# What about the buildings? Do we need separate lists for buildings and projects, or do we fold them all into one list?

# It's not even clear that adding the buildings gives us much. In some cases, there are 6 identical records
# in the LIHTC buildings file, representing 6 different buildings all at the same address.
# The advantage might be that it makes it easier to link projects across files based on something like
# inspections, linking through the street address rather than project name.

# I'm skipping this for now.


f = 'lihtc_projects_pa.csv'
assert f in files

id_field = 'lihtc_project_id'
ac_by_id = defaultdict(dict)
with open(f'{path}/{f}', 'r') as g:
    reader = csv.DictReader(g)
    for row in reader:
        in_allegheny_county = row['county_fips_code'] == '42003' or row['fips2000'][:5] == '42003'
        # Whitelist known exceptions
        in_allegheny_county = in_allegheny_county or (row[id_field] in ['PAA19890800', 'PAA19900328', 'PAA19910120'])
        # Additional inclusions could be made based on latitude+longitude or city or zip_code
        if in_allegheny_county:
            ac_by_id[row[id_field]][id_field] = row[id_field]
            if 'source_file' not in ac_by_id[row[id_field]]:
                ac_by_id[row[id_field]]['source_file'] = f
            else:
                ac_by_id[row[id_field]]['source_file'] += '|' + f

            for field in fields_to_get:
                if field in row and row[field] not in [None, '']:
                    ac_by_id[row[id_field]][field] = row[field]

#########################
# Examine the breakdown of records containing development_code.
id_field = 'development_code'
pa_files_by_id = {}
for dev_code, flist in files_by_development_code.items():
    unique_files = list(set(flist))
    pa_files_by_id[dev_code] = '|'.join(sorted(unique_files))

list_of_dicts = [{id_field: k, 'file_list': v} for k, v in pa_files_by_id.items()]
for d in list_of_dicts:
    d['city'] = city_by_dev_code[d[id_field]]

write_to_csv('files_by_development_code.csv', list_of_dicts, [id_field, 'file_list', 'city'])
# Presently, all development codes are in both

ac_by_id = defaultdict(dict)

dev_code_files = ['housing_inspections_ac.csv', 'public_housing_projects_ac.csv'] #, 'public_housing_buildings_ac.csv']
for f in dev_code_files:
    assert f in files

for f in dev_code_files:
    with open(f'{path}/{f}', 'r') as g:
        reader = csv.DictReader(g)
        for row in reader:
            add_row_to_linking_dict(f, row, id_field, fields_to_get, ac_by_id)

master_list += [v for k, v in ac_by_id.items()]

#########################
# Examine the breakdown of records containing development_code values based on which files each development_code appears in.
#id_field = 'development_code'
#pa_files_by_id = defaultdict(list)
#for dev_code, file_list in files_by_development_code.items():
#    pa_files_by_id[dev_code] = '|'.join(sorted(list(set(file_list))))
#
#files_list = [{id_field: k, 'file_list': v} for k, v in pa_files_by_id.items()]
#for d in files_list:
#    d['city'] = city_by_dev_code[d[id_field]]
#
#write_to_csv('files_by_development_code.csv', files_list, [id_field, 'file_list', 'city'])
# Two records that are in the statewide

#########################
write_to_csv('master_list.csv', master_list, fields_to_get + possible_keys + ['source_file'])

ids_by = defaultdict(lambda: defaultdict(list))
#crosslink_ids = ['property_id']
#with open(f'{path}/bidirectional_crosslinks.csv', 'r') as g:
#    reader = csv.DictReader(g)
#    for row in reader:
#        for c_id in crosslink_ids:
#            if c_id in row and row[c_id] not in ['', None]:
#                ids_by[c_id][row[c_id]] = row

# Construct bidirectional lookup table for linking IDs
house_cat_id_name = 'house_cat_id'

all_ids_by = defaultdict(list)
crosslinks = {}
ids_to_add = {} # Used for adding 'house_cat_id' 
# values.

with open(f'punidirectional_links.csv', 'r') as g:
    reader = csv.DictReader(g)
    for row in reader:
        source_field = row['source_field']
        source_value = row['source_value']
        target_field = row['target_field']
        target_value = row['target_value']
        assert source_value != ''
        assert target_value != ''

        if row['relationship'] == 'needs': #target_field == house_cat_id_name:
            ids_to_add[f'{source_field}=={source_value}'] = target_value
        else:
            ids_by[source_field][source_value] = {target_field: target_value}
            ids_by[target_field][target_value] = {source_field: source_value}
            all_ids_by[source_field].append(source_value)
            all_ids_by[target_field].append(target_value)
            equations = sorted([f'{source_field}=={source_value}', f'{target_field}=={target_value}'])
            crosslinks['+'.join(equations)] = {'source_field': source_field, 'source_value': source_value,
                    'target_field': target_field, 'target_value': target_value}


deduplicated_master_list = []
duplicates_to_merge = defaultdict(list)

for record in master_list:
    for id_condition, value_to_add in ids_to_add.items():
        id_field, id_value = id_condition.split('==')
        if id_field in record and record[id_field] == id_value:
            record[house_cat_id_name] = value_to_add
    for key, crosslink in crosslinks.items():
        source_field = crosslink['source_field']
        source_value = crosslink['source_value']
        target_field = crosslink['target_field']
        target_value = crosslink['target_value']

        if (source_field in record and record[source_field] == source_value) or (target_field in record and record[target_field] == target_value):
            duplicates_to_merge[key].append(copy.deepcopy(record))
            print(record)
            break # Stop searching for crosslinks (however, this prevents multi-record merges)
    else:
        deduplicated_master_list.append(record)

ic(duplicates_to_merge)
assert False
# Bellefield Dwellings has one HUD Property ID 800018223, but two
# LIHTC Project ID values, one from 1988 and one from 2011.
# It also has two different state IDs and two different federal
# IDs. Thus, LIHTC Project IDs are just records of funding
# (as maybe most of these are in some way).

# Each of the LIHTC records gives some information (such as number of units).

# [ ] Add these to unidirectional_links.csv when the code below knows how to deal
# with one-to-many links:
#lihtc_project_id,PAA19880440,funded,property_id,800018223,Bellefield Dwellings,This is the first LIHTC project funding link.
#lihtc_project_id,PAA20133006,funded,property_id,800018223,Bellefield Dwellings,This is the second LIHTC project funding link.

# [ ] development_code,PA006000811,is another ID for,800237651,LAVENDER HEIGHTS I/Lavender Heights

# [ ] PA006000822 (Carnegie Apartments Additions is a second LIHTC project that should be linked to the original 13-unit Carnegie Apartments)

# [ ] PAA19890620 and PAA19910435 are two different LIHTC Project IDs for the same location (HIGHLAND AVE APARTMENTS).
# The funding was for 3 units and then 1 unit, respectively. But it's not clear if the 1 is part of the 3.



#lihtc_project_id,PAA19890620,funded,house_cat_id,15145-highland-ave-apartments,Highland Ave Apartments
#lihtc_project_id,PAA19910435,funded,house_cat_id,15145-highland-ave-apartments,Highland Ave Apartments

# [ ] We need our own ID to associate with this, I guess, and we need it to show up in the master lists.

# 1) Add house_cat_id to a record with a given ID.
# 2) Merge multiple records linked with 

for dup_key, dups in duplicates_to_merge.items():
    merged_record = {}
    if len(dups) != 2:
        ic(dup_key)
        ic(dups)
        assert False
    for key, value in dups[0].items():
        other_value = dups[1].get(key, None)
        if other_value is None:
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
            ic(dup_key)
            ic(dups)
            print(f"Since this code doesn't know how to merge key = {key}, value = {value}, other value = {dups[1][key]}, it's just going to list both.")
            merged_record[key] = f'{value}|{other_value}'
            #raise ValueError(f'What should we do with key = {key}, value = {value}, other value = {dups[1][key]}?')

    for key, value in dups[1].items():
        if key not in merged_record or merged_record[key] == '':
            merged_record[key] = value

    #merged_record[house_cat_id_name] =
    deduplicated_master_list.append(merged_record)


write_to_csv('deduplicated_master_list.csv', deduplicated_master_list, fields_to_get + possible_keys + [house_cat_id_name, 'source_file'])
#ic(all_ids_by)
#deduplicated_master_list = []
#duplicates_to_merge = defaultdict(list)
#for record in master_list:
#    for linking_field, linking_ids in all_ids_by.items():
#        if linking_field in record and record[linking_field] in linking_ids:
#            # Deduplicate this one by saving it and then trying to collect the other one and matching them up at the end.
#            other_lookup = ids_by[linking_field][record[linking_field]]
#            other_field = list(other_lookup.keys())[0]
#            other_value = list(other_lookup.values())[0]
#            assert len(other_lookup.keys()) == 1
#            equations = sorted([f'{linking_field}=={record[linking_field]}', f'{other_field}=={other_value}'])
#            ic(equations)
#            duplicates_to_merge['+'.join(equations)].append(record)
#        else:
#            deduplicated_master_list.append(record)
#            continue
ic(crosslinks)
ic(duplicates_to_merge)
ic(len(master_list))
ic(len(deduplicated_master_list))
#########################
pprint(keys_by_file)
pprint(files_by_key)
#pprint(pa_files_by_property_id)
ic(len(pa_files_by_property_id))
ic(len(files_by['property_id']))

#pprint(files_by_development_code)
ic(len(files_by_development_code))

# Buildings and projects should be treated distinctly. Anything else?

# We need to filter some files down to Allegheny County.

# Two records with fha_loan_id == '03335292' were found, one from mf_subsidy_loans_ac.csv|mf_loans.csv and one from mf_mortgages_pa.csv (based on the linking attempt that just added separate records rather than linking).
# The first two files both had a correct street address but the wrong ZIP code, while the second had the right ZIP code but a seemingly very wrong street address.
#hud_property_name      property_street_address         city        zip_code    fha_loan_id property_id source_file
#Oak Hill Brackenridge  537 Oak Hill Drive              Pittsburgh  15213       03335292    800244735   mf_subsidy_loans_ac.csv|mf_loans_ac.csv
#Oak Hill Brackenridge  120  209 & 239 Blakey Court  1  Pittsburgh  15219       03335292                mf_mortgages_pa.csv
# This will present a linking problem.
# One option: Make lists of all the conflicting address information.
# Another option: Use the (seemingly accurate) geocoordinates in mf_loans.

