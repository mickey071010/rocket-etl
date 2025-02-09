import re, csv, copy
from pprint import pprint
from icecream import ic
from collections import defaultdict
try:
    from _deduplicate import standardize_field, index_filename, possible_keys # These calls can't be made
    from _util import multikeysort # from tango_with_django.
except ModuleNotFoundError:
    from engine.payload.house_cat._deduplicate import standardize_field, index_filename, possible_keys
    from engine.payload.house_cat._util import multikeysort

#from parameters.local_parameters import DESTINATION_DIR
DESTINATION_DIR = "/Users/drw/WPRDC/etl/rocket-etl/output_files/"

path = DESTINATION_DIR + "house_cat"

def write_to_csv(filename, list_of_dicts, keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def check_uniqueness_of_ids(f, id_field):
    with open(f'{path}/{f}', 'r') as g:
        reader = csv.DictReader(g)
        id_values = []
        for row in reader:
            id_values.append(row[id_field])
        if len(id_values) != len(set(id_values)):
            raise ValueError(f"There are some duplicate {id_field} values in {f}!")

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
        ac_by_id[row[id_field]]['source_file'] = '|'.join(sorted(list(set(xs + [f]))))
        # The above line includes "sorted" to ensure that the source_files are always
        # listed in alphabetical order, for consistency across multiple generations
        # of this file (allowing easy comparisons).
    for field in fields_to_get:
        if field in row and row[field] not in [None, '']:
            ac_by_id[row[id_field]][field] = row[field]

def add_to_master_list(id_field, sources, available_files, fields_to_get, master_list, check_uniqueness=False):
    ac_by_id = defaultdict(dict)

    for f in sources:
        assert f in available_files
        if check_uniqueness:
            check_uniqueness_of_ids(f, id_field)

    for f in sources:
        with open(f'{path}/{f}', 'r') as g:
            reader = csv.DictReader(g)
            for row in reader:
                add_row_to_linking_dict(f, row, id_field, fields_to_get, ac_by_id)

    master_list += [v for k, v in ac_by_id.items()]

def link_records_into_index():
    generate_intermediate_files = False
    all_files = get_files_in_folder(path)
    excluded_files = ['deduplicated_index.csv', 'example-affordable-housing-projects--fields-merged.csv', 'example-affordable-housing-projects--fields-sorted.csv', 'phfa_pgh_demographics--not-in-our-master-list-2021-05.csv', 'temp-backup-of-example-affordable-housing-projects--fields-merged.csv']
    files = [f for f in all_files if f[-3:].lower() == 'csv' and f not in excluded_files and f[:2] != 'eg']
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

    if generate_intermediate_files:
        # Examine the breakdown of records containing property_id values based on which files each property_id appears in.
        pa_files_by_property_id = defaultdict(list)
        for property_id, file_list in files_by_property_id.items():
        #    if file_list[0] == 'mf_8_contracts_pa.csv' and len(set(file_list)) == 1:
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


    # [ ] Combine mf_subsidy_ac, mf_subsidy_loans_ac, and mf_loans_ac, join with mf_8_contracts_pa

    fields_to_get = ['hud_property_name',
            'property_street_address', 'municipality_name', 'city', 'zip_code', 'units',
            'latitude', 'longitude',
            'census_tract',
            'scattered_sites',
            'contract_id', # mf_subsidy_ac
            'fha_loan_id',
            'normalized_state_id',
            'pmindx',
#            'property_id',
            ]
    master_list = []

    mf_ac_by_property_id = defaultdict(dict)
    id_field = 'property_id'

    sources = ac_property_id_files = ['mf_subsidy_8_ac.csv', 'mf_subsidy_loans_ac.csv', 'mf_loans_ac.csv'] # mf_inspections_pa.csv
    # and mf_8_contracts_pa COULD BE added here if there are other fields we want to extract (that
    # is, beyond constructing a master list). # These are Allegheny County files where the
    # property_id identifies the projects.

    add_to_master_list(id_field, sources, files, fields_to_get, master_list, check_uniqueness=False)
    #########################
    # Examine the breakdown of records containing fha_loan_id values based on which files each fha_loan_id appears in.
    if generate_intermediate_files:
        id_field = 'fha_loan_id'
        pa_files_by_fha_loan_id = defaultdict(list)
        for fha_loan_id, file_list in files_by[id_field].items():
            pa_files_by_fha_loan_id[fha_loan_id] = '|'.join(sorted(list(set(file_list))))

        fha_loan_id_files_list = [{'fha_loan_id': fha_loan_id, 'file_list': v} for fha_loan_id, v in pa_files_by_fha_loan_id.items()]
        for d in fha_loan_id_files_list:
            d['city'] = city_by_fha_loan_id[d[id_field]]

        write_to_csv('files_by_fha_loan_id.csv', fha_loan_id_files_list, [id_field, 'file_list', 'city'])

    # The OLD results look like this:
    #file_list                                  count   percent
    #mf_mortgages_pa.csv                        311     75.12
    #mf_loans_ac.csv|mf_mortgages_pa.csv        85      20.53
    #mf_loans_ac.csv                            10      2.42
    #mf_init_commit_pa.csv|mf_mortgages_pa.csv  8       1.93

    # I can't find any Allegheny County cities in the first grouping (mf_mortgages_pa records
    # that are not in mf_loans_ac), so basically we can use mf_loans_ac to join to the other
    # files. This is great because mf_loans_ac already has the property_id value.
    # The only exception was one of the eight records in the intersection of mf_init_commit_pa
    # and mf_mortgages_pa.csv (which itself only has 8 records): River Vue Apartments,
    # which is listed in both files as having the city name "Pittsburg" and the ZIP code "51212".
    # (This record initally caused problems downstream as the deduplication code in _plink.py assumed
    # that no key value occurred in more than one record, and there are instances of River Vue
    # Apartments with its fha_loan_id in both mf_loans_ac.csv|mf_subsidy_loans_ac.csv and
    # mf_mortgages_pa.csv|mf_init_commit_pa.csv. It's actually easier to just not add this one at
    # all (since it adds a duplicate record with incorrectly typed city name and ZIP code
    # and adds no obvious value). The only thing worth noting is that it CAN be looked up in
    # those other two files.)

    # Add fha_loan_id-based properties to master list.

    ###ac_by_id = defaultdict(dict)
    ###id_field = 'fha_loan_id'
    ###
    ###fha_files = ['mf_init_commit_pa.csv', 'mf_mortgages_pa.csv']
    ###for f in fha_files:
    ###    assert f in files
    ###
    ###for f in fha_files:
    ###    with open(f'{path}/{f}', 'r') as g:
    ###        reader = csv.DictReader(g)
    ###        for row in reader:
    ###            if row[id_field] == '03332013': # River Vue Apartments
    ###                add_row_to_linking_dict(f, row, id_field, fields_to_get, ac_by_id)
    ###
    ###master_list += [v for k, v in ac_by_id.items()]
    ## Instead of adding this, let's just tack on the filenames to the associated record.

    #for r in master_list:
    #    if r.get('fha_loan_id', None) == '03332013': # River Vue Apartments
    #        r['source_file'] += '|mf_mortgages_pa.csv|mf_init_commit_pa.csv'

    ## IMPORTANT NOTE: In 91% of records, fha_loan_id == associated_fha_laon_id
    #### DIVISION BETWEEN OLD (one quarter) AND NEW (20 years of Initial Commits (or whatever))
    #file_list	                                                                            count
    #mf_init_commit_pa.csv|mf_mortgages_pa.csv	                                            304
    #mf_init_commit_pa.csv	                                                            228
    #mf_init_commit_pa.csv|mf_loans_ac.csv|mf_mortgages_pa.csv	                            63
    #mf_init_commit_pa.csv|mf_loans_ac.csv|mf_mortgages_pa.csv|phfa_pgh_demographics.csv    22
    #phfa_pgh_demographics.csv	                                                            14
    #mf_loans_ac.csv	                                                                    9
    #mf_mortgages_pa.csv	                                                            8
    #mf_init_commit_pa.csv|phfa_pgh_demographics.csv	                                    4
    #crowdsourced_records.csv|phfa_pgh_demographics.csv                                     1

    ### Now there's a ton of mf_init_commit-only records that need to be pulled in. I guess
    ### we're pulling in mf_mortgages_pa also (even though they were left out before).
    ### The previous analysis was done before phfa_pgh_demographics.csv was added or
    ### crowdsourced_records.csv. It should be noted that crowdsourced_records.csv has the
    ### field fha_loan_id but no actual value for the single record it currently contains,
    ### so this histogram method no longer jibes with all the file formats.

    # Add fha_loan_id-based properties to master list.

    ac_by_id = defaultdict(dict)
    id_field = 'fha_loan_id'

    fha_files = ['mf_init_commit_pa.csv', 'mf_mortgages_pa.csv']
    for f in fha_files:
        assert f in files

    ac_cities = ['Pittsburgh', 'McKeesport', 'Monroeville', 'Bethel Park']
    ac_cities += ['Turtle Creek', 'Pittsburg', 'Wilkinsburg', 'Coraopolis']
    ac_cities += ['Crafton', 'Jefferson Hills', 'Forest Hills', 'Oakdale']
    ac_cities += ['Verona', 'McKees Rocks', 'Harmar Township', 'Bridgeville']
    ac_cities += ['South Park', 'Brackenridge', 'Oakmont', 'Cheswick']
    ac_cities += ['Moon', 'Etna', 'Upper St. Clair', 'Sharpsburg']
    ac_cities += ['Borough of Avalon', 'Pitcairn', 'Wilmerding', 'Leetsdale']
    ac_cities += ['Plum', 'McCandless', 'Munhall', 'West Mifflin']
    ac_cities += ['East Pittsburgh', 'Port Vue', 'Braddock', 'Duquesne']
    ac_cities += ['Moon Township']
    # Whitehall is deliberately left off this list since there is another
    # Whitehall, PA where the actual FHA properties all are.
    AC_CITIES = [c.upper() for c in ac_cities]
    for f in fha_files:
        with open(f'{path}/{f}', 'r') as g:
            reader = csv.DictReader(g)
            for row in reader:
                if row['city'] == 'Pittsburg':
                    row['city'] = 'Pittsburgh'
                elif row['city'] == 'Moon Township':
                    row['city'] = 'Moon'
                in_allegheny_county = row['city'] in ac_cities
                in_allegheny_county = in_allegheny_county or row['city'].upper() in AC_CITIES
                #if row[id_field] == '03332013': # River Vue Apartments
                if in_allegheny_county:
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

    # It looks like after the 2021 update, our 'fips2000'-equivalent field is
    # now called 'census_tract_2000'.
    f = 'lihtc_projects_pa.csv'
    assert f in files

    id_field = 'lihtc_project_id'
    ac_by_id = defaultdict(dict)
    with open(f'{path}/{f}', 'r') as g:
        reader = csv.DictReader(g)
        for row in reader:
            in_allegheny_county = row['county_fips_code'] == '42003' or row['census_tract_2000'][:5] == '42003'
            # Whitelist known exceptions
            in_allegheny_county = in_allegheny_county or (row[id_field] in ['PAA19890800', 'PAA19900328', 'PAA19910120'])
            # Additional inclusions could be made based on latitude+longitude or city or zip_code
            if in_allegheny_county:
                add_row_to_linking_dict(f, row, id_field, fields_to_get, ac_by_id)

    lihtc_projects = [v for k, v in ac_by_id.items()]
    master_list += lihtc_projects


    if generate_intermediate_files:
        write_to_csv('lihtc_projects_ac.csv', lihtc_projects, fields_to_get + possible_keys + ['source_file'])
    # LIHTC collisions:
    # LIHTC has about 4 collisions, like the two records at the property_street_address == '110 MCINTYRE RD'

    # What about the buildings? Do we need separate lists for buildings and projects, or do we fold them all into one list?

    # It's not even clear that adding the buildings gives us much. In some cases, there are 6 identical records
    # in the LIHTC buildings file, representing 6 different buildings all at the same address.
    # The advantage might be that it makes it easier to link projects across files based on something like
    # inspections, linking through the street address rather than project name.

    # I'm skipping this for now.

    #########################
    # Examine the breakdown of records containing development_code.
    id_field = 'development_code'

    if generate_intermediate_files:
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

    sources = ['housing_inspections_ac.csv', 'public_housing_projects_ac.csv'] #, 'public_housing_buildings_ac.csv'] # dev_code_files
    add_to_master_list(id_field, sources, files, fields_to_get, master_list, check_uniqueness=False)

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

    ########################
    # Add PHFA Demographics file.
    id_field = 'pmindx'
    sources = ['phfa_pgh_demographics.csv']
    add_to_master_list(id_field, sources, files, fields_to_get, master_list, check_uniqueness=False)

    ########################
    # Add crowdsourced_records file.
    id_field = 'crowdsourced_id'
    sources = ['crowdsourced_records.csv']
    # Add all new id_field values to possible_keys in _deduplicate.py.
    # Those get added to fields_to_write below.
    ### Template for adding new data sources ###
    add_to_master_list(id_field, sources, files, fields_to_get, master_list, check_uniqueness=True)
    #########################
    fields_to_write = fields_to_get
    for f in possible_keys:
        if f not in fields_to_write:
            fields_to_write.append(f)
    #########################
    for row in master_list:
        for fieldname, value in row.items():
            if type(value) == str:
                if fieldname not in ['source_file']:
                    row[fieldname] = standardize_field(value, fieldname)

    #########################
    # Sort master_list to make it easier to compare files.
    sorted_master_list = multikeysort(master_list, ['crowdsourced_id', 'pmindx', 'development_code', 'normalized_state_id', 'property_id'])
    ic(len(sorted_master_list))
    ########################

    ########################
    write_to_csv(index_filename, sorted_master_list, fields_to_write + ['source_file'])

if __name__ == '__main__':
    link_records_into_index() # Generate the index of records, making
    # obvious links but leaving trickier ones for _deduplicate.py

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

# [ ] We need our own ID to associate with this, I guess, and we need it to show up in the master lists.

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

