import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.ckan_util import find_resource_id, get_resource_data
from engine.etl_util import write_to_csv
from engine.notify import send_to_slack
from engine.parameters.local_parameters import SOURCE_DIR, PRODUCTION
from engine.post_processors import check_for_empty_table

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

key_project_identifiers = ['property_id', 'normalized_state_id', 'development_code', 'pmindx'] # This is
# the minimal set of project identifiers needed to uniquely identify a project. This may expand as 
# we add other data sources.

def string_or_blank(x):
    if x in [None, '']:
        return '_'
    return x

def form_keychain(data):
    zs = [string_or_blank(data[p_id]) for p_id in key_project_identifiers]
    return ':'.join(zs)

id_value = 0
ID_FIELD_NAME = 'id'
source_filename_by_project_id = {
        'contract_id': 'house_cat_projectindex_contract_id.csv',
        'development_code': 'house_cat_projectindex_development_code.csv',
        'fha_loan_id': 'house_cat_projectindex_fha_loan_id.csv',
        'lihtc_project_id': 'house_cat_projectindex_lihtc_project_id.csv',
        'normalized_state_id': 'house_cat_projectindex_normalized_state_id.csv',
        'pmindx': 'house_cat_projectindex_pmindx'
        }

class LookupSchema(pl.BaseSchema):
    projectindex_id = fields.Integer(load_from='projectindex_id', dump_to='projectindex_id')
    projectidentifier_id = fields.String(load_from='projectidentifier_id', dump_to='projectidentifier_id')

class PropertyIndexSchema(pl.BaseSchema):
    job_code = 'update_index'
    the_id = fields.Integer(dump_only=True, dump_to=ID_FIELD_NAME)
    property_id = fields.String(load_from='property_id'.lower(), dump_to='property_id', allow_none=True)
    hud_property_name = fields.String(load_from='hud_property_name'.lower(), dump_to='hud_property_name')
    property_street_address = fields.String(load_from='property_street_address'.lower(), dump_to='property_street_address', allow_none=True)
    municipality_name = fields.String(load_from='municipality_name'.lower(), dump_to='municipality_name', allow_none=True)
    city = fields.String(load_from='city'.lower(), dump_to='city')
    zip_code = fields.String(load_from='zip_code'.lower(), dump_to='zip_code', allow_none=True)
    units = fields.String(load_from='units'.lower(), dump_to='units', allow_none=True)
    #source_file = fields.String(load_from='source_file'.lower(), dump_to='source_file')
    #index = fields.Integer(load_from='index'.lower(), dump_to='index')
    latitude = fields.String(load_from='latitude'.lower(), dump_to='latitude', allow_none=True)
    longitude = fields.String(load_from='longitude'.lower(), dump_to='longitude', allow_none=True)

    house_cat_id = fields.String(dump_only=True, dump_to='house_cat_id', allow_none=False)
    # These are the project identifiers that get shuffled off into little tiny side tables.
    contract_id = fields.String(load_from='contract_id'.lower(), load_only=True, dump_to='contract_id', allow_none=True)
    fha_loan_id = fields.String(load_from='fha_loan_id'.lower(), load_only=True, dump_to='fha_loan_id', allow_none=True)
    normalized_state_id = fields.String(load_from='normalized_state_id'.lower(), load_only=True, dump_to='normalized_state_id', allow_none=True)
    pmindx = fields.String(load_from='pmindx'.lower(), load_only=True, dump_to='pmindx', allow_none=True)
    lihtc_project_id = fields.String(load_from='lihtc_project_id'.lower(), load_only=True, dump_to='lihtc_project_id', allow_none=True)
    development_code = fields.String(load_from='development_code'.lower(), load_only=True, dump_to='development_code', allow_none=True)

    #house_cat_id = fields.String(load_from='house_cat_id'.lower(), dump_to='house_cat_id', allow_none=True)

    class Meta:
        ordered = True

    @post_load
    def set_id(self, data):
        global id_value
        data['the_id'] = id_value
        id_value += 1

    @post_load
    def synthesize_house_cat_id(self, data):
        data['house_cat_id'] = form_keychain(data)

# dfg
housecat_tango_with_django_package_id = '3f6411c8-d03d-45b2-8225-673841e5c2b3'
from engine.payload.house_cat._deduplicate import deduplicated_index_filename

def generate_deduplicated_index(job, **kwparameters):
    # Run _flatbread.py in local output mode (python launchpad.py house_cat/_flatbread.py to_file)
    # Then use those files to run _super_link.py. (This process takes a while (~20-30 minutes?).)

    # Or else download the tables from CKAN.
    from engine.payload.house_cat._super_link import link_records_into_index
    from engine.payload.house_cat._deduplicate import deduplicate_records
    link_records_into_index()
    deduplicate_records()
    # Now save the deduplicated index to the expected source_file location? 

def hunt_and_peck_update_index(job, **kwparameters):
    # 1) Get existing index from CKAN
    from engine.credentials import site, API_key
    resource_id = find_resource_id(job.production_package_id, job.resource_name)
    max_records = 50000
    records = get_resource_data(site, resource_id, API_key, count=max_records)
    if len(records) == 0:
        raise ValueError("No data obtained!")
    if len(records) == max_records:
        raise ValueError("Maybe failed to obtain every single record!")

    records_by_property_id = {}
    records_by_keychain = {}
    ids = []
    for record in records:
        if record['property_id'] not in [None, '']:
            records_by_property_id[record['property_id']] = dict(record)
        records_by_keychain[record['house_cat_id']] = dict(record)
        ids.append(record[ID_FIELD_NAME])

   # 2) Get incoming index
    assert job.source_type == 'local'

    lookups_by_project_identifier = defaultdict(list)

    projectidentifier_ids = set()
    
    property_id_records = []
    keychain_records = []
    with open(job.target, 'r') as f:
        reader = csv.DictReader(f)
        # 3) Link property identifiers to row ID in house_cat_projectindex table...
        #   For each record in deduplicated_index.csv AND each project identifier column,
        #       a) Look up the _id value in the CKAN Project Index table.
        rows_with_id = []
        for row in reader:
            if 'property_id' in row and row['property_id'] not in ['', None]:
                # Look up the _id value based on the project_id value.
                that_id = records_by_property_id[row['property_id']][ID_FIELD_NAME]
            else: # This is going to be easier if we can just inspect a concatenated 
                # list of property IDs in a new index table column (rather than 
                # having to link through to other tables).
                keychain = form_keychain(row)
                if keychain in records_by_keychain:
                    that_id = records_by_keychain[keychain][ID_FIELD_NAME]
                else:
                    # This row is not yet in the table, so it can be upserted with a new id value.
                    that_id = max(ids) + 1
                    ids.append(that_id)

            # Synthesize the record to push, using this primary key value.
            row[ID_FIELD_NAME] = that_id
            rows_with_id.append(row)

            #       b) Add id, <project_identifier_value> to a crosswalk table.
            #   Then upsert those crosswalk records to house_cat_projectindex_<project_identifier>.
            for p_id in project_identifiers:
                if row[p_id] != '':
                    id_values = row[p_id].split('|')
                    for id_value in id_values:
                        lookups_by_project_identifier[p_id].append({'projectindex_id': that_id, 'projectidentifier_id': id_value})
                        # These fields need to be made into those tiny lookup tables AND added to house_cat_projectidentifier:
                        projectidentifier_ids.add(id_value)

        # Write property index updates
        job.target += '.csv' # Sneaky way to change the job.target value without bothering to decompose the original file name.
        write_to_csv(job.target, rows_with_id)

        l_of_ds = [{'projectidentifier_id': pi_id} for pi_id in projectidentifier_ids]
        # Write project identifiers update.
        write_to_csv(f'{job.local_directory}house_cat_projectidentifier.csv', l_of_ds)
        # Write lookup tables.
        for p_id in project_identifiers:
            write_to_csv(f'{job.local_directory}house_cat_projectindex_{p_id}.csv', lookups_by_project_identifier[p_id])

def make_little_lookup_tables(job, **kwparameters):
    #it_takes_two_to_tango() # Generate all these little lookup tables.
    pass

# Jobs
# 1) Update the property index (might require one pass per property identifier).
# 2) Upsert all project identifiers to house_cat_projectidentifier.
# 3) Link property identifiers to row ID in house_cat_projectindex table...
#       For each record in deduplicated_index.csv AND each project identifier column,
#           a) Look up the _id value in the CKAN Project Index table.
#           b) Add _id, <project_identifier_value> to a crosswalk table.
#       Then upsert those crosswalk records to house_cat_projectindex_<project_identifier>.

job_dicts = [
    {
        'job_code': 'wipe_and_replace_index', #PropertyIndexSchema().job_code, # 'index'
        'source_type': 'local',
        'source_file': deduplicated_index_filename,
        'updates': 'Monthly',
        'schema': PropertyIndexSchema,
        #'filters': [['property_id', '!=', None]], # Might it be necessary to iterate through all the property fields
        # like this, upserting on each one?
        'always_wipe_data': False, # We'd like to keep the records to try to preserve the _id values
        # to maintain links through the little lookup tables.

        'primary_key_fields': [ID_FIELD_NAME],
        'destination': 'ckan',
        'package': housecat_tango_with_django_package_id,
        'resource_name': 'house_cat_projectindex',
        'upload_method': 'upsert',
        # This all seemed great until I realized that we can't upsert PropertyIndex 
        # records because there is no consistent key. In the absence of such a key, we have to search the 
        # existing table to find matching records and update them; when no matches are found, we must 
        # insert the records. The only alternative is to keep wiping the table and then regenerating all
        # the little lookup tables with the new _id values.

        # Try the approach with hunt-and-peck-style upserts (which would find the record
        # to overwrite and then update maybe based on the _id value).

        # If it's too complex, give up on maintaining consistent id values (or enforce them through
        # unidirectional_links.csv) and just wipe the tables and regenerate them every time.

        #'resource_description': f'Derived from https://www.hud.gov/program_offices/housing/mfh/mfdata/mfproduction', #\n\njob code: {MultifamilyProductionInitialCommitmentSchema().job_code},'
        #'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
    {
        'job_code': PropertyIndexSchema().job_code, # 'update_index'
        #From the house_cat data architecture plan: CROWDSOURCED HOUSING PROJECTS
        #User-contributed records can be dumped in a separate table with basically the same
        #schema as the Property Information table, and the id field from that table can serve
        #as a fifth project identifier. Once we approve one of those records (maybe by
        #switching an "approved" boolean to True), it can show up in search results
        #(either by the query querying the crowdsourced table or by the approved records
        #being copied to the Property Information table).
        # This means that we can't wipe the index, and maybe we can't wipe any of the other
        # tables because they might wind up getting populated by those crowdsourced
        # results. But for any table we can't wipe, we need to think about what happens
        # if one of its rows gets orphaned. Suppose we realize that a particular LIHTC
        # project ID was a typo. We correct the typo, but because we're upserting new
        # values and not wiping old ones, the erroneous ID can still be in there. Is
        # this a problem? It still gets associated with the property, so if the original
        # housing project only had one LIHTC project ID, the new version will have two:
        # the original erroneous one and the corrected one.
        # So consider wiping all tables and then regenerating all the lookup support tables.
        # This means that whenever this ETL job is run, another one, which adds the 
        # crowdsourced records must also be run. This suggestes that the simplest 
        # solution is to integrate the crowdsourced table into these jobs. Specifically,
        # the crowdsourced data should get integrated into the synthesis of the index,
        # even before the deduplication.
        'source_type': 'local',
        'source_file': deduplicated_index_filename,
        'updates': 'Monthly',
        'schema': PropertyIndexSchema,
        'custom_processing': hunt_and_peck_update_index, # Weird new ETL job:
        # Everything happens in the custom_processing part because this 
        # one is so weird. Make 'destination' local so nothing else
        # happens to the data portal.

        #'filters': [['property_id', '!=', None]], # Might it be necessary to iterate through all the property fields
        # like this, upserting on each one?
        'always_wipe_data': False, # We'd like to keep the records to try to preserve the _id values
        # to maintain links through the little lookup tables.

        #'primary_key_fields': ['property_id'], # This doesn't work because of the many projects that
        # have no property_id.
        'destination': 'local',
        'package': housecat_tango_with_django_package_id,
        'resource_name': 'house_cat_projectindex',
        'upload_method': 'insert', # This all seemed great until I realized that we can't upsert PropertyIndex 
        # records because there is no consistent key. In the absence of such a key, we have to search the 
        # existing table to find matching records and update them; when no matches are found, we must 
        # insert the records. The only alternative is to keep wiping the table and then regenerating all
        # the little lookup tables with the new _id values.

        # Try the approach with hunt-and-peck-style upserts (which would find the record
        # to overwrite and then update maybe based on the _id value).

        # If it's too complex, give up on maintaining consistent id values (or enforce them through
        # unidirectional_links.csv) and just wipe the tables and regenerate them every time.



        #'resource_description': f'Derived from https://www.hud.gov/program_offices/housing/mfh/mfdata/mfproduction', #\n\njob code: {MultifamilyProductionInitialCommitmentSchema().job_code},'
        #'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
]

job_dict_template = \
    {
        'source_type': 'local',
        'schema': LookupSchema,
        #'filters': [['property_id', '!=', None]], # Might it be necessary to iterate through all the property fields
        # like this, upserting on each one?
        'always_wipe_data': True, # The current strategy is to try to regenerate 
        # everything but the id values whenever there's an update.
        #'primary_key_fields': ['projectindex_id', 'projectidentifier_id'], # Wiping the data
        # lets us insert the records and not use primary keys.
        'destination': 'ckan',
        'package': housecat_tango_with_django_package_id,
        'upload_method': 'insert',
    }

from engine.payload.house_cat._deduplicate import possible_keys as project_identifiers
project_identifiers.remove('property_id')
for p_id in project_identifiers:
    job_dict = dict(job_dict_template)
    job_dict['job_code'] = p_id
    job_dict['source_file'] = source_filename_by_project_id[p_id]
    job_dict['resource_name'] = job_dict['source_file'].split('.')[0]
    job_dicts.append(job_dict)

# [ ] Add total list of project identifiers.

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
