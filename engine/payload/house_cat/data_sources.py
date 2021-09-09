import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from launchpad import get_job_dicts
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import local_file_and_dir
from engine.ckan_util import find_resource_id, get_resource_fields
from engine.notify import send_to_slack
from engine.parameters.local_parameters import SOURCE_DIR, PRODUCTION
from engine.post_processors import check_for_empty_table

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

group_by_job_code = {
        'mf_mortgages': 'Multifamily Insured Financing', # (HUD)
        'mf_init_commit': 'Multifamily Insured Financing', # (HUD)
        'lihtc': 'LIHTC', # This comes from HUD but it may also come from PHFA
        'lihtc_building': 'LIHTC',
        #'lihtc_building2': 'LIHTC', # This mapping may not be needed since
        # lihtc_building* map to the same CKAN table.
        'housing_inspections': 'Inspections', #(REAC for Public Housing and Multifamily)
        'hud_public_housing_projects': 'Public Housing', # (HUD and down the road city/county authorities)
        'hud_public_housing_buildings': 'Public Housing',
        'mf_subsidy_loans': 'Multifamily Direct Subsidy',
        'mf_subsidy_8': 'Multifamily Direct Subsidy',
        'mf_contracts_8': 'Multifamily Direct Subsidy',
        'mf_loans': 'Multifamily Insured Financing',
        'mf_inspections_1': 'Inspections',

        'hfa_lihtc': 'LIHTC',

        'hunt_and_peck': 'Property Information',
        }


class HouseCatSourcesSchema(pl.BaseSchema):
    job_code = 'data_sources'
    source_name = fields.String(load_from='resource_name'.lower(), dump_to='source_name', allow_none=True)
    source_full_url = fields.String(load_from='source_full_url'.lower(), dump_to='source_full_url', allow_none=True)
    source_landing_page = fields.String(load_from='resource_description'.lower(), dump_to='source_landing_page', allow_none=True)
    other_job_code = fields.String(load_from='job_code'.lower(), dump_to='job_code')
    job_directory = fields.String(load_from='job_directory'.lower(), dump_to='job_directory')
    resource_name = fields.String(load_from='resource_name'.lower(), dump_to='resource_name', allow_none=True)
    resource_id = fields.String(load_from='resource_id', dump_to='resource_id', allow_none=True)
    package_id = fields.String(load_from='package'.lower(), dump_to='package_id', allow_none=True)
    data_group = fields.String(dump_only=True, dump_to='data_group')

    class Meta:
        ordered = True

    # Split the parenthetical part off of the resource_name
    # to make source_name.

    @pre_load
    def fix_description(self, data):
        f = 'resource_description'
        f2 = 'source_landing_page'
        data[f2] = None
        if data[f] is not None:
            if re.match('Derived from ', data[f]):
                data[f2] = re.sub('Derived from ', '', data[f])

    @post_load
    def set_group(self, data):
        f = 'other_job_code'
        f2 = 'data_group'
        if data[f] in group_by_job_code:
            data[f2] = group_by_job_code[data[f]]
        else:
            data[f2] = 'Uncategorized'

    @post_load
    def fix_source_name(self, data):
        f = 'resource_name'
        f2 = 'source_name'
        if data[f] is not None:
            data[f2] = re.sub(' \(.*', '', data[f])

    @pre_load
    def add_resource_id(self, data):
        i1 = 'package'
        i2 = 'resource_name'
        if data[i1] is not None and data[i2] is not None:
            r_id = find_resource_id(data[i1], data[i2])
            data['resource_id'] = r_id

class HouseCatFieldsSchema(pl.BaseSchema):
    job_code = 'fields'
    source_name = fields.String(load_from='resource_name'.lower(), dump_to='source_name', allow_none=True)
    source_full_url = fields.String(load_from='source_full_url'.lower(), dump_to='source_full_url', allow_none=True)
    source_landing_page = fields.String(load_from='resource_description'.lower(), dump_to='source_landing_page', allow_none=True)
    other_job_code = fields.String(load_from='job_code'.lower(), dump_to='job_code')
    job_directory = fields.String(load_from='job_directory'.lower(), dump_to='job_directory')
    resource_name = fields.String(load_from='resource_name'.lower(), dump_to='resource_name', allow_none=True)
    resource_id = fields.String(load_from='resource_id', dump_to='resource_id', allow_none=True)
    package_id = fields.String(load_from='package'.lower(), dump_to='package_id', allow_none=True)
    data_group = fields.String(load_from='data_group', dump_to='data_group')
    field = fields.String(load_from='field', dump_to='field')

    class Meta:
        ordered = True

def scrape_rocket_jobs(job, **kwparameters):
    #if not kwparameters['use_local_input_file']:
    #job.path_to_scrape # 'engine/payload/house_cat/_flatbread.py'
    path_to_scrape = job.custom_parameters['path_to_scrape']
    scraped_job_dicts, payload_location, module_name = get_job_dicts(path_to_scrape)

    if 'only_these_job_codes' in job.custom_parameters:
        scraped_job_dicts = [d for d in scraped_job_dicts if d['job_code'] in job.custom_parameters['only_these_job_codes']]

    # Convert list of dicts to a CSV file.
    from engine.etl_util import write_to_csv
    filename = job.source_file
    _, local_directory = local_file_and_dir(job, SOURCE_DIR)
    output_path = local_directory + filename
    write_to_csv(output_path, scraped_job_dicts, ['resource_name',
        'package', 'job_code', 'job_directory', 'source_full_url',
        'resource_description'])

def scrape_housecat_tables(job, **kwparameters):
    from engine.credentials import site, API_key
    filename = job.source_file
    _, local_directory = local_file_and_dir(job, SOURCE_DIR)
    output_path = local_directory + filename
    # 1) Go to all resource IDs in house_cat/sources.csv.
    list_of_dicts = []
    with open(re.sub('source_files', 'output_files', local_directory) + 'sources.csv', 'r') as g:
        reader = csv.DictReader(g)
        for row in reader:
            # 2) Get list of fields.
            fieldnames = get_resource_fields(site, row['resource_id'], API_key)[0]
            # 3) Add to output file.
            for f in fieldnames:
                row['field'] = f
                list_of_dicts.append(dict(row))
        from engine.etl_util import write_to_csv
        write_to_csv(output_path, list_of_dicts)
    ## 4) Change job.target to point to that output file

# dfg

from engine.payload.house_cat._parameters import housecat_tango_with_django_package_id

job_dicts = [
    {
        'job_code': HouseCatSourcesSchema().job_code, #'data_sources'
        'source_type': 'local',
        'source_file': 'sources.csv',
        #'encoding': 'binary',
        'custom_processing': scrape_rocket_jobs,
        'custom_parameters': {'path_to_scrape': 'engine/payload/house_cat/_flatbread.py'},
        'schema': HouseCatSourcesSchema,
        'filters': [['resource_description', '!=', None]], # We can only filter on fields in the source file.
        # If 'resource_description' is blank, filter out the source.
        # Otherwise, pull the source_landing_page out of there.
        'always_wipe_data': True,
        #'primary_key_fields': ['hud_project_number'],
        'destination': 'ckan',
        'destination_file': 'sources.csv',
        'package': housecat_tango_with_django_package_id,
        'resource_name': 'house_cat_data_sources',
        'upload_method': 'insert',
        'resource_description': f'Derived from engine/payload/house_cat/_flatbread.py',
        'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
    {
        'job_code': HouseCatSourcesSchema().job_code + '_phfa', #'data_sources_phfa'
        'source_type': 'local',
        'source_file': 'phfa_sources.csv',
        #'encoding': 'binary',
        'custom_processing': scrape_rocket_jobs,
        'custom_parameters': {'path_to_scrape': 'engine/payload/house_cat/_hfa.py'},
        'schema': HouseCatSourcesSchema,
        'filters': [['resource_description', '!=', None]], # We can only filter on fields in the source file.
        # If 'resource_description' is blank, filter out the source.
        # Otherwise, pull the source_landing_page out of there.
        'always_wipe_data': False,
        #'primary_key_fields': ['hud_project_number'],
        'destination': 'ckan',
        'destination_file': 'sources.csv',
        'package': housecat_tango_with_django_package_id,
        'resource_name': 'house_cat_data_sources',
        'upload_method': 'insert',
        #'resource_description': f'Derived from engine/payload/house_cat/_hfa.py',
        'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
    {
        'job_code': HouseCatSourcesSchema().job_code + '_tango', #'data_sources_tango'
        'source_type': 'local',
        'source_file': 'tango_sources.csv',
        #'encoding': 'binary',
        'custom_processing': scrape_rocket_jobs,
        'custom_parameters': {'path_to_scrape': 'engine/payload/house_cat/tango_with_django.py',
            'only_these_job_codes': 'hunt_and_peck'},
        'schema': HouseCatSourcesSchema,
        'filters': [['resource_description', '!=', None]], # We can only filter on fields in the source file.
        # If 'resource_description' is blank, filter out the source.
        # Otherwise, pull the source_landing_page out of there.
        'always_wipe_data': False,
        #'primary_key_fields': ['hud_project_number'],
        'destination': 'ckan',
        'destination_file': 'sources.csv',
        'package': housecat_tango_with_django_package_id,
        'resource_name': 'house_cat_data_sources',
        'upload_method': 'insert',
        #'resource_description': f'Derived from engine/payload/house_cat/tango_with_django.py',
        'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
    # Create listing of dumped fields by CKAN table/data source.
    # It might be easier to just scrape the CKAN tables, which we have already tabulated in sources.csv.
    # job_code  resource_id  list_of_field_names (pipe-delimited)
    # OR
    # job_code_0 resource_id_0 field_name_0
    # job_code_0 resource_id_0 field_name_1
    # job_code_0 resource_id_0 field_name_2
    # job_code_1 resource_id_1 field_name_0
    # job_code_1 resource_id_1 field_name_1
    # job_code_1 resource_id_1 field_name_2
    {
        'job_code': HouseCatFieldsSchema().job_code, #'fields'
        'source_type': 'local',
        'source_file': 'fields.csv',
        #'encoding': 'binary',
        'custom_processing': scrape_housecat_tables,
        #'custom_parameters': {'': ''},
        'schema': HouseCatFieldsSchema,
        #'filters': [['resource_description', '!=', None]], # We can only filter on fields in the source file.
        # If 'resource_description' is blank, filter out the source.
        # Otherwise, pull the source_landing_page out of there.
        'always_wipe_data': True,
        #'primary_key_fields': ['hud_project_number'],
        'destination': 'file',
        'destination_file': 'fields.csv',
        'package': housecat_tango_with_django_package_id,
        'resource_name': 'house_cat_fields',
        'upload_method': 'insert',
        #'resource_description': f'Derived from engine/payload/house_cat/tango_with_django.py',
        'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    }
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
