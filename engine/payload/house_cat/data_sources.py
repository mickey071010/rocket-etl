import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from launchpad import get_job_dicts
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import local_file_and_dir
from engine.ckan_util import find_resource_id
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
        }


class HouseCatSourcesSchema(pl.BaseSchema):
    job_code = 'data_sources'
    resource_name = fields.String(load_from='resource_name'.lower(), dump_to='resource_name', allow_none=True)
    source_name = fields.String(load_from='resource_name'.lower(), dump_to='source_name', allow_none=True)
    package_id = fields.String(load_from='package'.lower(), dump_to='package_id', allow_none=True)
    resource_id = fields.String(load_from='resource_id', dump_to='resource_id', allow_none=True)
    other_job_code = fields.String(load_from='job_code'.lower(), dump_to='job_code')
    job_directory = fields.String(load_from='job_directory'.lower(), dump_to='job_directory')
    source_full_url = fields.String(load_from='source_full_url'.lower(), dump_to='source_full_url', allow_none=True)
    source_landing_page = fields.String(load_from='resource_description'.lower(), dump_to='source_landing_page', allow_none=True)
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
        ic(data[f])
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

def scrape_rocket_jobs(job, **kwparameters):
    #if not kwparameters['use_local_input_file']:
    #job.path_to_scrape # 'engine/payload/house_cat/_flatbread.py'
    path_to_scrape = job.custom_parameters['path_to_scrape']
    scraped_job_dicts, payload_location, module_name = get_job_dicts(path_to_scrape)
    # Convert list of dicts to a CSV file.
    from engine.etl_util import write_to_csv
    filename = job.source_file
    _, local_directory = local_file_and_dir(job, SOURCE_DIR)
    output_path = local_directory + filename
    write_to_csv(output_path, scraped_job_dicts, ['resource_name',
        'package', 'job_code', 'job_directory', 'source_full_url',
        'resource_description'])

# dfg

housecat_package_id = 'bb77b955-b7c3-4a05-ac10-448e4857ade4'

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
        'package': housecat_package_id,
        'resource_name': 'house_cat Data Sources',
        'upload_method': 'insert',
        'resource_description': f'Derived from engine/payload/house_cat/_flatbread.py',
        'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
