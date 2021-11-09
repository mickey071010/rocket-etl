import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import fetch_city_file
from engine.notify import send_to_slack
from engine.parameters.local_parameters import SOURCE_DIR, PRODUCTION
from engine.post_processors import check_for_empty_table

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa


class MunicipalityProfilesSchema(pl.BaseSchema):
    job_code = 'muni_profiles'
    metric_name = fields.String(load_from='METRIC_NAME'.lower(), dump_to='metric_name')
    report_group = fields.String(load_from='REPORT_GROUP'.lower(), dump_to='report_group')
    caldr_yr = fields.Integer(load_from='CALDR_YR'.lower(), dump_to='calendar_year')
    kpi_count = fields.Integer(load_from='KPI_COUNT'.lower(), dump_to='kpi_count', allow_none=True)
    geo_area_name = fields.String(load_from='GEO_AREA_NAME'.lower(), dump_to='geo_area_name')
    population_name = fields.String(load_from='POPULATION_NAME'.lower(), dump_to='population_name')
    acs_5_yr_est = fields.Integer(load_from='ACS_5_YR_EST'.lower(), dump_to='acs_5_yr_est')
    kpi_population = fields.Integer(load_from='KPI_POPULATION'.lower(), dump_to='kpi_population', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        fields_with_nas = ['kpi_count', 'kpi_population']
        for f in fields_with_nas:
            if data[f] in ['NA', 'NULL']:
                data[f] = None

class PittsburghNeighborhoodProfilesSchema(pl.BaseSchema):
    job_code = 'hood_profiles'
    metric_name = fields.String(load_from='METRIC_NAME'.lower(), dump_to='metric_name')
    report_group = fields.String(load_from='REPORT_GROUP'.lower(), dump_to='report_group')
    caldr_yr = fields.Integer(load_from='CALDR_YR'.lower(), dump_to='calendar_year')
    kpi_count = fields.Integer(load_from='KPI_COUNT'.lower(), dump_to='kpi_count', allow_none=True)
    geo_area_name = fields.String(load_from='GEO_AREA_NAME'.lower(), dump_to='geo_area_name')

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        fields_with_nas = ['kpi_count']
        for f in fields_with_nas:
            if data[f] in ['NA', 'NULL']:
                data[f] = None

# dfg

ac_profiles_package_id = '3f5b398f-ac2b-428c-b033-0f11935e16d1'

job_dicts = [
    {
        'job_code': MunicipalityProfilesSchema().job_code, # 'muni_profiles'
        'source_type': 'sftp',
        'source_dir': 'DHS',
        'source_file': 'MunicipalityProfiles.csv',
        'connector_config_string': 'sftp.county_sftp',
        'updates': 'Quarterly',
        'schema': MunicipalityProfilesSchema,
        'always_wipe_data': True,
        'destination': 'ckan',
        'package': ac_profiles_package_id,
        'resource_name': 'Municipality Profiles',
        'upload_method': 'insert',
        'custom_post_processing': check_for_empty_table, # Why not?
    },
    {
        'job_code': PittsburghNeighborhoodProfilesSchema().job_code, # 'hood_profiles'
        'source_type': 'sftp',
        'source_dir': 'DHS',
        'source_file': 'PittsburghProfiles.csv',
        'connector_config_string': 'sftp.county_sftp',
        'updates': 'Quarterly',
        'schema': PittsburghNeighborhoodProfilesSchema,
        'always_wipe_data': True,
        'destination': 'ckan',
        'package': ac_profiles_package_id,
        'resource_name': 'Pittsburgh Neighborhood Profiles',
        'upload_method': 'insert',
        'custom_post_processing': check_for_empty_table, # Why not?
    },
]
# Schedule information:
#The ETL process delivers the dataset quarterly on the 1st day of the second month of each quarter, so 2/1, 5/1, 8/1 and 11/1. The file is initially sent from DHS. CountyStat grabs it and adds some Census Data and removes some extraneous fields and puts it in CSV format, starting at around 9:15 AM.

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
