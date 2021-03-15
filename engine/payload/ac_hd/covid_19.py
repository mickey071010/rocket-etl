import os, csv, json, requests, sys, traceback
import ckanapi
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import find_resource_id, post_process
from engine.credentials import site, API_key

from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from unidecode import unidecode

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class DeathDemographicsSchema(pl.BaseSchema):
    category = fields.String()
    demographic = fields.String()
    deaths = fields.Integer()
    update_date = fields.Date()

    class Meta:
        ordered = True

class DeathsByDateSchema(pl.BaseSchema):
    date = fields.Date()
    deaths = fields.Integer()
    update_date = fields.Date()

    class Meta:
        ordered = True

class CasesByPlaceSchema(pl.BaseSchema):
    neighborhood_municipality = fields.String()
    indv_tested = fields.Integer()
    cases = fields.Integer()
    deaths = fields.Integer()
    update_date = fields.Date()

    class Meta:
        ordered = True

class TestsSchema(pl.BaseSchema):
    indv_id = fields.String()
    collection_date = fields.Date()
    test_result = fields.String(allow_none=True)
    case_status = fields.String()
    hospital_flag = fields.String(allow_none=True)
    icu_flag = fields.String(allow_none=True)
    vent_flag = fields.String(allow_none=True)
    age_bucket = fields.String()
    sex = fields.String()
    race = fields.String()
    ethnicity = fields.String()
    update_date = fields.Date()

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k in ['test_result', 'hospital_flag', 'icu_flag', 'vent_flag']:
                if v in ['NA']:
                    data[k] = None

def express_load_then_delete_file(job, **kwparameters):
    """The basic idea is that the job processes with a 'file' destination,
    so the ETL job loads the file into destination_file_path. Then as a
    custom post-processing step, that file is Express-Loaded. This is
    faster (particularly for large files) and avoids 504 errors and unneeded
    API requests."""
    # Eventually this function should be moved either to etl_util.py or
    # more likely the pipeline framework. In either case, this approach
    # can be formalized, either as a destination or upload method and
    # possibly implemented as a loader (CKANExpressLoader).
    if kwparameters['test_mode']:
        job.package_id = TEST_PACKAGE_ID
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    csv_file_path = job.destination_file_path
    resource_id = find_resource_id(job.package_id, job.resource_name)
    if resource_id is None:
        # If the resource does not already exist, create it.
        print(f"Unable to find a resource with name '{job.resource_name}' in package with ID {job.package_id}.")
        print(f"Creating new resource, and uploading CSV file {csv_file_path} to resource with name '{job.resource_name}' in package with ID {job.package_id}.")
        resource_as_dict = ckan.action.resource_create(package_id=job.package_id,
            name = job.resource_name,
            upload=open(csv_file_path, 'r'))
    else:
        print(f"Uploading CSV file {csv_file_path} to resource with name '{job.resource_name}' in package with ID {job.package_id}.")
        resource_as_dict = ckan.action.resource_patch(id = resource_id,
            upload=open(csv_file_path, 'r'))
        # Running resource_update once sets the file to the correct file and triggers some datastore action and
        # the Express Loader, but for some reason, it seems to be processing the old file.

        # So instead, let's run resource_patch (which just sets the file) and then run resource_update.
        #resource_as_dict = ckan.action.resource_update(id = resource_id)
        resource_as_dict = ckan.action.resource_update(id = resource_id,
            upload=open(csv_file_path, 'r'))

    print(f"Removing temp file at {csv_file_path}")
    os.remove(csv_file_path)

    # Since launchpad.py doesn't update the last_etl_update metadata value in this case
    # because this is a workaround, do it manually here:
    post_process(resource_id, job)
    # [ ] But really, an ExpressLoader is probably called for, or at least a standardized express_load_then_delete_file function.

covid_19_package_id = '80e0ca5d-c88a-4b1a-bf5d-51d0aaeeac86' # Production version of COVID-19 testing data package

job_dicts = [
    {
        'job_code': 'death_demographics',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'CovidDeathDemographics.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': DeathDemographicsSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'destination': 'file',
        'destination_file': f'covid_19_death_demographics.csv',
        'package': covid_19_package_id,
        'resource_name': f'Allegheny County COVID-19 Deaths by Demographic Groups',
        'custom_post_processing': express_load_then_delete_file
    },
    {
        'job_code': 'deaths_by_date',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'CovidDeathsTimeSeries.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': DeathsByDateSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'destination': 'file',
        'destination_file': f'covid_19_deaths_by_date.csv',
        'package': covid_19_package_id,
        'resource_name': f'Allegheny County COVID-19 Deaths by Date',
        'custom_post_processing': express_load_then_delete_file
    },
    {
        'job_code': 'cases_by_place',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'CovidMuniHoodCounts.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': CasesByPlaceSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'destination': 'file',
        'destination_file': f'covid_19_cases_by_place.csv',
        'package': covid_19_package_id,
        'resource_name': f'Allegheny County COVID-19 Counts by Municipality and Pittsburgh Neighborhood',
        'custom_post_processing': express_load_then_delete_file
    },
    {
        'job_code': 'tests',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'CovidTestingCases.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': TestsSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'destination': 'file',
        'destination_file': f'covid_19_tests.csv',
        'package': covid_19_package_id,
        'resource_name': f'Allegheny County COVID-19 Tests and Cases',
        'custom_post_processing': express_load_then_delete_file
    },
]
