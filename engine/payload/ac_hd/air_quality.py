import os, csv, json, requests, sys, traceback
import ckanapi
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import find_resource_id
from engine.credentials import site, API_key

from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from unidecode import unidecode

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

#
#The new AQI Data feed is now up and running! However, we are awaiting approval from the Health Department for a new Air Quality Dashboard, so I think we should time out releasing the new version with that. I’ve attached the data dictionaries for the files. Here is a break down of the relevant files for you to test ETL’s:

#    aqi_daily.csv – Last week of daily AQI readings for all sites
#    aqi_hourly – Last week of hourly AQI readings for all sites
#    aqi_today.csv – Current Max AQI readings for the day (overwrites every hour)
#    sourcesites.csv – Current and past sites used for AQI readings.
#    wprdc_daily_historical_20201116 – Historical daily AQI readings since Jan 1st 2016.
#    wprdc_hourly_historical_20201116 – Historical hourly AQI readings since Jan 1st 2016.
#


class AirQualityDailySchema(pl.BaseSchema):
    date = fields.Date(load_from='AqiDate'.lower())
    site = fields.String(load_from='SiteName'.lower())
    program = fields.String(load_from='AQIProgramName'.lower())
    parameter = fields.String(load_from='ParameterName'.lower())
    parameter_concentration = fields.String(load_from='ParameterConcentration'.lower(), allow_none=True)
    unit_description = fields.String(load_from='AqsUnitDescription'.lower(), allow_none=True)
    unit = fields.String(load_from='Unit'.lower())
    air_quality_index = fields.Integer(load_from='AirQualityIndex'.lower(), allow_none=True)
    category_description = fields.String(load_from='CategoryDescription'.lower(), allow_none=True)
    health_advisory = fields.String(load_from='HealthAdvisory'.lower(), allow_none=True)
    health_effects = fields.String(load_from='HealthEffects'.lower(), allow_none=True)
    sensitive_groups = fields.String(load_from='SensitiveGroups'.lower(), allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nones(self, data):
        for k, v in data.items():
            if k in ['healthadvisory', 'healtheffects', 'sensitivegroups']:
                if v in ['None', 'NA']:
                    data[k] = None

class AirQualityHourlySchema(pl.BaseSchema):
    datetime = fields.DateTime(load_from='AqiDateTimeEST'.lower())
    datetime_utc = fields.DateTime(load_from='AqiDateTimeUTC'.lower())
    site = fields.String(load_from='SiteName'.lower())
    program = fields.String(load_from='ResponsibleAQIProgramName'.lower())
    parameter = fields.String(load_from='ResponsibleParameterName'.lower())
    parameter_concentration = fields.String(load_from='ParameterConcentration'.lower(), allow_none=True)
    unit_description = fields.String(load_from='AqsUnitDescription'.lower(), allow_none=True)
    unit = fields.String(load_from='Unit'.lower())
    air_quality_index = fields.Integer(load_from='AirQualityIndex'.lower(), allow_none=True)
    category_description = fields.String(load_from='CategoryDescription'.lower(), allow_none=True)
    health_advisory = fields.String(load_from='HealthAdvisory'.lower(), allow_none=True)
    health_effects = fields.String(load_from='HealthEffects'.lower(), allow_none=True)
    sensitive_groups = fields.String(load_from='SensitiveGroups'.lower(), allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nones(self, data):
        for k, v in data.items():
            if k in ['healthadvisory', 'healtheffects', 'sensitivegroups']:
                if v in ['None', 'NA']:
                    data[k] = None

class MeasurementSites(pl.BaseSchema):
    site_name = fields.String(load_from='SiteName'.lower())
    description = fields.String(load_from='Description'.lower(), allow_none=True)
    air_now_mnemonic = fields.String(load_from='AirNowMnemonic'.lower(), allow_none=True)
    street_address1 = fields.String(load_from='StreetAddress1'.lower(), allow_none=True)
    street_address2 = fields.String(load_from='StreetAddress2'.lower(), allow_none=True)
    city = fields.String(load_from='City'.lower(), allow_none=True)
    county = fields.String(load_from='County'.lower(), allow_none=True)
    state_region = fields.String(load_from='StateRegion'.lower(), allow_none=True)
    zip_code = fields.Date(load_from='ZipCode'.lower(), allow_none=True)
    latitude = fields.Float(load_from='Latitude'.lower(), allow_none=True)
    longitude = fields.Float(load_from='Longitude'.lower(), allow_none=True)
    enabled = fields.String(load_from='Enabled'.lower())

    class Meta:
        ordered = True

    @pre_load
    def fix_nones(self, data):
        for k, v in data.items():
            if k in ['description', 'AirNowMnemonic'.lower(), 'StreetAddress1'.lower(),
                    'StreetAddress2'.lower(), 'city', 'county', 'StateRegion'.lower(),
                    'zipcode', 'latitude', 'longitude']:
                if v in ['None', 'NA']:
                    data[k] = None

    @pre_load
    def fix_booleans(self, data):
        for k, v in data.items():
            if k in ['enabled']:
                if v in ['None', 'NA']:
                    data[k] = None
                else:
                    data[k] == (data[k].lower() == 'true')

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
        job.package = TEST_PACKAGE_ID
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    csv_file_path = job.destination_file_path
    resource_id = find_resource_id(job.package, job.resource_name)
    if resource_id is None:
        # If the resource does not already exist, create it.

        # [ ] However, it's necessary to do something to set the schema.
        print(f"Unable to find a resource with name '{job.resource_name}' in package with ID {job.package}.")
        print(f"Creating new resource, and uploading CSV file {csv_file_path} to resource with name '{job.resource_name}' in package with ID {job.package}.")
        resource_as_dict = ckan.action.resource_create(package_id=job.package,
            name = job.resource_name,
            upload=open(csv_file_path, 'r'))
    else:
        print(f"Uploading CSV file {csv_file_path} to resource with name '{job.resource_name}' in package with ID {job.package}.")
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

air_quality_package_id = 'c7b3266c-adc6-41c0-b19a-8d4353bfcdaf' # Production version of air-quality data package

job_dicts = [
    {
        'job_code': 'air_daily',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'aqi_daily.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': AirQualityDailySchema,
        'primary_key_fields': ['date', 'site', 'program', 'parameter'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'air_daily.csv', # purposes.
        'package': air_quality_package_id,
        'resource_name': f'Daily Air Quality Data (new format)'
    },
    {
        'job_code': 'air_hourly',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'aqi_hourly.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': AirQualityHourlySchema,
        'primary_key_fields': ['datetime_utc', 'site', 'program', 'parameter'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'air_hourly.csv', # purposes.
        'package': air_quality_package_id,
        'resource_name': f'Hourly Air Quality Data (new format)'
    },
    {
        'job_code': 'air_max_today',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'aqi_today.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': AirQualityHourlySchema,
        'primary_key_fields': ['datetime_utc', 'site', 'program', 'parameter'],
        'always_wipe_data': True,
        'upload_method': 'upsert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'air_max_today.csv', # purposes.
        'package': air_quality_package_id,
        'resource_name': f"Current Maximum Air Quality Readings for Today (new format)",
#        'custom_post_processing': express_load_then_delete_file
    },
#    { # Commented out until the schema has been adjusted to include the full address.
#        'job_code': 'measurement_sites',
#        'source_type': 'sftp',
#        'source_dir': 'Health Department',
#        'source_file': f'sourcesites.csv',
#        'connector_config_string': 'sftp.county_sftp',
#        'encoding': 'utf-8-sig',
#        'schema': MeasurementSites,
#        'primary_key_fields': ['sitename'],
#        'always_wipe_data': True,
#        'upload_method': 'upsert',
#        'destinations': ['file'], # These lines are just for testing
#        'destination_file': f'sourcesites.csv', # purposes.
#        'package': air_quality_package_id,
#        'resource_name': f"Sensor Locations (new format)",
#    },
]
