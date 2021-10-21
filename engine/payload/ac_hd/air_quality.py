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

# Now we're down to just four files:
# aqi_daily.csv
# hourly_readings.csv
# sourcesites.csv
# sourcesites.geojson

class AQIDailySchema(pl.BaseSchema):
    date = fields.Date(load_from='Date'.lower(), dump_to='date')
    site_name = fields.String(load_from='SiteName'.lower(), dump_to='site')
    parameter_name = fields.String(load_from='ParameterName'.lower(), dump_to='parameter')
    index_value = fields.Integer(load_from='IndexValue'.lower(), dump_to='index_value')
    #reported_unit_name = fields.String(load_from='ReportedUnitName'.lower(), dump_to='unit') # It doesn't seem like this is necessary here.
    description = fields.String(load_from='Description'.lower(), dump_to='description')
    health_advisory = fields.String(load_from='HealthAdvisory'.lower(), dump_to='health_advisory', allow_none=True)
    health_effects = fields.String(load_from='HealthEffects'.lower(), dump_to='health_effects', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nones(self, data):
        for k, v in data.items():
            if k in ['healthadvisory', 'healtheffects']:
                if v in ['None', 'NA']:
                    data[k] = None

class HistoricAQIDailySchema(AQIDailySchema):

    @pre_load
    def convert_floaty_index(self, data):
        f = 'indexvalue'
        if f in data and data[f] is not None:
            data[f] = int(float(data[f]))

class AQIHourlySchema(pl.BaseSchema):
    datetime_est = fields.DateTime(load_from='datetimeest', dump_to='datetime_est')
    site_name = fields.String(load_from='SiteName'.lower(), dump_to='site')
    parameter_name = fields.String(load_from='ParameterName'.lower(), dump_to='parameter')
    is_valid = fields.Boolean(load_from='IsValid'.lower(), dump_to='is_valid')
    report_value = fields.String(load_from='ReportValue'.lower(), dump_to='report_value', allow_none=True)
    reported_unit_name = fields.String(load_from='ReportedUnitName'.lower(), dump_to='unit')
    reported_unit_description = fields.String(load_from='ReportedUnitDescription'.lower(), dump_to='unit_description')
    highest_flag = fields.String(load_from='HighestFlag'.lower(), dump_to='highest_flag', allow_none=True)
    aqs_parameter_category = fields.String(load_from='AqsParameterCategory'.lower(), dump_to='aqs_parameter_category', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        fields_with_nas = ['reportvalue', 'highestflag', 'aqsparametercategory']
        for f in fields_with_nas:
            if data[f] in ['NA']:
                data[f] = None

class MeasurementSites(pl.BaseSchema):
    site_name = fields.String(load_from='SiteName'.lower())
    description = fields.String(load_from='Description'.lower(), allow_none=True)
    air_now_mnemonic = fields.String(load_from='AirNowMnemonic'.lower(), allow_none=True)
    address = fields.String(load_from='address'.lower(), allow_none=True)
    #county = fields.String(load_from='County'.lower(), allow_none=True)
    latitude = fields.Float(load_from='Latitude'.lower(), allow_none=True)
    longitude = fields.Float(load_from='Longitude'.lower(), allow_none=True)
    enabled = fields.Boolean(load_from='Enabled'.lower())

    class Meta:
        ordered = True

    @pre_load
    def fix_nones(self, data):
        for k, v in data.items():
            if k in ['description', 'AirNowMnemonic'.lower(), 'address'.lower(),
                    'latitude', 'longitude']:
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

air_quality_package_id = '4659f303-d189-489c-8c94-87e1fb7407cf' # Test version of air-quality data package
#air_quality_package_id = 'c7b3266c-adc6-41c0-b19a-8d4353bfcdaf' # Production version of air-quality data package

job_dicts = [
    {
        'job_code': 'aqi_daily',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'aqi_daily.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': AQIDailySchema,
        'primary_key_fields': ['date', 'site', 'parameter'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'package': air_quality_package_id,
        'resource_name': f'Daily AQI Data'
    },
    {
        'job_code': 'aqi_daily_historic',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'aqi_daily_historic_2021_10_19.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': HistoricAQIDailySchema,
        'primary_key_fields': ['date', 'site', 'parameter'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'destination_file': f'aqi_daily.csv',
        'package': air_quality_package_id,
        'resource_name': f'Daily AQI Data'
    },
    {
        'job_code': 'air_hourly',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        #'source_file': f'hourly_readings.csv',
        'source_file': f'hourly_readings_historic_2021_10_19.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': AQIHourlySchema,
        'filters': [['parametername', 'not in', ['Sound', 'RCL TEMP']]], # Edit out some experimental fields.
        'primary_key_fields': ['datetime_est', 'site', 'parameter'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'destination_file': f'air_hourly.csv',
        'package': air_quality_package_id,
        'resource_name': f'Hourly Air Quality Data (new format)'
    },
    {
        'job_code': 'measurement_sites',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'sourcesites.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': MeasurementSites,
        'primary_key_fields': ['site_name'],
        'always_wipe_data': True,
        'upload_method': 'upsert',
        #'destination_file': f'sourcesites.csv',
        'package': air_quality_package_id,
        'resource_name': f"Sensor Locations",
    },
    {
        'job_code': 'measurement_sites_geojson',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': 'sourcesites.geojson',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': None,
        #'destination_file': f'sourcesites.geojson',
        'destination': 'ckan_filestore',
        'destination_file': 'sourcesites.geojson', # This sets the
        # filename that CKANFilestoreLoader will use to name the file.
        'package': air_quality_package_id,
        'resource_name': 'Sensor Locations (GeoJSON)'
    },
]
