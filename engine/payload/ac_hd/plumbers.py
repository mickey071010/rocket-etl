import csv, json, requests, sys, traceback, re
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from unidecode import unidecode

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class PlumbersSchema(pl.BaseSchema):
    registration_number = fields.String(load_from='hp_id')
    last_name = fields.String(load_from='lname')
    first_name = fields.String(load_from='fname')
    city = fields.String(allow_none=True)
    state = fields.String()
    zip = fields.String(allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_city(self, data):
        if 'city' in data:
            if data['city'] in [None, 'NA', '']:
                data['city'] = None
            else:
                data['city'] = re.sub('[,.`]+$', '', data['city'].strip()).upper()
                if data['city'] in ['PGH', 'PITTSBRUGH']:
                    data['city'] = 'PITTSBURGH'

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k in ['zip']:
                if v in ['NA']:
                    data[k] = None

plumbers_package_id = 'bafa99e3-6773-4dec-95bf-77d34b7754eb' # Production version of COVID-19 testing data package

job_dicts = [
    {
        'job_code': 'plumbers',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'alco-plumbers.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': PlumbersSchema,
        #'primary_key_fields': [], # Multiple records can exist for each registration number.
        'always_wipe_data': True,
        'upload_method': 'insert',
        #'destination_file': f'plumbers.csv',
        'package': plumbers_package_id,
        'resource_name': f'Plumbers'
    },
]
