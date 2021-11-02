import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa


class KaneCensusSchema(pl.BaseSchema):
    facility = fields.String()
    run_date = fields.Date()
    gender_race_group = fields.String()
    patient_count = fields.Integer()
    gender = fields.String()
    race = fields.String()

    class Meta:
        ordered = True

    @pre_load()
    def make_iso_date(self, in_data):
        # Some early dates were in this form: 3/21/2019
        # Late dates are nicer: 2019-04-30
        if in_data['run_date']:
            in_data['run_date'] = parser.parse(in_data['run_date']).date().isoformat()
            #datetime.datetime.strptime(in_data['date'], '%m/%d/%Y').isoformat()

kane_package_id = 'db327693-d758-431e-9f59-a906bdef46b3' # Package ID for Kane Census dataset on production server

job_dicts = [
    {
        'job_code': 'kane',
        'source_type': 'sftp',
        'source_dir': 'Kane_Daily_Census',
        'source_file': 'kane-census.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'latin-1',
        'schema': KaneCensusSchema,
        'primary_key_fields': ['facility', 'run_date', 'gender_race_group'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'package': kane_package_id,
        'resource_name': 'Kane Census',
    },
]

print("This job was migrated to rocket-etl on 2021-11-02, at a time when the Kane feed was broken and no source file was available.")
