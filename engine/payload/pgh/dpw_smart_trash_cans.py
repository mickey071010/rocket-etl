import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import fetch_city_file
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class smartTrashCansSchema(pl.BaseSchema):
    container_id = fields.String(allow_none=False)
    receptacle_model_id = fields.String(allow_none=False)
    assignment_date = fields.DateTime(allow_none=False)
    last_updated_date = fields.DateTime(allow_none=False)
    group_name = fields.String(allow_none=False)
    address = fields.String(allow_none=False)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip = fields.String(allow_none=True)
    neighborhood = fields.String(allow_none=True)
    dpw_division = fields.String(allow_none=True)
    council_district = fields.String(allow_none=True)
    ward = fields.String(allow_none=True)
    fire_zone = fields.String(allow_none=True)
    x = fields.Float(allow_none=False)
    y = fields.Float(allow_none=False)

    class Meta:
        ordered = True

    @pre_load
    def fix_nas_and_dates(self, data):
        fields_to_fix = ['neighborhood', 'dpw_division',
                'council_district', 'ward', 'fire_zone']
        for field in fields_to_fix:
            if data[field] == 'NA':
                data[field] = None

    @pre_load
    def fix_datetimes(self, data):
        for k, v in data.items():
            if 'date' in k:
                if v:
                    try:
                        data[k] = parser.parse(v).isoformat()
                    except:
                        data[k] = None

smart_trash_cans_package_id = "b1282e47-6a70-4f18-98df-f081e7406e34" # Production version of Smart Trash Cans package

def conditionally_get_city_files(job, **kwparameters):
    if not kwparameters['use_local_input_file']:
        fetch_city_file(job)

job_dicts = [
    {
        'job_code': 'smart_trash',
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'smart_trash_containers.csv',
        'encoding': 'utf-8-sig',
        'custom_processing': conditionally_get_city_files,
        'schema': smartTrashCansSchema,
        'always_wipe_data': True,
        'primary_key_fields': ['container_id'],
        'upload_method': 'upsert',
        'package': smart_trash_cans_package_id,
        'resource_name': 'Smart Trash Containers',
    },
    {
        'job_code': 'smart_trash_geojson',
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'smart_trash_containers.geojson',
        'encoding': 'utf-8-sig',
        'custom_processing': conditionally_get_city_files,
        'schema': None,
        'destination': 'ckan_filestore',
        'package': smart_trash_cans_package_id, # [ ] Change this field to package_id
        'resource_name': 'Smart Trash Containers (GeoJSON)'
    },
]
