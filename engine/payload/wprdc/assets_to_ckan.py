import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class AssetSchema(pl.BaseSchema):
    name = fields.String()
    asset_type = fields.String()
    #raw_asset_ids = fields.String(allow_none=True)
    tags = fields.String(allow_none=True)
    location_id = fields.Integer(allow_none=True)
    street_address = fields.String(allow_none=True)
    unit = fields.String(allow_none=True)
    unit_type = fields.String(allow_none=True)
    municipality = fields.String(allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip_code = fields.String(allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    parcel_id = fields.String(allow_none=True)
    residence = fields.Boolean(allow_none=True)
    iffy_geocoding = fields.String(allow_none=True)
    available_transportation = fields.String(allow_none=True)
    parent_location_id = fields.Integer(allow_none=True)
    parent_location = fields.String(allow_none=True)
    url = fields.String(allow_none=True)
    email = fields.String(allow_none=True)
    phone = fields.String(allow_none=True)
    hours_of_operation = fields.String(allow_none=True)
    holiday_hours_of_operation = fields.String(allow_none=True)
    periodicity = fields.String(allow_none=True)
    capacity = fields.Integer(allow_none=True)
    wifi_network = fields.String(allow_none=True)
    internet_access = fields.String(allow_none=True)
    computers_available = fields.Boolean(allow_none=True)
    accessibility = fields.Boolean(allow_none=True)
    open_to_public = fields.String(allow_none=True)
    child_friendly = fields.Boolean(allow_none=True)
    sensitive = fields.Boolean(allow_none=True)
    do_not_display = fields.Boolean(allow_none=True)
    localizability = fields.String()
    services = fields.String(allow_none=True)
    hard_to_count_population = fields.String(allow_none=True)
    data_source_names = fields.String(allow_none=True)
    data_source_urls = fields.String(allow_none=True)
    organization_name = fields.String(allow_none=True)
    organization_phone = fields.String(allow_none=True)
    organization_email = fields.String(allow_none=True)
    etl_notes = fields.String(allow_none=True)
    geocoding_properties = fields.String(allow_none=True)

    class Meta:
        ordered = True

asset_package_id = "cd2b3e27-ca31-43e0-a8c6-2e6c43b4050a" # Production version of Asset package

job_dicts = [
        {
        'source_type': 'http',
        'source_file': 'asset_dump.csv',
        'source_full_url': 'https://assets.wprdc.org/asset_dump.csv',
        'encoding': 'utf-8-sig',
        'schema': AssetSchema,
        #'primary_key_fields': ['synthesized_key'],
        'always_wipe_data': True,
        'upload_method': 'insert',
        #'destinations': ['file'],
        #'destination_file': f'asset_dump.csv',
        'package': asset_package_id,
        'resource_name': 'All Allegheny County Assets for the Asset map'
    },
]
