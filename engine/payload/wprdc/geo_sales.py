import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint
import re

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class GeoSalesSchema(pl.BaseSchema):
    parid = fields.String(dump_to="PARID", allow_none=True)
    propertyhousenum = fields.Integer(
        dump_to="PROPERTYHOUSENUM", allow_none=True)
    propertyfraction = fields.String(
        dump_to="PROPERTYFRACTION", allow_none=True)
    propertyaddressdir = fields.String(
        dump_to="PROPERTYADDRESSDIR", allow_none=True)
    propertyaddressstreet = fields.String(
        dump_to="PROPERTYADDRESSSTREET", allow_none=True)
    propertyaddresssuf = fields.String(
        dump_to="PROPERTYADDRESSSUF", allow_none=True)
    propertyaddressunitdesc = fields.String(
        dump_to="PROPERTYADDRESSUNITDESC", allow_none=True)
    propertyunitno = fields.String(dump_to="PROPERTYUNITNO", allow_none=True)
    propertycity = fields.String(dump_to="PROPERTYCITY", allow_none=True)
    propertystate = fields.String(dump_to="PROPERTYSTATE", allow_none=True)
    propertyzip = fields.Float(dump_to="PROPERTYZIP", allow_none=True)
    schoolcode = fields.String(dump_to="SCHOOLCODE", allow_none=True)
    schooldesc = fields.String(dump_to="SCHOOLDESC", allow_none=True)
    municode = fields.String(dump_to="MUNICODE", allow_none=True)
    munidesc = fields.String(dump_to="MUNIDESC", allow_none=True)
    recorddate = fields.Date(dump_to="RECORDDATE", allow_none=True)
    saledate = fields.Date(dump_to="SALEDATE", allow_none=True)
    price = fields.Float(dump_to="PRICE", allow_none=True)
    deedbook = fields.String(dump_to="DEEDBOOK", allow_none=True)
    deedpage = fields.String(dump_to="DEEDPAGE", allow_none=True)
    salecode = fields.String(dump_to="SALECODE", allow_none=True)
    saledesc = fields.String(dump_to="SALEDESC", allow_none=True)
    instrtyp = fields.String(dump_to="INSTRTYP", allow_none=True)
    instrtypdesc = fields.String(dump_to="INSTRTYPDESC", allow_none=True)

    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_dates(self, data):
        date_format = "%m-%d-%Y"
        if data['saledate']:
            data['saledate'] = datetime.strptime(
                data['saledate'], date_format).date().isoformat()
        if data['recorddate']:
            data['recorddate'] = datetime.strptime(
                data['recorddate'], date_format).date().isoformat()



geo_property_package_id = "6102a454-e7af-45c3-9d5a-e79e65a36a12" # Production version of Property Data with Geographic Identifiers 

job_dicts = [
    {
        'job_code': 'geosales',
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'geocoded_sales.csv',
        'encoding': 'latin-1',
        'schema': GeoSalesSchema,
        'primary_key_fields': ['PARID'],
        'always_wipe_data': False, # Should this be True or is it better for this table to be a cumulative archive
        # (in constrast with how the sales table currently works)?
        'upload_method': 'upsert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'air_daily.csv', # purposes.
        'package': geo_property_package_id,
        'resource_name': 'Property Sales Data with Parcel Centroids',
    },
]
