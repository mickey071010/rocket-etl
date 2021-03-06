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

class SalesSchema(pl.BaseSchema):
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

    class Meta:
        ordered = True

    @pre_load()
    def fix_dates(self, data):
        date_format = "%m-%d-%Y"
        if data['saledate']:
            data['saledate'] = datetime.strptime(
                data['saledate'], date_format).date().isoformat()
        if data['recorddate']:
            data['recorddate'] = datetime.strptime(
                data['recorddate'], date_format).date().isoformat()

sales_package_id = '9e0ce87d-07b8-420c-a8aa-9de6104f61d6'

job_dicts = [
    {
        'job_code': 'sales',
        'source_type': 'sftp',
        'source_dir': 'Property_Assessments',
        'source_file': 'AA301PAALL.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': SalesSchema,
        'primary_key_fields': ["PARID", "RECORDDATE", "SALEDATE", "DEEDBOOK",
                      "DEEDPAGE", "INSTRTYP", "PRICE", "SALECODE"],
        'always_wipe_data': True,
        'upload_method': 'upsert', # This job must be done by upsert
        # because the source file contains a small percentage of duplicate rows.

        #'destination_file': f'sales.csv', # purposes.
        'package': sales_package_id,
        'resource_name': 'Property Sales Transactions',
    }
]
