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

def rev_geocode(pin):
    if pin:
        r = requests.get(
            'http://tools.wprdc.org/geo/reverse_geocode/', params={'pin': pin})
        if r.status_code == 200:
            j = r.json()
            return j['results']
        else:
            return None

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

    #municipality = fields.String(dump_to="MUNICIPALITY", allow_none=True)
    #neighborhood = fields.String(dump_to="NEIGHBORHOOD", allow_none=True)
    #pgh_council_district = fields.String(dump_to="PGH_COUNCIL_DISTRICT", allow_none=True)
    #pgh_ward = fields.String(dump_to="PGH_WARD", allow_none=True)
    #pgh_public_works_division = fields.String(dump_to="PGH_PUBLIC_WORKS_DIVISION", allow_none=True)
    #pgh_police_zone = fields.String(dump_to="PGH_POLICE_ZONE", allow_none=True)
    #pgh_fire_zone = fields.String(dump_to="PGH_FIRE_ZONE", allow_none=True)
    #tract = fields.String(dump_to="TRACT", allow_none=True)
    #block_group = fields.String(dump_to="BLOCK_GROUP", allow_none=True)

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

    @pre_load
    def reverse_geocode(self, data):
        pass
        geo_data = rev_geocode(data['parid'])
        if geo_data:
            if 'pittsburgh_neighborhood' in geo_data:
                data['neighborhood'] = geo_data['pittsburgh_neighborhood']['name']

            if 'pittsburgh_city_council' in geo_data:
                data['pgh_council_district'] = geo_data['pittsburgh_city_council']['name']

            if 'pittsburgh_ward' in geo_data:
                data['pgh_ward'] = geo_data['pittsburgh_ward']['name']

            if 'us_census_tract' in geo_data:
                data['tract'] = geo_data['us_census_tract']['name']

            if 'pittsburgh_dpw_division' in geo_data:
                data['pgh_public_works_division'] = geo_data['pittsburgh_dpw_division']['name']

            if 'pittsburgh_police_zone' in geo_data:
                data['pgh_police_zone'] = geo_data['pittsburgh_police_zone']['name']

            if 'pittsburgh_fire_zone' in geo_data:
                data['pgh_fire_zone'] = geo_data['pittsburgh_fire_zone']['name']

            if 'allegheny_county_municipality' in geo_data:
                data['municipality'] = geo_data['allegheny_county_municipality']['name']

            if 'us_block_group' in geo_data:
                data['block_group'] = geo_data['us_block_group']['name']

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
        'upload_method': 'upsert',
        'destinations': ['file'], # These lines are just for testing
        'destination_file': f'sales.csv', # purposes.
        'package': sales_package_id,
        'resource_name': 'Property Sales Transactions',
    }
]
