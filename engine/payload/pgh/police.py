# This job was migrated to rocket just to create a CSV file to upload to work around some weird Server 500 CKAN error,
# but it could easily be deployed to replace the old job.

import csv, json, requests, sys, traceback, re
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load, pre_dump
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.etl_util import fetch_city_file

blacklist = ['sex', 'rape']

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def rev_geocode(lon, lat):
    if lat and lon:
        r = requests.get('http://tools.wprdc.org/geo/reverse_geocode/'
                         '?lat=' + str(lat) +
                         '&lng=' + str(lon))
        j = json.loads(r.text)
        try:
            dpw = j['results']['pittsburgh_dpw_division']['name']
            cc = j['results']['pittsburgh_city_council']['name']
        except:
            dpw, cc = None, None

        return dpw, cc
    else:
        return None, None

class PreBlotterSchema(pl.BaseSchema):
    pk = fields.String(dump_to="PK")
    ccr = fields.String(dump_to="CCR", allow_none=True)
    hierarchy = fields.Integer(dump_to="HIERARCHY", allow_none=True)
    incidentdate = fields.String(load_only=True, allow_none=True)
    incidenttime = fields.DateTime(dump_to="INCIDENTTIME", allow_none=True)
    incidentlocation = fields.String(dump_to="INCIDENTLOCATION", allow_none=True)
    clearedflag = fields.String(dump_to="CLEAREDFLAG", allow_none=True)
    incidentneighborhood = fields.String(dump_to="INCIDENTNEIGHBORHOOD", allow_none=True)
    incidentzone = fields.String(dump_to="INCIDENTZONE", allow_none=True)
    hierarchydesc = fields.String(dump_to="INCIDENTHIERARCHYDESC", allow_none=True)
    offenses = fields.String(dump_to="OFFENSES", allow_none=True)
    incidentract = fields.String(dump_to="INCIDENTTRACT", allow_none=True)
    council_district = fields.String(dump_only=True, dump_to="COUNCIL_DISTRICT", allow_none=True)
    public_works_division = fields.String(dump_only=True, dump_to="PUBLIC_WORKS_DIVISION", allow_none=True)
    x = fields.Float(dump_to="X", allow_none=True)
    y = fields.Float(dump_to="Y", allow_none=True)

    class Meta():
        ordered = True

    @pre_load
    def fix_time(self, data):
        if data['incidenttime']:
            data['incidenttime'] = datetime.strptime(data['incidenttime'], "%m/%d/%Y %H:%M").isoformat()

    @pre_load
    def anon_sex_crimes(self, data):
        fix = False
        for word in blacklist:
            if word in data['offenses']:
                # check that it's a zone
                fix = True
                break

        if fix:
            if not re.match(r'^Zone\s\d+\s*\Z', data['incidentlocation']):
                data['incidentlocation'] = "Zone " + data['incidentzone']
            data['incidentneighborhood'], data['incidenttract'], data['X'], data['Y'] = None, None, None, None

    @pre_dump
    def get_zones(self, data):
        data['public_works_division'], data['council_district'] = rev_geocode(data['x'], data['y'])

def conditionally_get_city_files(job, **kwparameters):
    if not kwparameters['use_local_files']:
        fetch_city_file(job)

blotter_package_id = "046e5b6a-0f90-4f8e-8c16-14057fd8872e"

job_dicts = [
    {
        'job_code': '30_day_blotter',
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'Blotter_PreUCR.csv',
        'encoding': 'latin-1',
        'custom_processing': conditionally_get_city_files,
        'schema': PreBlotterSchema,
        'always_wipe_data': True,
        'primary_key_fields': ['PK'],
        'upload_method': 'upsert',
        'package': blotter_package_id,
        'resource_name': 'Blotter Data',
    },
]
