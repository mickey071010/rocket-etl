import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import fetch_city_file
from engine.arcgis_util import get_arcgis_dataset, get_arcgis_data_url, standard_arcgis_job_dicts
from engine.notify import send_to_slack
from engine.scraping_util import scrape_nth_link
from engine.parameters.local_parameters import SOURCE_DIR, PRODUCTION

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

seeds = [] # The point of the seeds list is to let another counterpart script (called "counterpart.py")
# come along, pull the data_json_url and list of seeds out of this script, and
# then look for deviations between the live data.json file and the seeds coverage.

job_dicts = []

data_json_url = 'https://pghgishub-pittsburghpa.opendata.arcgis.com/data.json'
test_package_id = 'f618f456-0d69-46ff-abc2-1e80ef101c49' # Test package for arcgis_jobs.py

_, data_json_content = get_arcgis_dataset('Pittsburgh Steps', data_json_url, None) # Cache data.json
# to avoid looking it up for each job in this file.
#############

class StepsSchema(pl.BaseSchema):
    objectid = fields.Integer(load_from='\ufeffobjectid'.lower(), dump_to='objectid')
    length = fields.String(load_from='length'.lower(), dump_to='length')
    streets_ = fields.Integer(load_from='streets_'.lower(), dump_to='streets_')
    streets_id = fields.Integer(load_from='streets_id'.lower(), dump_to='streets_id')
    code = fields.Integer(load_from='code'.lower(), dump_to='code')
    _type = fields.Integer(load_from='type'.lower(), dump_to='type')
    id_no = fields.Integer(load_from='id_no'.lower(), dump_to='id_no')
    oid = fields.Integer(load_from='oid'.lower(), dump_to='oid')
    _id = fields.Integer(load_from='id'.lower(), dump_to='id')
    dpwn1 = fields.Integer(load_from='dpwn1'.lower(), dump_to='dpwn1')
    dpwn2 = fields.String(load_from='dpwn2'.lower(), dump_to='dpwn2')
    location = fields.String(load_from='location'.lower(), dump_to='location')
    style = fields.Integer(load_from='style'.lower(), dump_to='style')
    angle = fields.String(load_from='angle'.lower(), dump_to='angle')
    segs = fields.Integer(load_from='segs'.lower(), dump_to='segs')
    st_ = fields.String(load_from='st_'.lower(), dump_to='st_')
    street_nam = fields.String(load_from='street_nam'.lower(), dump_to='street_name')
    from_stree = fields.String(load_from='from_stree'.lower(), dump_to='from_street')
    to_street = fields.String(load_from='to_street'.lower(), dump_to='to_street')
    _int_ = fields.String(load_from='int_'.lower(), dump_to='int_')
    hood = fields.Integer(load_from='hood'.lower(), dump_to='hood')
    hood2 = fields.String(load_from='hood2'.lower(), dump_to='hood2')
    l_feet = fields.Integer(load_from='l_feet'.lower(), dump_to='l_feet')
    width = fields.Integer(load_from='width'.lower(), dump_to='width')
    steps = fields.Integer(load_from='steps'.lower(), dump_to='steps')
    treads = fields.Integer(load_from='treads'.lower(), dump_to='treads')
    year = fields.Integer(load_from='year'.lower(), dump_to='year')
    coded_ = fields.String(load_from='coded_'.lower(), dump_to='coded_')
    pix = fields.String(load_from='pix'.lower(), dump_to='pix')
    comment = fields.String(load_from='comment'.lower(), dump_to='comment')
    shape_length = fields.Float(load_from='Shape__Length'.lower(), dump_to='shape_length')

    class Meta:
        ordered = True

seeds.append({
        'arcgis_dataset_title': 'Pittsburgh Steps',
        'base_job_code': 'steps',
        'package_id': '9d35d609-e8f9-4c51-9dce-e7dd14e252d4', # Production version of Pittsburgh Steps package
        'schema': StepsSchema,
        'new_wave_format': True
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])

##########
class LandslidesSchema(pl.BaseSchema):
    fid = fields.Integer(load_from='FID'.lower(), dump_to='fid')
    code = fields.String(load_from='code'.lower(), dump_to='code')
    acres = fields.Float(load_from='acres'.lower(), dump_to='acres')
    sqmiles = fields.Float(load_from='sqmiles'.lower(), dump_to='sqmiles')
    created_user = fields.String(load_from='created_user'.lower(), dump_to='created_user')
    created_date = fields.String(load_from='created_date'.lower(), dump_to='created_date')
    last_edited_user = fields.String(load_from='last_edited_user'.lower(), dump_to='last_edited_user')
    last_edited_date = fields.String(load_from='last_edited_date'.lower(), dump_to='last_edited_date')
    landslideprone = fields.String(load_from='landslideprone'.lower(), dump_to='landslideprone')
    shape_length = fields.Float(load_from='SHAPE_Length'.lower(), dump_to='shape_length')
    shape_area = fields.Float(load_from='SHAPE_Area'.lower(), dump_to='shape_area')

    class Meta:
        ordered = True

seeds.append({
        'arcgis_dataset_title': 'Landslide Prone Areas',
        'base_job_code': 'landslides',
        'package_id': '6eb1be84-7abe-45c3-8a37-90db80ea6149', # Production package ID for Landslide Prone Areas
        'schema': LandslidesSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])
#####
class IPODSchema(pl.BaseSchema):
    fid = fields.String(load_from='\ufeffFID'.lower(), dump_to='fid')
    created_user = fields.String(load_from='created_user'.lower(), dump_to='created_user')
    created_date = fields.DateTime(load_from='created_date'.lower(), dump_to='created_date')
    last_edited_user = fields.String(load_from='last_edited_user'.lower(), dump_to='last_edited_user')
    last_edited_date = fields.DateTime(load_from='last_edited_date'.lower(), dump_to='last_edited_date')
    uptown_ipod = fields.String(load_from='uptown_ipod'.lower(), dump_to='uptown_ipod')
    shape_length = fields.Float(load_from='SHAPE_Length'.lower(), dump_to='shape_length')
    shape_area = fields.Float(load_from='SHAPE_Area'.lower(), dump_to='shape_area')

    class Meta:
        ordered = True

    @pre_load
    def fix_datetimes(self, data):
        for f in ['created_date', 'last_edited_date']:
            if data[f] not in ['', 'NA', None]:
                data[f] = parser.parse(data[f]).isoformat()

seeds.append({
        'arcgis_dataset_title': 'Uptown IPOD Zoning',
        'base_job_code': 'ipod',
        'package_id': '06c15511-37dd-44eb-9148-73c135457b07', # Production package ID for Uptown IPOD Zoning
        'schema': IPODSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])


assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
