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
    fid = fields.String(load_from='\ufeffFID'.lower(), dump_to='fid')
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

#####
class HistoricDistrictsSchema(pl.BaseSchema):
    _objectid = fields.String(load_from='\ufeffobjectid'.lower(), dump_to='objectid')
    _type = fields.String(load_from='type'.lower(), dump_to='type')
    historic_name = fields.String(load_from='historic_name'.lower(), dump_to='historic_name')
    created_user = fields.String(load_from='created_user'.lower(), dump_to='created_user')
    created_date = fields.String(load_from='created_date'.lower(), dump_to='created_date')
    last_edited_user = fields.String(load_from='last_edited_user'.lower(), dump_to='last_edited_user')
    last_edited_date = fields.String(load_from='last_edited_date'.lower(), dump_to='last_edited_date')
    guideline_link = fields.String(load_from='guideline_link'.lower(), dump_to='guideline_link', allow_none=True)
    shape__area = fields.Float(load_from='Shape__Area'.lower(), dump_to='shape_area')
    shape__length = fields.Float(load_from='Shape__Length'.lower(), dump_to='shape_length')

    class Meta:
        ordered = True

    @pre_load
    def fix_datetimes(self, data):
        for f in ['created_date', 'last_edited_date']:
            if data[f] not in ['', 'NA', None]:
                data[f] = parser.parse(data[f]).isoformat()

seeds.append({
        'arcgis_dataset_title': 'City Designated Historic Districts',
        'base_job_code': 'historic_districts',
        'package_id': '8f92ae09-4cfa-4e0d-9c46-779a66d93d1e', # Production package ID for City Designated Historic Districts
        'schema': HistoricDistrictsSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])

#####
class ParksSchema(pl.BaseSchema):
    _objectid_1 = fields.String(load_from='\ufeffobjectid_1'.lower(), dump_to='objectid_1')
    objectid = fields.String(load_from='objectid'.lower(), dump_to='objectid')
    acreage = fields.Float(load_from='acreage'.lower(), dump_to='acreage')
    sqft = fields.Float(load_from='sqft'.lower(), dump_to='sqft')
    final_cat = fields.String(load_from='final_cat'.lower(), dump_to='final_cat')
    type_ = fields.String(load_from='type_'.lower(), dump_to='type')
    sector = fields.Integer(load_from='sector'.lower(), dump_to='sector', allow_none=True)
    origpkname = fields.String(load_from='origpkname'.lower(), dump_to='origpkname', allow_none=True)
    updatepknm = fields.String(load_from='updatepknm'.lower(), dump_to='updatepknm')
    alterntnam = fields.String(load_from='alterntnam'.lower(), dump_to='alterntnam', allow_none=True)
    divname = fields.String(load_from='divname'.lower(), dump_to='divname', allow_none=True)
    shape_leng = fields.Float(load_from='shape_leng'.lower(), dump_to='shape_leng', allow_none=True)
    globalid = fields.String(load_from='globalid'.lower(), dump_to='globalid', allow_none=True)
    created_user = fields.String(load_from='created_user'.lower(), dump_to='created_user')
    created_date = fields.String(load_from='created_date'.lower(), dump_to='created_date')
    last_edited_user = fields.String(load_from='last_edited_user'.lower(), dump_to='last_edited_user')
    last_edited_date = fields.String(load_from='last_edited_date'.lower(), dump_to='last_edited_date')
    maintenanceresponsibility = fields.String(load_from='maintenanceresponsibility'.lower(), dump_to='maintenanceresponsibility', allow_none=True)
    dpw_ac = fields.String(load_from='dpw_ac'.lower(), dump_to='dpw_ac')
    global_iD_2 = fields.String(load_from='GlobalID_2'.lower(), dump_to='globalid_2')
    shape__area = fields.String(load_from='Shape__Area'.lower(), dump_to='shape_area')
    shape__length = fields.Float(load_from='Shape__Length'.lower(), dump_to='shape_length')

    class Meta:
        ordered = True

    @pre_load
    def fix_datetimes(self, data):
        for f in ['created_date', 'last_edited_date']:
            if data[f] not in ['', 'NA', None]:
                data[f] = parser.parse(data[f]).isoformat()

seeds.append({
        'arcgis_dataset_title': 'Parks',
        'base_job_code': 'parks',
        'package_id': 'e298e2ae-07c0-4aa4-a2ca-2c8db845b552', # Production package ID for Pittsburgh Parks
        'schema': ParksSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])

#####
class WardsSchema(pl.BaseSchema):
    _fid = fields.String(load_from='\ufeffFID'.lower(), dump_to='fid')
    ward = fields.String(load_from='ward'.lower(), dump_to='ward')
    acres = fields.Float(load_from='acres'.lower(), dump_to='acres')
    sqmiles = fields.Float(load_from='sqmiles'.lower(), dump_to='sqmiles')
    wardtext = fields.String(load_from='wardtext'.lower(), dump_to='wardtext')
    county_ward = fields.String(load_from='county_ward'.lower(), dump_to='county_ward')
    ward_county = fields.String(load_from='ward_county'.lower(), dump_to='ward_county')
    municode = fields.String(load_from='municode'.lower(), dump_to='municode')
    municode2 = fields.String(load_from='municode2'.lower(), dump_to='municode2')
    shape_length = fields.Float(load_from='SHAPE_Length'.lower(), dump_to='shape_length')
    shape_area = fields.String(load_from='SHAPE_Area'.lower(), dump_to='shape_area')

    class Meta:
        ordered = True

#    @pre_load
#    def fix_datetimes(self, data):
#        for f in ['created_date', 'last_edited_date']:
#            if data[f] not in ['', 'NA', None]:
#                data[f] = parser.parse(data[f]).isoformat()

seeds.append({
        'arcgis_dataset_title': 'Wards',
        'base_job_code': 'wards',
        'package_id': '766bbec2-e744-408e-9c8c-a58b662b6007', # Production package ID for Wards
        'schema': WardsSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])
#####
class GreenwaysSchema(pl.BaseSchema):
    _fid = fields.String(load_from='\ufeffFID'.lower(), dump_to='fid')
    objectid = fields.String(load_from='objectid'.lower(), dump_to='objectid', allow_none=True)
    area = fields.Float(load_from='area'.lower(), dump_to='area')
    perimeter = fields.Float(load_from='perimeter'.lower(), dump_to='perimeter')
    greenway_ = fields.Integer(load_from='greenway_'.lower(), dump_to='greenway', allow_none=True)
    greenway_i = fields.Integer(load_from='greenway_i'.lower(), dump_to='greenway_i', allow_none=True)
    name = fields.String(load_from='name'.lower(), dump_to='name')
    acres = fields.Float(load_from='acres'.lower(), dump_to='acres')
    sqmiles = fields.Float(load_from='sqmiles'.lower(), dump_to='sqmiles')
    nhood = fields.String(load_from='nhood'.lower(), dump_to='nhood', allow_none=True)
    dpwdiv = fields.Integer(load_from='dpwdiv'.lower(), dump_to='dpwdiv', allow_none=True)
    shape_leng = fields.Float(load_from='shape_leng'.lower(), dump_to='shape_leng', allow_none=True)
    date_resol = fields.String(load_from='date_resol'.lower(), dump_to='date_resol')
    label = fields.String(load_from='label'.lower(), dump_to='label')
    shape_length = fields.Float(load_from='SHAPE_Length'.lower(), dump_to='shape_length')
    shape_area = fields.Float(load_from='SHAPE_Area'.lower(), dump_to='shape_area')

    class Meta:
        ordered = True

    @pre_load
    def fix_datetimes(self, data):
        for f in ['date_resol']:
            if data[f] not in ['', 'NA', None]:
                data[f] = parser.parse(data[f]).isoformat()

seeds.append({
        'arcgis_dataset_title': 'Greenways',
        'base_job_code': 'greenways',
        'package_id': '8820c384-1424-45dd-a2bb-366a6a7c6d1b', # Production package ID for Greeways
        'schema': GreenwaysSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])

#####
class PlanningSectorsSchema(pl.BaseSchema):
    _objectid = fields.String(load_from='\ufeffOBJECTID'.lower(), dump_to='objectid')
    area = fields.Integer(load_from='area'.lower(), dump_to='area')
    perimeter = fields.String(load_from='perimeter'.lower(), dump_to='perimeter')
    plansect_ = fields.String(load_from='plansect_'.lower(), dump_to='plansect')
    plansect_i = fields.String(load_from='plansect_i'.lower(), dump_to='plansect_i')
    sectors = fields.Integer(load_from='sectors'.lower(), dump_to='sectors')
    label = fields.String(load_from='label'.lower(), dump_to='label')
    planner = fields.String(load_from='planner'.lower(), dump_to='planner')
    acres = fields.String(load_from='acres'.lower(), dump_to='acres', allow_none=True)
    shape__area = fields.Float(load_from='Shape__Area'.lower(), dump_to='shape_area')
    shape__length = fields.Float(load_from='Shape__Length'.lower(), dump_to='shape_length')

    class Meta:
        ordered = True

seeds.append({
        'arcgis_dataset_title': 'Planning Sectors',
        'base_job_code': 'planning',
        'package_id': 'd0611e28-edb1-4b3e-9147-60002942d27d', # Production package ID for Planning Sectors
        'schema': PlanningSectorsSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])

#####
class ZoningSchema(pl.BaseSchema):
    _objectid = fields.String(load_from='\ufeffobjectid'.lower(), dump_to='objectid')
    area = fields.Float(load_from='area'.lower(), dump_to='area')
    perimeter = fields.Float(load_from='perimeter'.lower(), dump_to='perimeter')
    zoning_ = fields.String(load_from='zoning_'.lower(), dump_to='zoning', allow_none=True)
    zoning_id = fields.String(load_from='zoning_id'.lower(), dump_to='zoning_id', allow_none=True)
    zon_new = fields.String(load_from='zon_new'.lower(), dump_to='zon_new')
    shape_leng = fields.String(load_from='shape_leng'.lower(), dump_to='shape_leng', allow_none=True)
    correctionlabel = fields.String(load_from='correctionlabel'.lower(), dump_to='correctionlabel', allow_none=True)
    full_zoning_type = fields.String(load_from='full_zoning_type'.lower(), dump_to='full_zoning_type')
    legendtype = fields.String(load_from='legendtype'.lower(), dump_to='legendtype')
    municode = fields.String(load_from='municode'.lower(), dump_to='municode', allow_none=True)
    status = fields.String(load_from='status'.lower(), dump_to='status')
    created_user = fields.String(load_from='created_user'.lower(), dump_to='created_user')
    created_date = fields.String(load_from='created_date'.lower(), dump_to='created_date')
    last_edited_user = fields.String(load_from='last_edited_user'.lower(), dump_to='last_edited_user')
    last_edited_date = fields.String(load_from='last_edited_date'.lower(), dump_to='last_edited_date')
    shape__area = fields.Float(load_from='Shape__Area'.lower(), dump_to='shape_area')
    shape__length = fields.Float(load_from='Shape__Length'.lower(), dump_to='shape_length')

    class Meta:
        ordered = True

    @pre_load
    def fix_datetimes(self, data):
        for f in ['created_date', 'last_edited_date']:
            if data[f] not in ['', 'NA', None]:
                data[f] = parser.parse(data[f]).isoformat()

seeds.append({
        'arcgis_dataset_title': 'Zoning',
        'base_job_code': 'zoning',
        'package_id': '01773197-baba-4f5e-aa77-ae87a04afafc', # Production package ID for Zoning
        'schema': ZoningSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])
#####
class HistoricSitesSchema(pl.BaseSchema):
    _objectid = fields.String(load_from='\ufeffobjectid'.lower(), dump_to='objectid')
    historic_i = fields.String(load_from='historic_i'.lower(), dump_to='historic_i')
    lotblock = fields.String(load_from='lotblock'.lower(), dump_to='lotblock')
    street = fields.String(load_from='street'.lower(), dump_to='street')
    address = fields.String(load_from='address'.lower(), dump_to='address')
    shape_leng_1 = fields.Float(load_from='shape_leng_1'.lower(), dump_to='shape_leng_1', allow_none=True)
    shape_area_1 = fields.Float(load_from='shape_area_1'.lower(), dump_to='shape_area_1', allow_none=True)
    alternativ = fields.String(load_from='alternativ'.lower(), dump_to='alternativ', allow_none=True)
    name = fields.String(load_from='name'.lower(), dump_to='name')
    provided_a = fields.String(load_from='provided_a'.lower(), dump_to='provided_a', allow_none=True)
    shape__area = fields.Float(load_from='Shape__Area'.lower(), dump_to='shape_area')
    shape__length = fields.Float(load_from='Shape__Length'.lower(), dump_to='shape_length')

    class Meta:
        ordered = True

seeds.append({
        'arcgis_dataset_title': 'City Designated Individual Historic Sites',
        'base_job_code': 'historic_sites',
        'package_id': '2ee7c48c-6fcf-407a-a8ca-477bdcfffda7', # Production package ID for Historic Sites
        'schema': HistoricSitesSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])
#####
class CouncilDistricts2012Schema(pl.BaseSchema):
    _fid = fields.Integer(load_from='\ufeffFID'.lower(), dump_to='fid')
    council = fields.String(load_from='council'.lower(), dump_to='council')
    transparency = fields.Integer(load_from='transparency'.lower(), dump_to='transparency')
    created_user = fields.String(load_from='created_user'.lower(), dump_to='created_user')
    created_date = fields.DateTime(load_from='created_date'.lower(), dump_to='created_date')
    last_edited_user = fields.String(load_from='last_edited_user'.lower(), dump_to='last_edited_user')
    last_edited_date = fields.DateTime(load_from='last_edited_date'.lower(), dump_to='last_edited_date')
    council_district = fields.String(load_from='council_district'.lower(), dump_to='council_district')
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
        'arcgis_dataset_title': 'City Council Districts 2012',
        'base_job_code': 'council_districts_2012',
        'package_id': '8249c8b6-37c6-4849-abe7-c9abbcdf6197', # Production package ID for City Council Districts 2012
        'schema': CouncilDistricts2012Schema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])
#####
class FireZonesSchema(pl.BaseSchema):
    _fid = fields.String(load_from='\ufeffFID'.lower(), dump_to='fid')
    area = fields.String(load_from='area'.lower(), dump_to='area', allow_none=True)
    perimeter = fields.Float(load_from='perimeter'.lower(), dump_to='perimeter', allow_none=True)
    firezones_ = fields.String(load_from='firezones_'.lower(), dump_to='firezones_', allow_none=True)
    firezones_id = fields.String(load_from='firezones_id'.lower(), dump_to='firezones_id', allow_none=True)
    dist_zone = fields.String(load_from='dist_zone'.lower(), dump_to='dist_zone')
    olddist_zone = fields.String(load_from='olddist_zone'.lower(), dump_to='olddist_zone', allow_none=True)
    mapbook = fields.String(load_from='mapbook'.lower(), dump_to='mapbook', allow_none=True)
    pagerotate = fields.Integer(load_from='pagerotate'.lower(), dump_to='pagerotate', allow_none=True)
    firezones = fields.String(load_from='firezones'.lower(), dump_to='firezones', allow_none=True)
    shape_length = fields.Float(load_from='SHAPE_Length'.lower(), dump_to='shape_length')
    shape_area = fields.Float(load_from='SHAPE_Area'.lower(), dump_to='shape_area')

    class Meta:
        ordered = True

seeds.append({
        'arcgis_dataset_title': 'Fire Zones',
        'base_job_code': 'fire_zones',
        'package_id': 'fd2f4880-e245-4c46-8071-5a364efa0abf', # Production package ID for Fire Zones
        'schema': FireZonesSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])
#####
class DPWDivisions2018to2020Schema(pl.BaseSchema):
    _objectid = fields.String(load_from='\ufeffobjectid'.lower(), dump_to='objectid')
    perimeter = fields.Float(load_from='perimeter'.lower(), dump_to='perimeter')
    dpwdivs_ = fields.String(load_from='dpwdivs_'.lower(), dump_to='dpwdivs', allow_none=True)
    dpwdivs_id = fields.String(load_from='dpwdivs_id'.lower(), dump_to='dpwdivs_id', allow_none=True)
    division = fields.String(load_from='division'.lower(), dump_to='division')
    unique_id = fields.String(load_from='unique_id'.lower(), dump_to='unique_id')
    shape__area = fields.Float(load_from='Shape__Area'.lower(), dump_to='shape_area')
    shape__length = fields.Float(load_from='Shape__Length'.lower(), dump_to='shape_length')

    class Meta:
        ordered = True

seeds.append({
        'arcgis_dataset_title': '2018 to 2020 DPW Divisions',
        'base_job_code': 'dpw_2018',
        'package_id': '4a86cee6-033f-41d2-bc56-6a9480819117', # Production package ID for 2018-2020 DPW Divisions
        'schema': DPWDivisions2018to2020Schema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])
#####
class UnderminedAreasSchema(pl.BaseSchema):
    _fid = fields.String(load_from='\ufeffFID'.lower(), dump_to='fid')
    und_aa_field = fields.String(load_from='und_aa_field'.lower(), dump_to='und_aa_field')
    created_user = fields.String(load_from='created_user'.lower(), dump_to='created_user')
    created_date = fields.DateTime(load_from='created_date'.lower(), dump_to='created_date')
    last_edited_user = fields.String(load_from='last_edited_user'.lower(), dump_to='last_edited_user')
    last_edited_date = fields.DateTime(load_from='last_edited_date'.lower(), dump_to='last_edited_date')
    undermined = fields.String(load_from='undermined'.lower(), dump_to='undermined')
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
        'arcgis_dataset_title': 'Undermined Areas',
        'base_job_code': 'undermined',
        'package_id': 'ea849f53-0aa9-4621-b9fb-e8dc323d3a9e', # Production package ID for Undermined Areas
        'schema': UnderminedAreasSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])
####(This one is completely new.)
class DPWEnvironmentalServicesDivisionsSchema(pl.BaseSchema):
    _objectid = fields.String(load_from='\ufeffobjectid'.lower(), dump_to='objectid')
    area = fields.Integer(load_from='area'.lower(), dump_to='area')
    perimeter = fields.Float(load_from='perimeter'.lower(), dump_to='perimeter')
    env_serv_ = fields.String(load_from='env_serv_'.lower(), dump_to='env_serv')
    env_serv_i = fields.String(load_from='env_serv_i'.lower(), dump_to='env_serv_i')
    acreage = fields.Integer(load_from='acreage'.lower(), dump_to='acreage')
    sq_miles = fields.Float(load_from='sq_miles'.lower(), dump_to='sq_miles')
    division = fields.String(load_from='division'.lower(), dump_to='division')
    unique_id = fields.String(load_from='unique_id'.lower(), dump_to='unique_id')
    shape__area = fields.Float(load_from='Shape__Area'.lower(), dump_to='shape_area')
    shape__length = fields.Float(load_from='Shape__Length'.lower(), dump_to='shape_length')

    class Meta:
        ordered = True

seeds.append({
        'arcgis_dataset_title': 'DPW Environmental Services Divisions',
        'base_job_code': 'dpw_divs',
        'package_id': 'f626afa5-bc88-44d2-8e1c-b586de9fe941', # Production package ID for DPW Environmental Services Divisions
        'schema': DPWEnvironmentalServicesDivisionsSchema,
        'new_wave_format': True
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])
#####
class SlopesSchema(pl.BaseSchema):
    _objectid_1 = fields.String(load_from='\ufeffobjectid_1'.lower(), dump_to='objectid_1')
    objectid = fields.String(load_from='objectid'.lower(), dump_to='objectid')
    created_us = fields.String(load_from='created_us'.lower(), dump_to='created_user')
    created_da = fields.DateTime(load_from='created_da'.lower(), dump_to='created_date')
    last_edite = fields.String(load_from='last_edite'.lower(), dump_to='last_edited_user')
    last_edi_1 = fields.DateTime(load_from='last_edi_1'.lower(), dump_to='last_edited_date')
    slope25 = fields.String(load_from='slope25'.lower(), dump_to='slope25')
    shape__are = fields.Float(load_from='shape__are'.lower(), dump_to='shape_are')
    shape__len = fields.Float(load_from='shape__len'.lower(), dump_to='shape_len')
    shape__area = fields.Float(load_from='Shape__Area'.lower(), dump_to='shape_area')
    shape__length = fields.Float(load_from='Shape__Length'.lower(), dump_to='shape_length')

    class Meta:
        ordered = True

    @pre_load
    def fix_datetimes(self, data):
        for f in ['created_da', 'last_edi_1']:
            if data[f] not in ['', 'NA', None]:
                data[f] = parser.parse(data[f]).isoformat()

seeds.append({
        'arcgis_dataset_title': '25% or Greater Slope',
        'base_job_code': 'slopes',
        'package_id': '0f643c56-1c53-4c88-824d-3a3876c0d3a0', # Production package ID for 25% or Greater Slope
        'schema': SlopesSchema,
        'new_wave_format': False
        })

job_dicts += standard_arcgis_job_dicts(data_json_url, data_json_content, **seeds[-1])
##########3
# DONE
#Dataset         Note      Service URL
# Pittsburgh Steps (Removed)
#               Removed from HUB, service no longer exists, Republishing this as a new service with same data, see one line below
#                        http://maps.pittsburghpa.gov/arcgis/rest/services/OpenData/PGHODSteps/FeatureServer
# Pittsburgh Steps (Created)
#               New Service Created to replace the broken Steps Service
#                        https://services1.arcgis.com/YZCmUqbcsUpOKfj7/arcgis/rest/services/PittsburghSteps/FeatureServer
# Pittsburgh Landslide Prone Areas (Removed)
#                Service No longer exists, removed it from our HUB page since there were 2 copies of Landslide data there and the other service still works
#                        http://maps.pittsburghpa.gov/arcgis/rest/services/OpenData/PGHODLandslideProne/FeatureServer
# Pittsburgh City Facilities (Removed)
#               Removed from HUB, service no longer exists, Facilities data available on WPRDC through other sources
#                        http://maps.pittsburghpa.gov//arcgis/rest/services/OpenData/PGHODCityFacilities/FeatureServer
# Pittsburgh Traffic Signals (Removed)
#               Removed from HUB, service no longer exists, Traffic Signals data available on WPRDC through other sources
#                        http://maps.pittsburghpa.gov/arcgis/rest/services/OpenData/PGHODTrafficSignals/FeatureServer
# Pittsburgh City Trees (Removed)
#                Removed from HUB, service no longer exists, City tree data available on WPRDC through other sources
#                        http://maps.pittsburghpa.gov//arcgis/rest/services/OpenData/PGHODCityTrees/FeatureServer
# Pittsburgh Parks (Removed)
#                Service Still exists, but removed it from our HUB page since there were 2 copies of Parks data there
#                        https://services1.arcgis.com/YZCmUqbcsUpOKfj7/arcgis/rest/services/PGHWebParks/FeatureServer
# Pittsburgh Wards (Removed)
#               Service Still exists, but removed it from our HUB page since there were 2 copies of Wards data there
#                       https://services1.arcgis.com/YZCmUqbcsUpOKfj7/arcgis/rest/services/PGHWards/FeatureServer
# Pittsburgh Greenways (Removed)
#               Service Still exists, but removed it from our HUB page since there were 2 copies of Greenways data there
#                       https://services1.arcgis.com/YZCmUqbcsUpOKfj7/arcgis/rest/services/PGHODGreenways/FeatureServer
# Pittsburgh City Council Districts 2012 (Removed)
#               Service Still exists, but removed it from our HUB page since there were 2 copies of Council Districts data there
#                       https://services1.arcgis.com/YZCmUqbcsUpOKfj7/arcgis/rest/services/PGH_CityCouncilOD/FeatureServer
# Pittsburgh Fire Zones (Removed)
#                Service No longer exists, removed it from our HUB page since there were 2 copies of Fire Zones data there and the other service still works
#                        http://maps.pittsburghpa.gov/arcgis/rest/services/OpenData/PGHODFireZones/FeatureServer
# Pittsburgh DPW Divisions (Title Change)
#                Changed Title to "2018 to 2020 DPW Divisions" - This data set is not current, changed title to reflect that
#                        https://services1.arcgis.com/YZCmUqbcsUpOKfj7/arcgis/rest/services/PGH_DPWDivisions/FeatureServer
# Pittsburgh Undermined Areas (Removed)
#                Service No longer exists, removed it from our HUB page since there were 2 copies of Undermined data there and the other service still works
#                        http://maps.pittsburghpa.gov//arcgis/rest/services/OpenData/PGHODUndermined/FeatureServer
# Pittsburgh DPW Environmental Services Divisions (Removed)
#                Removed from HUB, service no longer exists, Republishing this as a new service with same data, see one line below
#                       http://maps.pittsburghpa.gov//arcgis/rest/services/OpenData/PGHODDPWESDivisions/FeatureServer
# DPW Environmental Services Divisions (Created)
#                New Service Created to replace the broken ES Division Service
#                        https://services1.arcgis.com/YZCmUqbcsUpOKfj7/arcgis/rest/services/DPWEnvironmentalServicesDivisions/FeatureServer


####
# Also addressed
# City Designated Historic Districts
# Planning Sectors
# City Designated Individual Historic Sites
# Zoning
# Slopes
# Undermined Areas

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
