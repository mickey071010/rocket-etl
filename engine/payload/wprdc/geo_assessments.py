import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint
import re

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.parameters.remote_parameters import TEST_PACKAGE_ID

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class GeoAssessments(pl.BaseSchema):
    parid = fields.String(dump_to="PARID", allow_none=True)
    propertyhousenum = fields.String(
        dump_to="PROPERTYHOUSENUM", allow_none=True)
    propertyfraction = fields.String(
        dump_to="PROPERTYFRACTION", allow_none=True)
    propertyaddress = fields.String(dump_to="PROPERTYADDRESS", allow_none=True)
    propertycity = fields.String(dump_to="PROPERTYCITY", allow_none=True)
    propertystate = fields.String(dump_to="PROPERTYSTATE", allow_none=True)
    propertyunit = fields.String(dump_to="PROPERTYUNIT", allow_none=True)
    propertyzip = fields.String(dump_to="PROPERTYZIP", allow_none=True)
    municode = fields.String(dump_to="MUNICODE", allow_none=True)
    munidesc = fields.String(dump_to="MUNIDESC", allow_none=True)
    schoolcode = fields.String(dump_to="SCHOOLCODE", allow_none=True)
    schooldesc = fields.String(dump_to="SCHOOLDESC", allow_none=True)
    legal1 = fields.String(dump_to="LEGAL1", allow_none=True)
    legal2 = fields.String(dump_to="LEGAL2", allow_none=True)
    legal3 = fields.String(dump_to="LEGAL3", allow_none=True)
    neighcode = fields.String(dump_to="NEIGHCODE", allow_none=True)
    neighdesc = fields.String(dump_to="NEIGHDESC", allow_none=True)
    taxcode = fields.String(dump_to="TAXCODE", allow_none=True)
    taxdesc = fields.String(dump_to="TAXDESC", allow_none=True)
    taxsubcode = fields.String(dump_to="TAXSUBCODE", allow_none=True)
    taxsubcode_desc = fields.String(dump_to="TAXSUBCODE_DESC", allow_none=True)
    ownercode = fields.String(dump_to="OWNERCODE", allow_none=True)
    ownerdesc = fields.String(dump_to="OWNERDESC", allow_none=True)
    _class = fields.String(dump_to="CLASS", load_from='class', allow_none=True)
    classdesc = fields.String(dump_to="CLASSDESC", allow_none=True)
    usecode = fields.String(dump_to="USECODE", allow_none=True)
    usedesc = fields.String(dump_to="USEDESC", allow_none=True)
    lotarea = fields.Float(dump_to="LOTAREA", allow_none=True)
    homesteadflag = fields.String(dump_to="HOMESTEADFLAG", allow_none=True)
    cleangreen = fields.String(dump_to="CLEANGREEN", allow_none=True)
    farmsteadflag = fields.String(dump_to="FARMSTEADFLAG", allow_none=True)
    abatementflag = fields.String(dump_to="ABATEMENTFLAG", allow_none=True)
    recorddate = fields.String(dump_to="RECORDDATE", allow_none=True)
    saledate = fields.String(dump_to="SALEDATE", allow_none=True)
    saleprice = fields.Float(dump_to="SALEPRICE", allow_none=True)
    salecode = fields.String(dump_to="SALECODE", allow_none=True)
    saledesc = fields.String(dump_to="SALEDESC", allow_none=True)
    deedbook = fields.String(dump_to="DEEDBOOK", allow_none=True)
    deedpage = fields.String(dump_to="DEEDPAGE", allow_none=True)
    prevsaledate = fields.String(dump_to="PREVSALEDATE", allow_none=True)
    prevsaleprice = fields.Float(dump_to="PREVSALEPRICE", allow_none=True)
    prevsaledate2 = fields.String(dump_to="PREVSALEDATE2", allow_none=True)
    prevsaleprice2 = fields.Float(dump_to="PREVSALEPRICE2", allow_none=True)
    changenoticeaddress1 = fields.String(
        dump_to="CHANGENOTICEADDRESS1", allow_none=True)
    changenoticeaddress2 = fields.String(
        dump_to="CHANGENOTICEADDRESS2", allow_none=True)
    changenoticeaddress3 = fields.String(
        dump_to="CHANGENOTICEADDRESS3", allow_none=True)
    changenoticeaddress4 = fields.String(
        dump_to="CHANGENOTICEADDRESS4", allow_none=True)
    countybuilding = fields.Float(dump_to="COUNTYBUILDING", allow_none=True)
    countyland = fields.Float(dump_to="COUNTYLAND", allow_none=True)
    countytotal = fields.Float(dump_to="COUNTYTOTAL", allow_none=True)
    countyexemptbldg = fields.Float(
        dump_to="COUNTYEXEMPTBLDG", allow_none=True)
    localbuilding = fields.Float(dump_to="LOCALBUILDING", allow_none=True)
    localland = fields.Float(dump_to="LOCALLAND", allow_none=True)
    localtotal = fields.Float(dump_to="LOCALTOTAL", allow_none=True)
    fairmarketbuilding = fields.Float(
        dump_to="FAIRMARKETBUILDING", allow_none=True)
    fairmarketland = fields.Float(dump_to="FAIRMARKETLAND", allow_none=True)
    fairmarkettotal = fields.Float(dump_to="FAIRMARKETTOTAL", allow_none=True)
    style = fields.String(dump_to="STYLE", allow_none=True)
    styledesc = fields.String(dump_to="STYLEDESC", allow_none=True)
    stories = fields.String(dump_to="STORIES", allow_none=True)
    yearblt = fields.Float(dump_to="YEARBLT", allow_none=True)
    exteriorfinish = fields.String(dump_to="EXTERIORFINISH", allow_none=True)
    extfinish_desc = fields.String(dump_to="EXTFINISH_DESC", allow_none=True)
    roof = fields.String(dump_to="ROOF", allow_none=True)
    roofdesc = fields.String(dump_to="ROOFDESC", allow_none=True)
    basement = fields.String(dump_to="BASEMENT", allow_none=True)
    basementdesc = fields.String(dump_to="BASEMENTDESC", allow_none=True)
    grade = fields.String(dump_to="GRADE", allow_none=True)
    gradedesc = fields.String(dump_to="GRADEDESC", allow_none=True)
    condition = fields.String(dump_to="CONDITION", allow_none=True)
    conditiondesc = fields.String(dump_to="CONDITIONDESC", allow_none=True)
    cdu = fields.String(dump_to="CDU", allow_none=True)
    cdudesc = fields.String(dump_to="CDUDESC", allow_none=True)
    totalrooms = fields.Float(dump_to="TOTALROOMS", allow_none=True)
    bedrooms = fields.Float(dump_to="BEDROOMS", allow_none=True)
    fullbaths = fields.Float(dump_to="FULLBATHS", allow_none=True)
    halfbaths = fields.Float(dump_to="HALFBATHS", allow_none=True)
    heatingcooling = fields.String(dump_to="HEATINGCOOLING", allow_none=True)
    heatingcoolingdesc = fields.String(
        dump_to="HEATINGCOOLINGDESC", allow_none=True)
    fireplaces = fields.Float(dump_to="FIREPLACES", allow_none=True)
    bsmtgarage = fields.String(dump_to="BSMTGARAGE", allow_none=True)
    finishedlivingarea = fields.Float(
        dump_to="FINISHEDLIVINGAREA", allow_none=True)
    cardnumber = fields.Float(dump_to="CARDNUMBER", allow_none=True)
    alt_id = fields.String(dump_to="ALT_ID", allow_none=True)
    taxyear = fields.Float(dump_to="TAXYEAR", allow_none=True)
    asofdate = fields.Date(dump_to="ASOFDATE", allow_none=True)

    # municipality = fields.String(dump_to="MUNICIPALITY", allow_none=True)
    # neighborhood = fields.String(dump_to="NEIGHBORHOOD", allow_none=True)
    # pgh_council_district = fields.String(dump_to="PGH_COUNCIL_DISTRICT",
    #                                      allow_none=True)
    # pgh_ward = fields.String(dump_to="PGH_WARD", allow_none=True)
    # pgh_public_works_division = fields.String(
    #     dump_to="PGH_PUBLIC_WORKS_DIVISION", allow_none=True)
    # pgh_police_zone = fields.String(dump_to="PGH_POLICE_ZONE", allow_none=True)
    # pgh_fire_zone = fields.String(dump_to="PGH_FIRE_ZONE", allow_none=True)
    # tract = fields.String(dump_to="TRACT", allow_none=True)
    # block_group = fields.String(dump_to="BLOCK_GROUP", allow_none=True)

    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_dates(self, data):
        if data['asofdate']:
            data['asofdate'] = datetime.strptime(
                data['asofdate'], "%d-%b-%y").date().isoformat()

    @pre_load
    def clear_whitespace(self, data):
        items = ['changenoticeaddress1', 'changenoticeaddress2', 'changenoticeaddress3', 'changenoticeaddress4',
                 'legal1', 'legal2', 'legal3']

        for item in items:
            if data[item] is not None:
                data[item] = re.sub(r'\s+', ' ', data[item])



geo_property_package_id = "6102a454-e7af-45c3-9d5a-e79e65a36a12" # Production version of Property Data with Geographic Identifiers 

job_dicts = [
    {
        'job_code': 'geoassessments',
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'geocoded_assessments.csv',
        'encoding': 'latin-1',
        'schema': GeoAssessments,
        'primary_key_fields': ['PARID'],
        'always_wipe_data': False, # Should this be True?
        'upload_method': 'upsert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'air_daily.csv', # purposes.
        'package': geo_property_package_id,
        'resource_name': 'Property Assessment Data with Parcel Centroids',
    },
]
