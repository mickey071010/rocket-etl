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

import os, calendar

class AssessmentSchema(pl.BaseSchema):
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
    _class = fields.String(dump_to="CLASS", allow_none=True)
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

# [ ] Are these fields like even being used at all?
    municipality = fields.String(allow_none=True)
    neighborhood = fields.String(allow_none=True)
    pgh_council_district = fields.String(allow_none=True)
    pgh_ward = fields.String(allow_none=True)
    pgh_public_works_division = fields.String(allow_none=True)
    pgh_police_zone = fields.String(allow_none=True)
    pgh_fire_zone = fields.String(allow_none=True)
    tract = fields.String(allow_none=True)
    block_group = fields.String(allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_dates(self, data):
        # print('fixing dates')
        if data['asofdate']:
            data['asofdate'] = datetime.strptime(
                data['asofdate'], "%d-%b-%y").date().isoformat()


def name_file_resource(resource_type=None):
    # Get name of month and year for previous month.
    now = datetime.now()
    last_month_num = (now.month - 1) % 12
    year = now.year
    if last_month_num == 0:
        last_month_num = 12
    if last_month_num == 12: # If going back to December,
        year -= 1            # set year to last year
    last_month_name = calendar.month_abbr[last_month_num].upper()

    file_title = f"{last_month_name}-{year} Property Assessments Parcel Data"
    return file_title


assessments_package_id = '2b3df818-601e-4f06-b150-643557229491' # Production version of assessments package
assessments_package_id = "812527ad-befc-4214-a4d3-e621d8230563" # Test package on data.wprdc.org

def custom_processing(job, **kwparameters):
    ## BEGIN CUSTOMIZABLE SECTION ##
    # Use local files on the second of the two jobs to avoid redownloading that giant file.
    #if 'destinations' in job and len(job['destinations']) == 1 and job['destinations'][0] == 'ckan_filestore':
    #    use_local_files = True
    ## Actually, this will be handled by combining all three of the original jobs (each with a different destination)
    ## into one with one source and a list of destinations.

    # The original assessments.py script has capitalize = True, like this:
    #      fields=AssessmentSchema().serialize_to_ckan_fields(capitalize=True),
    # and also those like six uncapitalized add-on reverse geocoded fields in the schema, which I don't see in the downloaded data table...

    # Under the old ETL job:
    # The file uploaded to the filestore is uploaded with the intention of it not being queryable, but there is a datastore for this resource.
    # This is the reason that the resource ID is used to target the upload.
    # The resource name will be something like "AUG-2019 Property Assessments Parcel Data" but the description is already there as
    # "For downloads."

    # Right below that resource is one named "Property Assessments Parcel Data" with the description
    # "For API Calls -- This is the most recent collection of Assessment Data, but made available to API calls. To download a copy of the data go to the previous month's dataset. It contains all the same data, but is downloadable."
    # The URL is automatically set to "URL: https://data.wprdc.org/datastore/dump/518b583f-7cc8-4f60-94d0-174cc98310dc", but probably those downloads will time out.


    # How should it work in the future?
    # 1) A) Test the downstream solution to eliminate the separate file upload.
    #    B) If that fails, consider compressing the CSV before uploading.

    # 2) Also we need to maintain a running archive of files... so maybe just uploading a compressed file with all files from that year would be best (2019-assessments.zip),
    # with the implementation being keeping a running archive on the office computer? A location-agnostic process would be better, but that's to work on in the future.
    # {Is storing those files in the cloud really any better? Maybe the best option is actually downloading the running ZIP archive from CKAN to a place that is known
    # to have sufficient disk space, uncompressing, adding the new file, and rezipping (or maybe just adding the final to the zipped file if there are programs
    # that allow this) and then reuploading the updated file.}

    print("Have you accounted for 1) capitalize = True, 2) those reverse-geocoded fields, 3) the fact that the original script uploaded the file to a resource ID rather than a resource name, and 4) the fact that the CSV file was being renamed before uploading?")
    print("Also, how are we going to make a zipped archive out of these files?")
    print("Downloading the old zip file, uncompressing, adding the latest one, and recompressing?")
    print("That's a lot of work!")
    file_id = "f2b8d575-e256-4718-94ad-1e12239ddb92"

    ## END CUSTOMIZABLE SECTION ##


job_dicts = [
    {
        'job_code': 'assessments_filestore'
        'source_type': 'sftp',
        'source_dir': 'property_assessments',
        'source_file': 'ALLEGHENY_COUNTY_MASTER_FILE.csv',
        'encoding': 'latin-1', # Taken from assessments.py and used instead of the default, 'utf-8'.
        'connector_config_string': 'sftp.county_sftp', # This is just used to look up parameters in the settings.json file.
        'schema': None,
        'always_wipe_data': True,
        'destinations': ['ckan_filestore'],
        'destination_file': 'assessments.csv',
        'package': assessments_archive_package_id,
        'resource_name': name_file_resource()
    },
    # Instead of all the LMAZ stuff, just save the file to a secret archive package.
    {
        'job_code': 'assessments_archive'
        'source_type': 'sftp',
        'source_dir': 'property_assessments',
        'source_file': 'ALLEGHENY_COUNTY_MASTER_FILE.csv',
        'encoding': 'latin-1', # Taken from assessments.py and used instead of the default, 'utf-8'.
        'connector_config_string': 'sftp.county_sftp', # This is just used to look up parameters in the settings.json file.
        'schema': None,
        'always_wipe_data': True,
        'destinations': ['ckan_filestore'],
        'destination_file': 'assessments.csv',
        'package': assessments_package_id,
        'resource_name': name_file_resource()
    },
    {
        'job_code': 'assessments_datastore'
        'source_type': 'sftp',
        'source_dir': 'property_assessments',
        'source_file': 'ALLEGHENY_COUNTY_MASTER_FILE.csv',
        'encoding': 'latin-1', # Taken from assessments.py and used instead of the default, 'utf-8'.
        'connector_config_string': 'sftp.county_sftp',
        'schema': AssessmentSchema,
        'primary_key_fields': ['PARID'],
        'upload_method': 'upsert',
        'always_wipe_data': True,
        'package': assessments_package_id,
        'resource_name': 'Property Assessments Parcel Data',
    },
]
