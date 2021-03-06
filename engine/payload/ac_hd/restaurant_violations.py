import os, csv, json, requests, sys, traceback
import ckanapi
import time
import re
from datetime import datetime
from dateutil import parser

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import find_resource_id
from engine.notify import send_to_slack
from engine.credentials import site, API_key
from engine.parameters.remote_parameters import TEST_PACKAGE_ID

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def fix_encoding_errors(s):
    s = re.sub("Ã©", "é", s)
    some_mangled_apostrophe = b"\xc3\xa2\xc2\x80\xc2\x99".decode("utf-8")
    s = re.sub(some_mangled_apostrophe, "'", s)
    mangled_e_with_reverse_diacritic = b"\xc3\x83\xc2\xa8".decode("utf-8")
    s = re.sub(mangled_e_with_reverse_diacritic, "è", s)
    mangled_degree_symbol = b"\xc3\x82\xc2\xba".decode("utf-8")
    s = re.sub(mangled_degree_symbol, "°", s)

    some_mangled_separator = b"\xc3\xa2\xc2\x80\xc2\x93".decode("utf-8")
    # Found in the 'description' field between "Mobile" and "Tier I".
    s = re.sub(some_mangled_separator, "-", s)
    return s

class ViolationsSchema(pl.BaseSchema):
    encounter = fields.String(allow_none=True)
    id = fields.String(allow_none=True)
    placard_st = fields.String(allow_none=True)
    # placard_desc = fields.String(allow_none=True)
    facility_name = fields.String(allow_none=True)
    bus_st_date = fields.Date(allow_none=True)
    # category_cd = fields.String(allow_none=True)
    description = fields.String(allow_none=True)
    description_new = fields.String(allow_none=True)

    num = fields.String(allow_none=True)
    street = fields.String(allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip = fields.String(allow_none=True) # This was (for no obvious
    # reason) a Float in the predecessor of this ETL job.

    inspect_dt = fields.Date(allow_none=True)
    start_time = fields.Time(allow_none=True)
    end_time = fields.Time(allow_none=True)

    municipal = fields.String(allow_none=True)

    rating = fields.String(allow_none=True)
    low = fields.String(allow_none=True)
    medium = fields.String(allow_none=True)
    high = fields.String(allow_none=True)
    url = fields.String(allow_none=True)

    class Meta():
        ordered = True

    @pre_load
    def strip_strings(self, data):
        fields_to_recode = ['facility_name', 'description']
        for field in fields_to_recode:
            data[field] = fix_encoding_errors(data[field].strip())

        fields_to_strip = ['num']
        for field in fields_to_strip:
            if type(data[field]) == str:
                data[field] = fix_encoding_errors(data[field].strip())

    def fix_dates_times(self, data):
        if data['bus_st_date']:
            #data['bus_st_date'] = datetime.strptime(data['bus_st_date'], "%m/%d/%Y %H:%M").date().isoformat()
            data['bus_st_date'] = parser.parse(data['bus_st_date']).date().isoformat()

        if data['inspect_dt']:
            #data['inspect_dt'] = datetime.strptime(data['inspect_dt'], "%m/%d/%Y %H:%M").date().isoformat()
            data['inspect_dt'] = parser.parse(data['inspect_dt']).date().isoformat()

        if data['start_time']:
            data['start_time'] = datetime.strptime(data['start_time'], "%I:%M %p").time().isoformat()

        if data['end_time']:
            data['end_time'] = datetime.strptime(data['end_time'], "%I:%M %p").time().isoformat()

        to_string = ['encounter', 'id']
        for field in to_string:
            if type(data[field]) != str:
                data[field] = str(int(data[field]))

        # Because of some issues with Excel files, two string values in the first row are getting parsed as floats instead of strings: ('encounter', 201401020037.0), ('id', 201202030011.0)

def express_load_then_delete_file(job, **kwparameters):
    """The basic idea is that the job processes with a 'file' destination,
    so the ETL job loads the file into destination_file_path. Then as a
    custom post-processing step, that file is Express-Loaded. This is
    faster (particularly for large files) and avoids 504 errors and unneeded
    API requests."""
    # Eventually this function should be moved either to etl_util.py or
    # more likely the pipeline framework. In either case, this approach
    # can be formalized, either as a destination or upload method and
    # possibly implemented as a loader (CKANExpressLoader).
    if kwparameters['test_mode']:
        job.package = TEST_PACKAGE_ID
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    csv_file_path = job.destination_file_path
    resource_id = find_resource_id(job.package, job.resource_name)
    if resource_id is None:
        # If the resource does not already exist, create it.
        print(f"Unable to find a resource with name '{job.resource_name}' in package with ID {job.package}.")
        print(f"Creating new resource, and uploading CSV file {csv_file_path} to resource with name '{job.resource_name}' in package with ID {job.package}.")
        resource_as_dict = ckan.action.resource_create(package_id=job.package,
            name = job.resource_name,
            upload=open(csv_file_path, 'r'))
    else:
        print(f"Uploading CSV file {csv_file_path} to resource with name '{job.resource_name}' in package with ID {job.package}.")
        resource_as_dict = ckan.action.resource_patch(id = resource_id,
            upload=open(csv_file_path, 'r'))
        # Running resource_update once sets the file to the correct file and triggers some datastore action and
        # the Express Loader, but for some reason, it seems to be processing the old file.

        # So instead, let's run resource_patch (which just sets the file) and then run resource_update.
        #resource_as_dict = ckan.action.resource_update(id = resource_id)
        resource_as_dict = ckan.action.resource_update(id = resource_id,
            upload=open(csv_file_path, 'r'))

    print(f"Removing temp file at {csv_file_path}")
    os.remove(csv_file_path)

package_id = "8744b4f6-5525-49be-9054-401a2c4c2fac" # Production package for Allegheny County Restaurant/Food Facility Inspections

job_dicts = [
    {
        'job_code': 'restaurant_violations',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': 'alco-restaurant-violations.csv',
        'encoding': 'latin-1',
        'connector_config_string': 'sftp.county_sftp',
        'schema': ViolationsSchema,
        'always_wipe_data': True,
        'upload_method': 'insert',
        'destination': 'file',
        'destination_file': f'alco-restuarant-violations.csv',
        'package': package_id,
        'resource_name': "Food Facility/Restaurant Inspection Violations",
        'custom_post_processing': express_load_then_delete_file
    },
]
