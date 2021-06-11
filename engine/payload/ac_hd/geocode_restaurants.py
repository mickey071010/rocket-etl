import csv, json, requests, sys, traceback
import time
import re
from dateutil import parser

from marshmallow import fields, pre_load, pre_dump
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import scientific_notation_to_integer
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def correct_address(address_str):
    translations = {}
    # Street-name corrections
    translations['ROBINSON CENTER'] = 'ROBINSON CENTRE'
    translations['Robinson Center'] = 'Robinson Centre'
    translations['Davision'] = 'Division'
    translations['Third Ave'] = '3rd Ave'
    translations['Wm '] = 'William '

    # City-name corrections:
    translations['Mc Keesport'] = 'McKeesport' # While Mc Keesport is apparently the standard version, the property assessments file has 55 instances of McKeesport.

    proposed_corrections = []
    maximally_translated = str(address_str)
    for before,after in translations.items():
        if before in address_str:
            proposed_corrections.append(re.sub(before,after,address_str))
            maximally_translated = re.sub(before,after,maximally_translated)

    ic(proposed_corrections)
    ic(maximally_translated)

    if maximally_translated != address_str and maximally_translated not in proposed_corrections:
        proposed_corrections = [maximally_translated] + proposed_corrections

    return list(set(proposed_corrections))

def geocode_address_string(address):
    address = re.sub("\s\s+", " ", address)
    url = "https://tools.wprdc.org/geo/geocode?addr={}".format(address)
    r = requests.get(url)
    result = r.json()
    time.sleep(0.1)
    if result['data']['status'] == "OK" or result['data']['status'][:9] == "There are":
        # Another possible status is something like
        # "There are 2 parcels that contain the point (-79.9300408497384, 40.4382315401933)."
        longitude, latitude = result['data']['geom']['coordinates']
        return longitude, latitude
    print("Unable to geocode {}, failing with status code {}.".format(address,result['data']['status']))
    return None, None

#def geocode_address_by_parts():
#     number = request.GET['number']
#    directional = request.GET.get('directional', None)
#    street_name = request.GET['street_name']
#    street_type = request.GET['street_type']
#    city = request.GET['city']
#    state = request.GET.get('state', None)
#    zip_code = request.GET.get('zip_code', None)

class RestaurantsSchema(pl.BaseSchema):
    id = fields.String()
    #storeid = fields.String(load_from='StoreID', dump_to='store_id')
    facility_name = fields.String()
    num = fields.String(allow_none=True) # Non-integer values include "8011-B".
    street = fields.String(allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip = fields.String(allow_none=True)
    municipal = fields.String(allow_none=True)
    category_cd = fields.String(allow_none=True)
    description = fields.String(allow_none=True)
    p_code = fields.String(allow_none=True)
    fdo = fields.Date(allow_none=True) # Here I did add allow_none=True because
    # there's no other good way of dealing with empty date fields (though
    # I fear that the earlier data dump coerced it all to 1984-06-17).

    bus_st_date = fields.Date(allow_none=True)
    bus_cl_date = fields.Date(allow_none=True)
    noseat = fields.Integer(dump_to="seat_count", allow_none=True) # allow_none=True was
    # added for the October 2019 manual extract (since None values occur there but not
    # in the archive or the FTPed files (apparently)) for this and other fields.
    noroom = fields.Integer(allow_none=True)
    sqfeet = fields.Integer(dump_to="sq_feet", allow_none=True)
    status = fields.String()
    placard_st = fields.String(allow_none=True)
    x = fields.Float(allow_none=True)
    y = fields.Float(allow_none=True)
    address = fields.String()

    class Meta:
        ordered = True

#    @pre_load
#    def geocode(self,data):
#        if 'num' in data and 'street' in data and 'city' in data:
#            num = data['num']
#            street = data['street']
#            city = data['city']
#            state = None
#            zip_code = None
#            directional = None
#            if 'state' in data:
#                state = data['state']
#            if 'zip' in data:
#                zip_code = data['zip'] # This line has been corrected since the last unsuccessful attempt.
#            longitude, latitude = geocode_address_by_parts(num, directional, street, city, state, zip_code)
#
#            if longitude is None:
#                streets = correct_address(street)
#                if len(streets) > 0:
#                    longitude, latitude = geocode_address_by_parts(num, directional, street, city, state, zip_code)
#            data['x'] = longitude
#            data['y'] = latitude

    @pre_load
    def geocode(self,data):
        if 'x' in data and data['x'] in ['', 'NA']: # Only geocode if necessary
            if 'address' in data:
                address_string = data['address']
                longitude, latitude = geocode_address_string(address_string)
                if longitude is None:
                    corrected_addresses = correct_address(address_string)
                    if len(corrected_addresses) > 0:
                        # For now just try the first of the proposed corrections:
                        longitude, latitude = geocode_address_string(corrected_addresses[0])
                data['x'] = longitude
                data['y'] = latitude

    @pre_load
    def convert_dates(self,data):
        date_fields = ['fdo', 'bus_st_date', 'bus_cl_date']
        for field in date_fields:
            if data[field] not in [None, '', 'NA']:
                data[field] = parser.parse(data[field]).date().isoformat()
            elif data[field] in ['', 'NA']:
                data[field] = None

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k in ['noroom', 'noseat', 'p_code', 'num', 'street', 'city', 'state', 'zip', 'municipal', 'category_cd', 'description', 'placard_st']:
                if v in ['NA']:
                    data[k] = None

    @pre_load
    def fix_na_or_coerce_to_integer(self, data):
        # Handle 'sqfeet' field here so the lack of defined order between
        # fix_nas and this function does not cause any problems.

        # Note that using the decimal module in scientific_notation_to_integer
        # will be slower than using float, and checking this value on every
        # single field will add a bit of time to the processing.
        for k, v in data.items():
            if k in ['sqfeet']:
                if v in ['NA', '', None]:
                    data[k] = None
                else:
                    try:
                        int(v)
                    except ValueError:
                        data[k] = scientific_notation_to_integer(v)
                        # scientific_notation_to_integer will throw
                        # an exception if v is not a null, an integer,
                        # or a value in scientific notation (which
                        # could be a float rather than an integer).

restaurants_package_id = "8744b4f6-5525-49be-9054-401a2c4c2fac" # restaurants package, production

job_dicts = [
    {
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': 'locations-for-geocode.csv',
        'encoding': 'latin-1',
        'connector_config_string': 'sftp.county_sftp',
        'schema': RestaurantsSchema,
        'primary_key_fields': ['id'],
        'upload_method': 'upsert',
        'package': restaurants_package_id,
        'resource_name': 'Geocoded Food Facilities',
    },
]
