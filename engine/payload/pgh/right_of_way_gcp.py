# This is the new version of the right-of-way ETL job that pulls
# data from Google Cloud Platform (under Project Data Rivers).

from marshmallow import fields, pre_load, post_load

import requests
from engine.wprdc_etl import pipeline as pl
from engine.credentials import site, API_key
from engine.parameters.local_parameters import SOURCE_DIR
from engine.etl_util import fetch_city_file, query_resource, Job
from engine.notify import send_to_slack

from dateutil import parser
from icecream import ic

print(f'There appear to be ID values in the source data that are not in the published data, such as DPW0700042 and DPW0504497. It may be that these are records without from_date and to_date values.')
print(f'There are a bunch of permits in the published data with types like "PublicWorks/Opening/NA/NA" that should be remapped to the corresponding standardized version.')
print(f'Should we just create a new version of the table without the extra stuff?')
print(f'For now, only publish records with open_date values after 2020-06-26.')
print(f'There are a small number of records that have the word "test" in both the description and the street_or_location field. Probably these could be purged, but it is hard to know exactly what regexes to use. Many have latitude/longitude and all have id values.')

def rev_geocode(lon, lat):
    if lat and lon:
        r = requests.get(
            'http://tools.wprdc.org/geo/reverse_geocode/', params={'lat': lat, 'lng': lon})
        if r.status_code == 200:
            j = r.json()

            return j['results']
        else:
            return None

class RightOfWaySchema(pl.BaseSchema):
    _id = fields.String(load_from='id'.lower(), dump_to='id')
    sequence = fields.Integer(dump_only=True, default=0, dump_to='sequence')
    _type = fields.String(load_from='type'.lower(), dump_to='type')
    opened_date = fields.Date(load_from='opened_date'.lower(), dump_to='open_date')
    from_date = fields.Date(load_from='from_date'.lower(), dump_to='from_date', allow_none=True)
    to_date = fields.Date(load_from='to_date'.lower(), dump_to='to_date', allow_none=True)
    restoration_date = fields.Date(load_from='restoration_date'.lower(), dump_to='restoration_date', allow_none=True)
    description = fields.String(load_from='description'.lower(), dump_to='description', allow_none=True)
    address = fields.String(load_from='address'.lower(), dump_to='address')
    street_or_location = fields.String(load_from='street_or_location'.lower(), dump_to='street_or_location', allow_none=True)
    from_street = fields.String(load_from='from_street'.lower(), dump_to='from_street', allow_none=True)
    to_street = fields.String(load_from='to_street'.lower(), dump_to='to_street', allow_none=True)
    business_name = fields.String(load_from='business_name'.lower(), dump_to='business_name', allow_none=True)
    license_type = fields.String(load_from='license_type'.lower(), dump_to='license_type', allow_none=True)

    #update_date = fields.DateTime(load_from='update_date'.lower(), dump_to='update_date')
    #total_fee = fields.Integer(load_from='total_fee'.lower(), dump_to='total_fee')
    #total_paid = fields.Integer(load_from='total_paid'.lower(), dump_to='total_paid')
    #public_owned = fields.Boolean(load_from='public_owned'.lower(), dump_to='public_owned')
    #status = fields.String(load_from='status'.lower(), dump_to='status', allow_none=True)
    #closed_date = fields.Date(load_from='closed_date'.lower(), dump_to='closed_date', allow_none=True)
    #fire_zone = fields.Integer(load_from='fire_zone'.lower(), dump_to='fire_zone', allow_none=True)
    # police_zone = fields.Integer(load_from='police_zone'.lower(), dump_to='police_zone', allow_none=True)
    neighborhood = fields.String(load_from='neighborhood'.lower(), dump_to='neighborhood', allow_none=True)
    council_district = fields.Integer(load_from='council_district'.lower(), dump_to='council_district', allow_none=True)
    ward = fields.Integer(load_from='ward'.lower(), dump_to='ward', allow_none=True)
    tract = fields.String(dump_to='tract', dump_only=True, allow_none=True)
    dpw_division = fields.Integer(load_from='dpw_division'.lower(), dump_to='public_works_division', allow_none=True)
    latitude = fields.Float(load_from='lat'.lower(), dump_to='address_lat', allow_none=True)
    longitude = fields.Float(load_from='long'.lower(), dump_to='address_lon', allow_none=True)
    from_lat = fields.Float(dump_only=True, allow_none=True, default=None)
    from_lon = fields.Float(dump_only=True, allow_none=True, default=None)
    to_lat = fields.Float(dump_only=True, allow_none=True, default=None)
    to_lon = fields.Float(dump_only=True, allow_none=True, default=None)

    @pre_load
    def reverse_geocode(self, data):

        if data['neighborhood'] in ['', None]: # Some of the records are already
            # reverse-geocoded, so avoid redoing the same work.
            if 'lat' in data and 'long' in data and data['lat'] not in ['', None]:
                geo_data = rev_geocode(data['long'], data['lat'])
                if geo_data:
                    ic(data)
                    assert False
                    if 'pittsburgh_neighborhood' in geo_data:
                        data['neighborhood'] = geo_data['pittsburgh_neighborhood']['name']

                    if 'pittsburgh_city_council' in geo_data:
                        data['council_district'] = geo_data['pittsburgh_city_council']['name']

                    if 'pittsburgh_ward' in geo_data:
                        data['ward'] = geo_data['pittsburgh_ward']['name']

                    if 'us_census_tract' in geo_data:
                        data['tract'] = geo_data['us_census_tract']['name']

                    if 'pittsburgh_dpw_division' in geo_data:
                        data['public_works_division'] = geo_data['pittsburgh_dpw_division']['name']

                    if 'pittsburgh_ward' in geo_data:
                        data['pli_division'] = geo_data['pittsburgh_ward']['name']

                    if 'pittsburgh_police_zone' in geo_data:
                        data['police_zone'] = geo_data['pittsburgh_police_zone']['name']

                    if 'pittsburgh_fire_zone' in geo_data:
                        data['fire_zone'] = geo_data['pittsburgh_fire_zone']['name']

    class Meta:
        ordered = True

#package_id = "23482953-50cc-4370-858c-eb0c034b8157"
package_id = "23482953-50cc-4370-858c-eb0c034b8157" # Production package for Right-of-Way Permits

if package_id == "812527ad-befc-4214-a4d3-e621d8230563":
    print("Using the test package.")


job_dicts = [
    {
        'source_type': 'gcp',
        'source_dir': '',
        'source_file': 'accela_permits.csv',
        'encoding': 'utf-8-sig',
        'schema': RightOfWaySchema,
        'filters': [['opened_date', '>=', '2020-06-27']],
        'primary_key_fields': ['id', 'sequence', 'type'],
        'upload_method': 'upsert',
        'package': package_id,
        'resource_name': 'Right-of-Way Permits and Traffic-Obstruction Permits',
#        #'custom_post_processing': function_to_verify_that_the_table_was_updated_and_then_delete_the_gcp_blob,
    }
]
