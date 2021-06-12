import csv, json, requests, sys, traceback
from datetime import datetime, date, timedelta
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.geojson2csv import convert_big_destination_geojson_file_to_source_csv

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa



job_dicts = []
# Try using the data.json file and rocket-etl/engine/arcgis_util.py for the PASDA datasets.
job_dicts = []
today = date.today()
year_month = today.strftime("%Y%m")
#first_day_of_month = today.replace(day=1)
#last_day_of_last_month = first_day_of_month - timedelta(days=1)
#year_month = last_day_of_last_month.strftime("%Y%m")
# On 05-26, the May version of the Shapefile is available...

########################3
base_job_code = 'ac_parcels' # 'Allegheny County Parcel Boundaries'
package_id = '709e4e52-6f82-4cd0-a848-f3e2b3f5d22b'


job_dicts += [
#    {
#        'job_code': f'{base_job_code}_web',
#        'source_type': 'http',
#        'source_full_url': # Should be 'https://www.pasda.psu.edu/uci/DataSummary.aspx?dataset=1214'
#        'encoding': 'utf-8',
#        'destination': 'ckan_link',
#        'package': package_id,
#        'resource_name': f'ArcGIS Hub Dataset',
#    },
#    {
#        'job_code': f'{base_job_code}_api',
#        'source_type': 'http',
#        'source_full_url': # Should be 'https://maps.pasda.psu.edu/ArcGIS/rest/services/pasda/AlleghenyCounty/MapServer/25'
#        'encoding': 'utf-8',
#        'destination': 'ckan_link',
#        'package': package_id,
#        'resource_name': f'Esri Rest API',
#    },
    {
        'job_code': f'{base_job_code}_geojson',
        'source_type': 'http',
        'source_full_url': f'https://www.pasda.psu.edu/json/AlleghenyCounty_Parcels{year_month}.geojson',
        'encoding': 'utf-8',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'GeoJSON',
    },
#    {
#        'job_code': f'{base_job_code}_csv', #### Seemingly unsupported!
#        'source_type': 'http',
#        'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'CSV', ag_dataset)[0],
#        'encoding': 'utf-8',
#        'schema': schema,
#        'always_wipe_data': True,
#        #'primary_key_fields': ['\ufeffobjectid', 'id_no', 'oid', 'id']
#        'destination': 'ckan',
#        'package': package_id,
#        'resource_name': f'CSV',
#        'upload_method': 'insert',
#    },
    {
        'job_code': f'{base_job_code}_kml',
        'source_type': 'http',
        'source_full_url': f'https://www.pasda.psu.edu/kmz/AlleghenyCounty_Parcels{year_month}.kmz',
        'encoding': 'binary',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'KML',
    },
    {
        'job_code': f'{base_job_code}_shapefile',
        'source_type': 'ftp',
        #'source_full_url': f'ftp://ftp.pasda.psu.edu/pub/pasda/alleghenycounty/AlleghenyCounty_Parcels{year_month}.zip'
        'source_site': 'ftp.pasda.psu.edu',
        'source_dir': 'pub/pasda/alleghenycounty',
        'source_file': f'AlleghenyCounty_Parcels{year_month}.zip',
        'encoding': 'binary',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'Shapefile',
    },
#    {
#        'job_code': f'tornados',
#        'source_type': 'ftp',
#        #'source_full_url': f'ftp://ftp.pasda.psu.edu/pub/pasda/alleghenycounty/AlleghenyCounty_Parcels{year_month}.zip'
#        'source_site': 'ftp.pasda.psu.edu',
#        'source_dir': 'pub/pasda/noaa',
#        'source_file': f'Tornados_PA.zip', # A small (35k) file for testing the FTP connection.
#        'encoding': 'binary',
#        'destination': 'ckan_filestore',
#        'package': package_id,
#        'resource_name': f'Shapefile',
#    },
]

########################3
class AddressPointsSchema(pl.BaseSchema):
    feature_key = fields.String(load_from='FEATURE_KE'.lower(), dump_to='feature_key')
    address_id = fields.String(load_from='ADDRESS_ID'.lower(), dump_to='address_id')
    parent_id = fields.String(load_from='PARENT_ID'.lower(), dump_to='parent_id')
    street_id = fields.String(load_from='STREET_ID'.lower(), dump_to='street_id')
    address_ty = fields.Integer(load_from='ADDRESS_TY'.lower(), dump_to='address_type')
    status = fields.String(load_from='STATUS'.lower(), dump_to='status', allow_none=True)
    addr_num_p = fields.String(load_from='ADDR_NUM_P'.lower(), dump_to='addr_num_prefix', allow_none=True)
    addr_num = fields.String(load_from='ADDR_NUM'.lower(), dump_to='addr_num', allow_none=True)
    addr_num_s = fields.String(load_from='ADDR_NUM_S'.lower(), dump_to='addr_num_suffix', allow_none=True)
    st_premodi = fields.String(load_from='ST_PREMODI'.lower(), dump_to='st_premodifier', allow_none=True)
    st_prefix = fields.String(load_from='ST_PREFIX'.lower(), dump_to='st_prefix', allow_none=True)
    st_pretype = fields.String(load_from='ST_PRETYPE'.lower(), dump_to='st_pretype', allow_none=True)
    st_name = fields.String(load_from='ST_NAME'.lower(), dump_to='st_name')
    st_type = fields.String(load_from='ST_TYPE'.lower(), dump_to='st_type', allow_none=True)
    st_postmod = fields.String(load_from='ST_POSTMOD'.lower(), dump_to='st_postmodifier', allow_none=True)
    unit_type = fields.String(load_from='UNIT_TYPE'.lower(), dump_to='unit_type', allow_none=True)
    unit = fields.String(load_from='UNIT'.lower(), dump_to='unit', allow_none=True)
    floor = fields.String(load_from='FLOOR'.lower(), dump_to='floor', allow_none=True)
    municipali = fields.String(load_from='MUNICIPALI'.lower(), dump_to='municipality')
    county = fields.String(load_from='COUNTY'.lower(), dump_to='county')
    state = fields.String(load_from='STATE'.lower(), dump_to='state')
    zip_code = fields.String(load_from='ZIP_CODE'.lower(), dump_to='zip_code')
    comment = fields.String(load_from='COMMENT'.lower(), dump_to='comment', allow_none=True)
    edit_date = fields.Date(load_from='EDIT_DATE'.lower(), dump_to='edit_date', allow_none=True)
    edit_user = fields.String(load_from='EDIT_USER'.lower(), dump_to='edit_user', allow_none=True)
    source = fields.String(load_from='SOURCE'.lower(), dump_to='source', allow_none=True)
    exp_flag = fields.String(load_from='EXP_FLAG'.lower(), dump_to='exp_flag', allow_none=True)
    full_addre = fields.String(load_from='FULL_ADDRE'.lower(), dump_to='full_address')
    point_x = fields.Float(load_from='POINT_X'.lower(), dump_to='point_x')
    point_y = fields.Float(load_from='POINT_Y'.lower(), dump_to='point_y')
    lat = fields.Float(load_from='LAT'.lower(), dump_to='latitude')
    lng = fields.Float(load_from='LNG'.lower(), dump_to='longitude')

    class Meta:
        ordered = True

schema = AddressPointsSchema
base_job_code = 'address_points' # 'Allegheny County Address Points'
package_id = '4988ae5c-a677-4a7f-9bd0-e735c19a8ff3'


job_dicts += [
#    {
#        'job_code': f'{base_job_code}_web',
#        'source_type': 'http',
#        'source_full_url': # Should be 'https://www.pasda.psu.edu/uci/DataSummary.aspx?dataset=1219'
#        'encoding': 'utf-8',
#        'destination': 'ckan_link',
#        'package': package_id,
#        'resource_name': f'ArcGIS Hub Dataset',
#    },
#    {
#        'job_code': f'{base_job_code}_api',
#        'source_type': 'http',
#        'source_full_url': # Should be 'https://maps.pasda.psu.edu/ArcGIS/rest/services/pasda/AlleghenyCounty/MapServer/32'
#        'encoding': 'utf-8',
#        'destination': 'ckan_link',
#        'package': package_id,
#        'resource_name': f'Esri Rest API',
#    },
    {
        'job_code': f'{base_job_code}_geojson',
        'source_type': 'http',
        'source_full_url': f'https://www.pasda.psu.edu/json/AlleghenyCounty_AddressPoints{year_month}.geojson',
        'encoding': 'utf-8',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'GeoJSON',
    },
#    { # There is a PASDA version of this file, but it's currently incomplete.
#        'job_code': f'{base_job_code}_csv',
#        'source_type': 'http',
#        'source_full_url': f'https://www.pasda.psu.edu/spreadsheet/AlleghenyCounty_AddressPoints{year_month}.csv',
#        'encoding': 'utf-8',
#        'schema': schema,
#        'always_wipe_data': True,
#        #'primary_key_fields': ['\ufeffobjectid', 'id_no', 'oid', 'id']
#        'destination': 'ckan',
#        'package': package_id,
#        'resource_name': f'CSV',
#        'upload_method': 'insert',
#    },
    {
        'job_code': f'{base_job_code}_kml',
        'source_type': 'http',
        'source_full_url': f'https://www.pasda.psu.edu/kmz/AlleghenyCounty_AddressPoints{year_month}.kmz',
        'encoding': 'binary',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'KMZ',
    },
    {
        'job_code': f'{base_job_code}_shapefile',
        'source_type': 'ftp',
        'source_site': 'ftp.pasda.psu.edu',
        'source_dir': 'pub/pasda/alleghenycounty',
        'source_file': f'AlleghenyCounty_AddressPoints{year_month}.zip',
        'encoding': 'binary',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'Shapefile',
    },
    { # Because the PASDA CSV file was incomplete once, download the GeoJSON file
    # and generate a CSV verson of it.
        'job_code': f'{base_job_code}_geojson_download',
        'source_type': 'http',
        'source_full_url': f'https://www.pasda.psu.edu/json/AlleghenyCounty_AddressPoints{year_month}.geojson',
        'encoding': 'utf-8',
        'destination': 'file',
        'custom_post_processing': convert_big_destination_geojson_file_to_source_csv,
    },
    {
        'job_code': f'{base_job_code}_csv_converted',
        'source_type': 'local',
        'source_file': f'AlleghenyCounty_AddressPoints{year_month}.csv',
        'encoding': 'utf-8',
        'schema': schema,
        'always_wipe_data': True,
        #'primary_key_fields': ['\ufeffobjectid', 'id_no', 'oid', 'id']
        'destination': 'ckan',
        'package': package_id,
        'resource_name': f'CSV',
        'upload_method': 'insert',
    },
]

########################3
base_job_code = 'centerlines' # 'Allegheny County Address Points'
package_id = '34f6668d-130d-4e10-b49b-598c43b83d27'


job_dicts += [
#    {
#        'job_code': f'{base_job_code}_web',
#        'source_type': 'http',
#        'source_full_url': # Should be 'https://www.pasda.psu.edu/uci/DataSummary.aspx?dataset=1224'
#        'encoding': 'utf-8',
#        'destination': 'ckan_link',
#        'package': package_id,
#        'resource_name': f'ArcGIS Hub Dataset',
#    },
#    {
#        'job_code': f'{base_job_code}_api',
#        'source_type': 'http',
#        'source_full_url': # Should be 'https://maps.pasda.psu.edu/ArcGIS/rest/services/pasda/AlleghenyCounty/MapServer/7'
#        'encoding': 'utf-8',
#        'destination': 'ckan_link',
#        'package': package_id,
#        'resource_name': f'Esri Rest API',
#    },
    {
        'job_code': f'{base_job_code}_geojson',
        'source_type': 'http',
        'source_full_url': f'https://www.pasda.psu.edu/json/AlleghenyCounty_StreetCenterlines{year_month}.geojson',
        'encoding': 'utf-8',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'GeoJSON',
    },
#    {
#        'job_code': f'{base_job_code}_csv',
#        'source_type': 'http',
#        'source_full_url': f'https://www.pasda.psu.edu/spreadsheet/AlleghenyCounty_StreetCenterlines{year_month}.csv', # not supported
#        'encoding': 'utf-8',
#        'schema': schema,
#        'always_wipe_data': True,
#        #'primary_key_fields': ['\ufeffobjectid', 'id_no', 'oid', 'id']
#        'destination': 'ckan',
#        'package': package_id,
#        'resource_name': f'CSV',
#        'upload_method': 'insert',
#    },
    {
        'job_code': f'{base_job_code}_kml',
        'source_type': 'http',
        'source_full_url': f'https://www.pasda.psu.edu/kmz/AlleghenyCounty_StreetCenterlines{year_month}.kmz',
        'encoding': 'binary',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'KML',
    },
    {
        'job_code': f'{base_job_code}_shapefile',
        'source_type': 'ftp',
        'source_site': 'ftp.pasda.psu.edu',
        'source_dir': 'pub/pasda/alleghenycounty',
        'source_file': f'AlleghenyCounty_StreetCenterlines{year_month}.zip',
        'encoding': 'binary',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'Shapefile',
    }
]
