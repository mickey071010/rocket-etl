import csv, json, requests, sys, traceback
from datetime import datetime, date, timedelta
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID

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
#        'source_full_url': # Should be 'https://maps.pasda.psu.edu/ArcGIS/rest/services/pasda/AlleghenyCounty/MapServer'
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
