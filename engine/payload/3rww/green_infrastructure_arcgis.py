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
from engine.parameters.remote_parameters import TEST_PACKAGE_ID

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

seeds = [] # The point of the seeds list is to let another counterpart script (called "counterpart.py")
# come along, pull the data_json_url and list of seeds out of this script, and
# then look for deviations between the live data.json file and the seeds coverage.

# [ ] Add some kind of flag to let a scheduler recognize this as an ArcGIS script?
# [ ] How does test_all traverse all the files? Maybe use that approach.


#############
class GreenInfrastructureSchema(pl.BaseSchema):
    object_id = fields.Integer(load_from='\ufeffOBJECTID'.lower(), dump_to='OBJECTID')
    project_name = fields.String(load_from='ProjectName'.lower(), dump_to='ProjectName')
    project_description = fields.String(load_from='ProjectDescription'.lower(), dump_to='ProjectDescription', allow_none=True)
    project_size = fields.String(load_from='ProjectSize'.lower(), dump_to='ProjectSize', allow_none=True)
    date_built = fields.String(load_from='DateBuilt'.lower(), dump_to='DateBuilt', allow_none=True)
    reduction_percent = fields.String(load_from='ReductionPercent'.lower(), dump_to='ReductionPercent', allow_none=True)
    reduction_volume = fields.String(load_from='ReductionVolume'.lower(), dump_to='ReductionVolume', allow_none=True)
    acreage_managed = fields.String(load_from='AcreageManaged'.lower(), dump_to='AcreageManaged', allow_none=True)
    impervious_acres_managed = fields.String(load_from='ImperviousAcresManaged'.lower(), dump_to='ImperviousAcresManaged', allow_none=True)
    global_iD = fields.String(load_from='GlobalID'.lower(), dump_to='GlobalID')
    size_vol = fields.Integer(load_from='size_vol'.lower(), dump_to='size_vol', allow_none=True)
    size_area = fields.Integer(load_from='size_area'.lower(), dump_to='size_area', allow_none=True)
    cost_total = fields.Integer(load_from='cost_total'.lower(), dump_to='cost_total', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        fields_with_nas = ['reductionpercent', 'acreagemanaged', 'imperviousacresmanaged']
        for f in fields_with_nas:
            if data[f] in ['NA']:
                data[f] = None
            
#####

data_json_url = 'https://data-3rww.opendata.arcgis.com/data.json'
package_id = '097c70b6-d5d3-434b-af90-23c8b1a99bfb'
arcgis_dataset_title = '3RWW Green Infrastructure Inventory'
gi_dataset, _ = get_arcgis_dataset(arcgis_dataset_title, data_json_url, None) # Cache data.json
# to avoid looking it up for each job in this file.

schema = GreenInfrastructureSchema

base_job_code = 'green_infrastructure'

job_dicts = [
    {
        'job_code': f'{base_job_code}_csv',
        'source_type': 'http',
        'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'CSV', gi_dataset)[0],
        'encoding': 'utf-8',
        'schema': schema,
        'always_wipe_data': True,
        #'primary_key_fields': ['\ufeffobjectid', 'id_no', 'oid', 'id']
        'destination': 'ckan' if schema is not None else 'ckan_filestore',
        'package': package_id,
        'resource_name': f'3RWW Green Infrastructure Inventory',
        'upload_method': 'insert',
    },
    {
        'job_code': f'{base_job_code}_geojson',
        'source_type': 'http',
        'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'GeoJSON', gi_dataset)[0],
        'encoding': 'utf-8',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'3RWW Green Infrastructure Inventory',
    },
    {
        'job_code': f'{base_job_code}_shapefile',
        'source_type': 'http',
        'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'Shapefile', gi_dataset)[0],
        'encoding': 'binary',
        'destination': 'ckan_filestore',
        'package': package_id,
        'resource_name': f'3RWW Green Infrastructure Inventory',
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
