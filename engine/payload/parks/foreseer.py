import requests, time, re, csv
from requests.auth import HTTPBasicAuth
from collections import OrderedDict, defaultdict
from datetime import date, timedelta, datetime
from dateutil.parser import parse

from engine.parameters.foreseer_credentials import FORESEER_USER, FORESEER_PASSWORD
from engine.parameters.remote_parameters import TEST_PACKAGE_ID

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.etl_util import Job, write_or_append_to_csv

from pprint import pprint
from icecream import ic

class ForeseerSchema(pl.BaseSchema):
    datetime = fields.DateTime(load_from='datetime', dump_to='datetime', allow_none=False)
    measurement_name = fields.String(load_from='measurement_name', dump_to='measurement_name', allow_none=False)
    value = fields.Float(load_from='value', dump_to='value', allow_none=False)
    units = fields.String(load_from='units', dump_to='units', allow_none=False)

    class Meta:
        ordered = True

def write_to_csv(filename, list_of_dicts, keys):
    ic(list_of_dicts)
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def select_point(devices, params, timestamp):
    device_name = params['device_name']
    point_number = params['point_number']
    measurement_name = params['measurement_name']

    device = next((d for d in devices if d['deviceName'] == device_name), None)
    assert device is not None
    point = next((p for p in device['pointList'] if int(p['pointUID']) == point_number), None)
    if point is None:
        print(f"Unable to find {point_number} among these {[p['pointUID'] for p in device['pointList']]}.")
        return None

    valid = point['pointAlarmStateString'] == 'Normal'
    if 'default_units' in params:
        units = params['default_units']
    else:
        units = point['pointUnits']

    return {'datetime': timestamp.isoformat(),
            'measurement_name': measurement_name,
            'description': point['pointDescription'],
            'value': point['pointValue'] if valid else None,
            'units': units if valid else None,
            'alarm_state': point['pointAlarmStateString'],
            'archive_type': point['pointArchiveType']
            }

def pull_measuremets_from_foreseer(jobject, **kwparameters):
    if not kwparameters['use_local_files']:
        #r = requests.get("https://foreseer.pittsburghparks.org/WebViews/JSON/export.py?realtimeonly=true", auth=HTTPBasicAuth(FORESEER_USER, FORESEER_PASSWORD))
        r = requests.get("https://foreseer.pittsburghparks.org/WebViews/JSON/export.py", auth=HTTPBasicAuth(FORESEER_USER, FORESEER_PASSWORD))
        d = r.json()
        import json
        with open('export.json', 'w') as f:
            f.write(json.dumps(d))

        timestamp = parse(d['server']['serverTime'])
        devices = d['deviceList']

        dicts = []
        headers = ['datetime', 'measurement_name', 'value', 'units']
        point_parameters = [{'device_name': 'PowerMeter-Sub09', 'point_number': 832, 'measurement_name': 'Total Energy Production of the Environmental Center'},
                {'device_name': 'PowerMeter-MainSub', 'point_number': 985, 'measurement_name': 'Total Energy Usage of the Environmental Center'},
                {'device_name': 'PowerMeter-Sub02', 'point_number': 619, 'measurement_name': 'Barn Energy Usage'},
                {'device_name': 'PowerMeter-Sub03', 'point_number': 663, 'measurement_name': 'Fountain Energy Usage'},
                {'device_name': '_Additional Derived Chan', 'point_number': 1184, 'measurement_name': 'Daily Energy Usage (so far this day)', 'default_units': 'kWh'},
                {'device_name': '_Additional Derived Chan', 'point_number': 1178, 'measurement_name': 'Daily Solar Production (so far this day)', 'default_units': 'kWh'},
                {'device_name': 'BraeSystem', 'point_number': 889, 'measurement_name': 'Rain Water Usage'},
                {'device_name': 'BraeSystem', 'point_number': 891, 'measurement_name': 'Water System Pressure'},
                {'device_name': 'BraeSystem', 'point_number': 892, 'measurement_name': 'Cistern Level', 'default_units': '%'},
                ]
        for params in point_parameters:
            dicts.append(select_point(devices, params, timestamp))

        write_to_csv(jobject.local_cache_filepath, dicts, headers)

foreseer_package_id = TEST_PACKAGE_ID # Production version of            data package

job_dicts = [
    {
        'job_code': 'foreseer',
        'source_type': 'local',
        'source_dir': '',
        'source_file': f'foreseer.csv',
        'custom_processing': pull_measuremets_from_foreseer,
        'encoding': 'utf-8-sig',
        'schema': ForeseerSchema,
        'primary_key_fields': ['datetime', 'measurement_name'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'destinations': ['file'], # These lines are just for testing
        'destination_file': f'foreseer.csv', # purposes.
        'package': foreseer_package_id,
        'resource_name': f'Frick Environmental Center Sensor Readings'
    },
]
