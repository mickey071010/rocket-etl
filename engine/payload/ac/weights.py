import csv, json, requests, sys, traceback
from datetime import datetime
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

class DevicesSchema(pl.BaseSchema):
    deviceid = fields.String(load_from='DeviceID', dump_to='device_id')
    storeid = fields.String(load_from='StoreID', dump_to='store_id')
    devicetype = fields.String(load_from='DeviceType', dump_to='device_type')
    devicegroup = fields.String(load_from='DeviceGroup', dump_to='device_group')
    make = fields.String(allow_none=True)
    model = fields.String(allow_none=True)
    serialnumber = fields.String(load_from='SerialNumber', dump_to='serial_number', allow_none=True) # Possibly 'NONE GIVEN' should be coereced to None
    pump = fields.String(allow_none=True)
    grade = fields.String(allow_none=True)
    capacity = fields.String(allow_none=True)
    remarks = fields.String(allow_none=True)
    deleted = fields.String(allow_none=True)

#    store_id = fields.String(dump_to='StoreID')
#    device_type = fields.String(dump_to='DeviceType')
#    device_group = fields.String(dump_to='DeviceGroup')
#    make = fields.String(dump_to='Make',allow_none=True)
#    model = fields.String(dump_to='Model',allow_none=True)
#    serial_number = fields.String(dump_to='SerialNumber',allow_none=True) # Possibly 'NONE GIVEN' should be coereced to None
#    pump = fields.String(dump_to='Pump',allow_none=True)
#    grade = fields.String(dump_to='Grade',allow_none=True)
#    capacity = fields.String(dump_to='Capacity',allow_none=True)
#    remarks = fields.String(dump_to='Remarks',allow_none=True)
#    deleted = fields.String(dump_to='Deleted',allow_none=True)

    class Meta:
        ordered = True

class InspectionsSchema(pl.BaseSchema):
    inspectionid = fields.String(load_from='InspectionID', dump_to='inspection_id')
    storeid = fields.String(load_from='StoreID', dump_to='store_id')
    deviceid = fields.String(load_from='DeviceID', dump_to='device_id')
    date = fields.Date() # It appears that the source file has nicely formatted date strings.
    result = fields.String()
    reinspection = fields.String()

    class Meta:
        ordered = True


class StoresSchema(pl.BaseSchema):
    storeid = fields.String(load_from='StoreID', dump_to='store_id')
    storename = fields.String(load_from='StoreName', dump_to='store_name')
    address = fields.String()
    mailingcity = fields.String(load_from='MailingCity', dump_to='mailing_city', allow_none=True)
    state = fields.String()
    zip = fields.String()
    municipality = fields.String(allow_none=True)
    corpid = fields.String(load_from='CorpID', dump_to='corp_id',allow_none=True)
    neighborhood = fields.String(allow_none=True)
    point_x = fields.Float(load_from='POINT_X',dump_to='longitude',allow_none=True)
    point_y = fields.Float(load_from='POINT_Y',dump_to='latitude',allow_none=True)
    businessphone = fields.String(load_from='BusinessPhone', dump_to='business_phone', allow_none=True)
    alternatephone = fields.String(load_from='AlternatePhone', dump_to='alternate_phone', allow_none=True)
    price_verification = fields.String()
    fueldispenser = fields.String(load_from='FuelDispenser', dump_to='fuel_dispenser')
    scale = fields.String()
    timing = fields.String()
    miscinspection = fields.String(load_from='MiscInspection', dump_to='misc_inspection')
    oob = fields.String()
    unit = fields.String(allow_none=True)
    shoppingcenter = fields.String(load_from='ShoppingCenter', dump_to='shopping_center', allow_none=True)
    newmunicipality = fields.String(load_from='NewMunicipality', dump_to='new_municipality', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k in ['point_x', 'point_y']:
                if v in ['NA']:
                    data[k] = None

weights_and_measures_package_id = "fd140adb-d740-4ce3-b1e7-31f37bf97d88"
job_dicts = [ # Notes: "Each of these is a dump with all the data, so it should be fairly simple-no worries about appending new data, etc." ==> These could be 'insert'/clear_first=True operations with no primary keys.
    {
        'job_code': 'devices',
        'source_type': 'sftp',
        'source_dir': 'Weights & Measures',
        'source_file': f'devices.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': DevicesSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': weights_and_measures_package_id,
        'resource_name': 'Devices',
    },
    {
        'job_code': 'inspections',
        'source_type': 'sftp',
        'source_dir': 'Weights & Measures',
        'source_file': f'inspections.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': InspectionsSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': weights_and_measures_package_id,
        'resource_name': 'Inspections',
    },
    {
        'job_code': 'stores',
        'source_type': 'sftp',
        'source_dir': 'Weights & Measures',
        'source_file': f'stores.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': StoresSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': weights_and_measures_package_id,
        'resource_name': 'Stores'
    },
]
