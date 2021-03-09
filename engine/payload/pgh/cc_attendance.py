import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load, pre_dump
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import fetch_city_file
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class RecCenterAttendanceSchema(pl.BaseSchema):
    date = fields.Date()
    center_name = fields.String(load_from="centername")
    attendance_count = fields.Integer(load_from="attendancecount")

    class Meta:
        ordered = True

    @pre_load
    def fix_date(self, data):
        data['date'] = datetime.strptime(data['date'], '%Y-%m-%d').isoformat()


def conditionally_get_city_files(job, **kwparameters):
    if not kwparameters['use_local_input_file']:
        fetch_city_file(job)

cc_attendance_package_id = "5b0b8acc-d8fc-4278-bc57-684e2e4faab5" # Production version of Community Center Attendance package

job_dicts = [
    {
        'job_code': 'cc_attendance',
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'RecCenterDailyAtten.csv',
        'encoding': 'utf-8-sig',
        'custom_processing': conditionally_get_city_files,
        'schema': RecCenterAttendanceSchema,
        'always_wipe_data': True,
        'primary_key_fields': ['date', 'center_name'],
        'upload_method': 'upsert',
        'package': cc_attendance_package_id,
        'resource_name': 'Community Center Daily Attendance'
     },
]
