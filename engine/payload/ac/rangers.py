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


class RangersOutreachSchema(pl.BaseSchema):
    month = fields.Date(load_from='month')
    location = fields.String(load_from='location')
    school_name = fields.String(load_from='school.name', allow_none=True) ### Not included in new data
    libraries = fields.String(load_from='libraries', allow_none=True) ### Not included in new data
    special_event = fields.String(load_from='special.event', allow_none=True)
    outreach_group = fields.String(load_from='outreach.group', allow_none=True)
    average_participant_age = fields.String(load_from='avg.age..outreach.contacts.', allow_none=True)
    programming = fields.String(load_from='programming', allow_none=True)
    date = fields.Date(load_from='date', allow_none=True)
    time = fields.String(load_from='time', allow_none=True)
    contact_type = fields.String(load_from='contact.type')
    number = fields.Integer(load_from='number', allow_none=True)
    volunteer_hours = fields.String(load_from='volunteer.hours', allow_none=True)
    ranger_trail_work_hours = fields.Float(load_from='ranger.trail.work.hours', allow_none=True)
    notes = fields.String(load_from='notes', allow_none=True)
    latitude = fields.Float(load_from='lat', allow_none=True)
    longitude = fields.Float(load_from='long', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nas_and_strip(self, data):
        fs = ['special_event', 'programming', 'outreach_group', 'school_name', 'libraries', 'location', 'notes']
        for f in fs:
            if f in data and data[f] not in [None]:
                data[f] = data[f].strip()

rangers_package_id = '01894671-26ec-423c-985c-55c66220e433' # Production version of Park Rangers Outreach dataset

job_dicts = [
    {
        'job_code': 'rangers_outreach',
        'source_type': 'sftp',
        'source_dir': 'ParkRangers',
        'source_file': 'ranger_contacts.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': RangersOutreachSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': rangers_package_id,
        'resource_name': 'Allegheny County Parks Ranger Outreach',
    },
]
