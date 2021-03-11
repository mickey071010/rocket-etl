import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.local_parameters import SOURCE_DIR

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def get_socrata_updated_date(url):
    import xmltodict
    headers = {"Accept": "application/atom+xml,application/atomsvc+xml,application/xml"} 
    # Without these headers, the returned JSON lacks the desired metadata.
    d = xmltodict.parse(requests.get(url, headers=headers).content)
    timestamp = d['a:feed']['a:entry'][0]['a:updated']
    return parser.parse(timestamp).date().isoformat()

class ByResidenceSchema(pl.BaseSchema):
    job_code = 'by_residence'
    date_updated_from_site = get_socrata_updated_date("https://data.pa.gov/api/odata/v4/gcnb-epac")
    date_updated = fields.Date(dump_only=True, dump_to='date_updated', default=date_updated_from_site)

    county = fields.String(load_from='County_Name'.lower(), dump_to='county')
    county_population = fields.Integer(load_from='County_Population'.lower(), dump_to='county_population')
    partially_covered = fields.Integer(load_from='Partially_Covered'.lower(), dump_to='partially_covered')
    rate_partially_covered = fields.Float(load_from='Rate_Partially_Covered'.lower(), dump_to='rate_partially_covered', allow_none=True)
    fully_covered = fields.Integer(load_from='Fully_Covered'.lower(), dump_to='fully_covered')
    rate_fully_covered = fields.Float(load_from='Rate_Fully_Covered'.lower(), dump_to='rate_fully_covered', allow_none=True)

    class Meta:
        ordered = True

# dfg

vaccinations_stats_archive_package_id = '5a3230bb-5a51-4eec-90bd-ec8796325216'

job_dicts = [
    {
        'job_code': ByResidenceSchema().job_code, # 'by_residence'
        'source_type': 'http',
        'source_file': 'COVID-19_Vaccinations_by_Residence_Current_County_Health.csv',
        'source_full_url': 'https://data.pa.gov/api/views/gcnb-epac/rows.csv?accessType=DOWNLOAD&api_foundry=true',
        'schema': ByResidenceSchema,
        'primary_key_fields': ['date_updated', 'county'],
        'destination': 'ckan',
        'destination_file': 'vaccinations_by_residence.csv',
        'package': vaccinations_stats_archive_package_id,
        'resource_name': 'COVID-19 Vaccinations by Residence Current County Health (archive)',
        'upload_method': 'upsert',
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
