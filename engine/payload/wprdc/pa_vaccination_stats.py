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

class ByAgeGroupSchema(pl.BaseSchema):
    job_code = 'by_age_group'
    date_updated_from_site = get_socrata_updated_date("https://data.pa.gov/api/odata/v4/niuh-2xe3")
    date_updated = fields.Date(dump_only=True, dump_to='date_updated', default=date_updated_from_site)

    county = fields.String(load_from='County_Name'.lower(), dump_to='county')
    age_group = fields.String(load_from='Age_Group'.lower(), dump_to='age_group')
    coverage = fields.String(load_from='Coverage'.lower(), dump_to='coverage')
    total_count = fields.Integer(load_from='Total_Count'.lower(), dump_to='total_count', allow_none=True)

    class Meta:
        ordered = True

class ByAgeGroupStatewideSchema(pl.BaseSchema):
    job_code = 'by_age_group_pa'
    date_updated_from_site = get_socrata_updated_date("https://data.pa.gov/api/odata/v4/xy2e-dqvt")
    date_updated = fields.Date(dump_only=True, dump_to='date_updated', default=date_updated_from_site)

    age_group = fields.String(load_from='Age_Group'.lower(), dump_to='age_group')
    partially_covered = fields.Integer(load_from='Partially_Covered'.lower(), dump_to='partially_covered')
    fully_covered = fields.Integer(load_from='Fully_Covered'.lower(), dump_to='fully_covered')

    class Meta:
        ordered = True

class ByRaceSchema(pl.BaseSchema):
    job_code = 'by_race'
    date_updated_from_site = get_socrata_updated_date("https://data.pa.gov/api/odata/v4/x5z9-57ub")
    date_updated = fields.Date(dump_only=True, dump_to='date_updated', default=date_updated_from_site)

    county = fields.String(load_from='County_Name'.lower(), dump_to='county')
    race = fields.String(load_from='Race'.lower(), dump_to='race')
    coverage = fields.String(load_from='Coverage'.lower(), dump_to='coverage')
    total_count = fields.Integer(load_from='Total_Count'.lower(), dump_to='total_count', allow_none=True)

    class Meta:
        ordered = True

class ByRaceStatewideSchema(pl.BaseSchema):
    job_code = 'by_race_pa'
    date_updated_from_site = get_socrata_updated_date("https://data.pa.gov/api/odata/v4/e384-bs7r")
    date_updated = fields.Date(dump_only=True, dump_to='date_updated', default=date_updated_from_site)

    race = fields.String(load_from='Race'.lower(), dump_to='race')
    partially_covered = fields.Integer(load_from='Partially_Covered'.lower(), dump_to='partially_covered')
    fully_covered = fields.Integer(load_from='Fully_Covered'.lower(), dump_to='fully_covered')

    class Meta:
        ordered = True

class ByDayAndCountySchema(pl.BaseSchema):
    job_code = 'by_day_and_county'
    #date_updated_from_site = get_socrata_updated_date("https://data.pa.gov/api/odata/v4/bicw-3gwi")
    #date_updated = fields.Date(dump_only=True, dump_to='date_updated', default=date_updated_from_site) # Not needed
    # for this job.
    date = fields.Date(load_from='Date'.lower(), dump_to='date')
    county = fields.String(load_from='County_Name'.lower(), dump_to='county')
    partially_covered = fields.Integer(load_from='Partially Covered'.lower(), dump_to='partially_covered', allow_none=True)
    fully_covered = fields.Integer(load_from='Fully_Covered'.lower(), dump_to='fully_covered', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def remove_commas_from_numbers(self, data):
        f = 'fully_covered'
        if f in data and data[f] is not None:
            data[f] = re.sub(',', '', data[f])

class ByGenderSchema(pl.BaseSchema):
    job_code = 'by_gender'
    date_updated_from_site = get_socrata_updated_date("https://data.pa.gov/api/odata/v4/bicw-3gwi")
    date_updated = fields.Date(dump_only=True, dump_to='date_updated', default=date_updated_from_site)
    gender = fields.String(load_from='Gender'.lower(), dump_to='gender')
    coverage = fields.String(load_from='Coverage'.lower(), dump_to='coverage')
    total_count = fields.Integer(load_from='Total_Count'.lower(), dump_to='total_count', allow_none=True)

    class Meta:
        ordered = True

class ByGenderStatewideSchema(pl.BaseSchema):
    job_code = 'by_gender_pa'
    date_updated_from_site = get_socrata_updated_date("https://data.pa.gov/api/odata/v4/jweg-3ezy")
    date_updated = fields.Date(dump_only=True, dump_to='date_updated', default=date_updated_from_site)
    gender = fields.String(load_from='Gender'.lower(), dump_to='gender')
    partially_covered = fields.Integer(load_from='Partially Covered'.lower(), dump_to='partially_covered', allow_none=True)
    fully_covered = fields.Integer(load_from='Fully_Covered'.lower(), dump_to='fully_covered', allow_none=True)

    class Meta:
        ordered = True

class ByEthnicitySchema(pl.BaseSchema):
    job_code = 'by_ethnicity'
    date_updated_from_site = get_socrata_updated_date("https://data.pa.gov/api/odata/v4/7ruj-m7k6")
    date_updated = fields.Date(dump_only=True, dump_to='date_updated', default=date_updated_from_site)
    ethnicity = fields.String(load_from='Ethnicity'.lower(), dump_to='ethnicity')
    coverage = fields.String(load_from='Coverage'.lower(), dump_to='coverage')
    total_count = fields.Integer(load_from='Total_Count'.lower(), dump_to='total_count', allow_none=True)

    class Meta:
        ordered = True

class ByEthnicityStatewideSchema(pl.BaseSchema):
    job_code = 'by_ethnicity_pa'
    date_updated_from_site = get_socrata_updated_date("https://data.pa.gov/api/odata/v4/u8hy-smfm")
    date_updated = fields.Date(dump_only=True, dump_to='date_updated', default=date_updated_from_site)
    ethnicity = fields.String(load_from='Ethnicity'.lower(), dump_to='ethnicity')
    partially_covered = fields.Integer(load_from='Partially Covered'.lower(), dump_to='partially_covered', allow_none=True)
    fully_covered = fields.Integer(load_from='Fully_Covered'.lower(), dump_to='fully_covered', allow_none=True)

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
        'resource_description': 'Archive of data from https://data.pa.gov/Health/COVID-19-Vaccinations-by-Residence-Current-County-/gcnb-epac',
    },
    {
        'job_code': ByAgeGroupSchema().job_code, # 'by_age_group'
        'source_type': 'http',
        'source_file': 'COVID-19_Vaccinations_by_Age_Group_Current_County_Health.csv',
        'source_full_url': 'https://data.pa.gov/api/views/niuh-2xe3/rows.csv?accessType=DOWNLOAD&api_foundry=true',
        'schema': ByAgeGroupSchema,
        'primary_key_fields': ['date_updated', 'age_group', 'coverage'],
        'destination': 'ckan',
        'destination_file': 'vaccinations_by_age_group.csv',
        'package': vaccinations_stats_archive_package_id,
        'resource_name': 'COVID-19 Vaccinations by Age Group Current County Health (archive)',
        'upload_method': 'upsert',
        'resource_description': 'Archive of data from https://data.pa.gov/Health/COVID-19-Vaccinations-by-Age-Group-Current-County-/niuh-2xe3',
    },
    {
        'job_code': ByAgeGroupStatewideSchema().job_code, # 'by_age_group_pa'
        'source_type': 'http',
        'source_file': 'COVID-19_Vaccinations_by_Age_Group_Current_Statewide_Health.csv',
        'source_full_url': 'https://data.pa.gov/api/views/xy2e-dqvt/rows.csv?accessType=DOWNLOAD&api_foundry=true',
        'schema': ByAgeGroupStatewideSchema,
        'primary_key_fields': ['date_updated', 'age_group'],
        'destination': 'ckan',
        'destination_file': 'vaccinations_by_age_group_pa.csv',
        'package': vaccinations_stats_archive_package_id,
        'resource_name': 'COVID-19 Vaccinations by Age Group Current Statewide Health (archive)',
        'upload_method': 'upsert',
        'resource_description': 'Archive of data from https://data.pa.gov/Health/COVID-19-Vaccinations-by-Age-Group-Current-Statewi/xy2e-dqvt'
    },
    {
        'job_code': ByRaceSchema().job_code, # 'by_race'
        'source_type': 'http',
        'source_file': 'COVID-19_Vaccinations_by_Race_Current_County_Health.csv',
        'source_full_url': 'https://data.pa.gov/api/views/x5z9-57ub/rows.csv?accessType=DOWNLOAD&api_foundry=true',
        'schema': ByRaceSchema,
        'primary_key_fields': ['date_updated', 'county', 'race', 'coverage'],
        'destination': 'ckan',
        'destination_file': 'vaccinations_by_race.csv',
        'package': vaccinations_stats_archive_package_id,
        'resource_name': 'COVID-19 Vaccinations by Race Current County Health (archive)',
        'upload_method': 'upsert',
        'resource_description': 'Archive of data from https://data.pa.gov/Health/COVID-19-Vaccinations-by-Race-Current-County-Healt/x5z9-57ub',
    },
    {
        'job_code': ByRaceStatewideSchema().job_code, # 'by_race_pa'
        'source_type': 'http',
        'source_file': 'COVID-19_Vaccinations_by_Race_Current_Statewide_Health.csv',
        'source_full_url': 'https://data.pa.gov/api/views/e384-bs7r/rows.csv?accessType=DOWNLOAD&api_foundry=true',
        'schema': ByRaceStatewideSchema,
        'primary_key_fields': ['date_updated', 'race'],
        'destination': 'ckan',
        'destination_file': 'vaccinations_by_race_pa.csv',
        'package': vaccinations_stats_archive_package_id,
        'resource_name': 'COVID-19 Vaccinations by Race Current Statewide Health (archive)',
        'upload_method': 'upsert',
        'resource_description': 'Archive of data from https://data.pa.gov/Health/COVID-19-Vaccinations-by-Race-Current-Statewide-He/e384-bs7r',
    },
    {
        'job_code': ByDayAndCountySchema().job_code, # 'by_day_and_county'
        'source_type': 'http',
        'source_file': 'COVID-19_Vaccinations_by_Day_by_County_of_Residence_Current_Health.csv',
        'source_full_url': 'https://data.pa.gov/api/views/bicw-3gwi/rows.csv?accessType=DOWNLOAD&api_foundry=true',
        'schema': ByDayAndCountySchema,
        'primary_key_fields': ['date', 'county'], # 'date_updated' is not needed here since 'date' is present.
        'destination': 'ckan',
        'destination_file': 'vaccinations_by_day_and_county.csv',
        'package': vaccinations_stats_archive_package_id,
        'resource_name': 'COVID-19 Vaccinations by Day by County of Residence Current Health (archive)',
        'upload_method': 'upsert',
        'resource_description': 'Archive of data from https://data.pa.gov/Health/COVID-19-Vaccinations-by-Day-by-County-of-Residenc/bicw-3gwi',
    },
    {
        'job_code': ByGenderSchema().job_code, # 'by_gender'
        'source_type': 'http',
        'source_file': 'COVID-19_Vaccinations_by_Gender_Current_County_Health.csv',
        'source_full_url': 'https://data.pa.gov/api/views/jweg-3ezy/rows.csv?accessType=DOWNLOAD&api_foundry=true',
        'schema': ByGenderSchema,
        'primary_key_fields': ['date_updated', 'gender', 'coverage'],
        'destination': 'ckan',
        'destination_file': 'vaccinations_by_gender.csv',
        'package': vaccinations_stats_archive_package_id,
        'resource_name': 'COVID-19 Vaccinations by Gender Current County Health (archive)',
        'upload_method': 'upsert',
        'resource_description': 'Archive of data from https://data.pa.gov/Health/COVID-19-Vaccinations-by-Gender-Current-County-Hea/jweg-3ezy',
    },
    {
        'job_code': ByGenderStatewideSchema().job_code, # 'by_gender_pa'
        'source_type': 'http',
        'source_file': 'COVID-19_Vaccinations_by_Gender_Current_Statewide_Health.csv',
        'source_full_url': 'https://data.pa.gov/api/views/id8t-dnk6/rows.csv?accessType=DOWNLOAD&api_foundry=true',
        'schema': ByGenderStatewideSchema,
        'primary_key_fields': ['date_updated', 'gender'],
        'destination': 'ckan',
        'destination_file': 'vaccinations_by_gender_pa.csv',
        'package': vaccinations_stats_archive_package_id,
        'resource_name': 'COVID-19 Vaccinations by Gender Current Statewide Health (archive)',
        'upload_method': 'upsert',
        'resource_description': 'Archive of data from https://data.pa.gov/Health/COVID-19-Vaccinations-by-Gender-Current-Statewide-/id8t-dnk6',
    },
    {
        'job_code': ByEthnicitySchema().job_code, # 'by_ethnicity'
        'source_type': 'http',
        'source_file': 'COVID-19_Vaccinations_by_Ethnicity_Current_County_Health.csv',
        'source_full_url': 'https://data.pa.gov/api/views/7ruj-m7k6/rows.csv?accessType=DOWNLOAD&api_foundry=true',
        'schema': ByEthnicitySchema,
        'primary_key_fields': ['date_updated', 'ethnicity', 'coverage'],
        'destination': 'ckan',
        'destination_file': 'vaccinations_by_ethnicity.csv',
        'package': vaccinations_stats_archive_package_id,
        'resource_name': 'COVID-19 Vaccinations by Ethnicity Current County Health (archive)',
        'upload_method': 'upsert',
        'resource_description': 'Archive of data from https://data.pa.gov/Health/COVID-19-Vaccinations-by-Ethnicity-Current-County-/7ruj-m7k6',
    },
    {
        'job_code': ByEthnicityStatewideSchema().job_code, # 'by_ethnicity_pa'
        'source_type': 'http',
        'source_file': 'COVID-19_Vaccinations_by_Ethnicity_Current_Statewide_Health.csv',
        'source_full_url': 'https://data.pa.gov/api/views/u8hy-smfm/rows.csv?accessType=DOWNLOAD&api_foundry=true',
        'schema': ByEthnicityStatewideSchema,
        'primary_key_fields': ['date_updated', 'ethnicity'],
        'destination': 'ckan',
        'destination_file': 'vaccinations_by_ethnicity_pa.csv',
        'package': vaccinations_stats_archive_package_id,
        'resource_name': 'COVID-19 Vaccinations by Ethnicity Current Statewide Health (archive)',
        'upload_method': 'upsert',
        'resource_description': 'Archive of data from https://data.pa.gov/Health/COVID-19-Vaccinations-by-Ethnicity-Current-Statewi/u8hy-smfm',
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
