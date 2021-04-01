import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import fetch_city_file
from engine.notify import send_to_slack
from engine.scraping_util import scrape_nth_link
from engine.parameters.local_parameters import SOURCE_DIR, REFERENCE_DIR

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa


class NursingHomeVaccinationsSchema(pl.BaseSchema):
    job_code = 'vaccinations'
#    survey_id = fields.Integer(load_from='SURVEY_ID_'.lower(), dump_to='survey_id_')
    facility_n = fields.String(load_from='FACILITY_N'.lower(), dump_to='facility_name')
    facility_i = fields.String(load_from='FACILITY_I'.lower(), dump_to='facility_id')
    street = fields.String(load_from='STREET'.lower(), dump_to='street')
    city_or_bo = fields.String(load_from='CITY_OR_BO'.lower(), dump_to='city')
    zip_code = fields.String(load_from='ZIP_CODE'.lower(), dump_to='zip_code')
#    zip_code_e = fields.String(load_from='ZIP_CODE_E'.lower(), dump_to='zip_code_e')
    longitude = fields.Float(load_from='LONGITUDE'.lower(), dump_to='longitude')
    latitude = fields.Float(load_from='LATITUDE'.lower(), dump_to='latitude')
    facility_u = fields.String(load_from='FACILITY_U'.lower(), dump_to='facility_url')
#    geocoding_ = fields.Integer(load_from='GEOCODING_'.lower(), dump_to='geocoding_')
    area_code = fields.Integer(load_from='AREA_CODE'.lower(), dump_to='area_code')
    telephone_ = fields.Integer(load_from='TELEPHONE_'.lower(), dump_to='telephone')
    contact_na = fields.String(load_from='CONTACT_NA'.lower(), dump_to='contact_name')
    contact_nu = fields.Integer(load_from='CONTACT_NU'.lower(), dump_to='contact_phone')
    contact_fa = fields.String(load_from='CONTACT_FA'.lower(), dump_to='contact_fax')
#    contact_em = fields.String(load_from='CONTACT_EM'.lower(), dump_to='contact_em')
    #lat = fields.Float(load_from='LAT'.lower(), dump_to='lat')
    #lng = fields.Float(load_from='LNG'.lower(), dump_to='lng')
    all_beds = fields.Integer(load_from='ALL_BEDS'.lower(), dump_to='all_beds', allow_none=True)
    current_census = fields.Integer(load_from='CURRENT_CENSUS'.lower(), dump_to='current_census', allow_none=True)
    resident_cases_to_display = fields.String(load_from='Resident_Cases_to_Display'.lower(), dump_to='resident_cases_to_display', allow_none=True)
    resident_deaths_to_display = fields.String(load_from='Resident_Deaths_to_Display'.lower(), dump_to='resident_deaths_to_display', allow_none=True)
    staff_cases_to_display = fields.String(load_from='Staff_Cases_to_Display'.lower(), dump_to='staff_cases_to_display', allow_none=True)
    doh_data_last_updated = fields.String(load_from='DOH_Data_Last_Updated'.lower(), dump_to='doh_data_last_updated')
    clinicdt1 = fields.String(load_from='clinicdt1'.lower(), dump_to='clinicdt1', allow_none=True)
    clinicdt2 = fields.String(load_from='clinicdt2'.lower(), dump_to='clinicdt2', allow_none=True)
    clinicdt3 = fields.Date(load_from='clinicdt3'.lower(), dump_to='clinicdt3', allow_none=True)
    clinicdt4 = fields.Date(load_from='clinicdt4'.lower(), dump_to='clinicdt4', allow_none=True)
    clinicdt5 = fields.Date(load_from='clinicdt5'.lower(), dump_to='clinicdt5', allow_none=True)
    clinicdt6 = fields.String(load_from='clinicdt6'.lower(), dump_to='clinicdt6', allow_none=True)
    clinicdt7 = fields.String(load_from='clinicdt7'.lower(), dump_to='clinicdt7', allow_none=True)
    clinicdt8 = fields.String(load_from='clinicdt8'.lower(), dump_to='clinicdt8', allow_none=True)
    clinicdt9 = fields.String(load_from='clinicdt9'.lower(), dump_to='clinicdt9', allow_none=True)
    clinicdt10 = fields.String(load_from='clinicdt10'.lower(), dump_to='clinicdt10', allow_none=True)
    #facility = fields.String(load_from='Facility'.lower(), dump_to='facility', allow_none=True)
    #clinicd6 = fields.String(load_from='clinicd6'.lower(), dump_to='clinicd6', allow_none=True)
    fpp_data_last_updated = fields.String(load_from='FPP_Data_Last_Updated'.lower(), dump_to='fpp_data_last_updated')

    class Meta:
        ordered = True

    @pre_load
    def fix_integers(self, data):
        fs = ['all_beds', 'current_census']
        for f in fs:
            if f in data and data[f] is not None:
                data[f] = int(float(data[f]))


#    @pre_load
#    def fix_dates(self, data):
#        """Marshmallow doesn't know how to handle a datetime as input. It can only
#        take strings that represent datetimes and convert them to datetimes.:
#        https://github.com/marshmallow-code/marshmallow/issues/656
#        So this is a workaround.
#        """
#        date_fields = ['maturity_date', 'initial_endorsement_date']
#        for f in date_fields:
#            if data[f] is not None:
#                data[f] = parser.parse(data[f]).date().isoformat()
#
#    @post_load
#    def handle_weird_field_name(self, data):
#        data['program_category'] = data['soa_category_sub_category']


# dfg

### PASTE YOUR CODE HERE ###

nursing_home_vaccinations_package_id = 'f89d5dd8-5dd3-46a3-9a17-8671f26153ae'

job_dicts = [
    {
        'job_code': NursingHomeVaccinationsSchema().job_code, #'vaccinations'
        'source_type': 'local', #http',
        'source_file': 'COVID19_Vaccine_Data_LTCF.csv',  # You could change this to CovidData(f'{REFERENCE_DIR}PASDA.csv').writeToFile()
        #'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/comp/rpts/mfh/mf_f47', 'xlsx', 1, 2, 'FHA'),
        #'encoding': 'binary',
        'schema': NursingHomeVaccinationsSchema,
        #'always_wipe_data': True,
        'primary_key_fields': ['facility_i'],
        'destination': 'ckan',
        'package': nursing_home_vaccinations_package_id,
        'resource_name': 'Change this to a far more reasonable name',
        'upload_method': 'upsert',
        'resource_description': f'Derived from https://www.hud.gov/program_offices/housing/comp/rpts/mfh/mf_f47',
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
