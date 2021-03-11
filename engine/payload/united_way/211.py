import csv, requests, sys, traceback
from datetime import datetime, timedelta
from dateutil import parser
from pprint import pprint
import re, xmltodict, time
from collections import OrderedDict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import fetch_city_file, write_to_csv
from engine.time_field_util import get_extant_time_range
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID

from engine.parameters.referweb_credentials import referweb_API_key

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class Requests211Schema(pl.BaseSchema):
    contact_date = fields.Date(allow_none=False)
    gender = fields.String(allow_none=True)
    age_range = fields.String(allow_none=True)
    zip_code = fields.String(allow_none=True)
    county = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    needs_category = fields.String(allow_none=True)
    needs_code = fields.String(allow_none=True) # Sometimes this is empty in the source data.
    level_1_classification = fields.String(allow_none=True)
    level_2_classification = fields.String(allow_none=True)
    needs_met = fields.Boolean(allow_none=False)

    class Meta:
        ordered = True

    @pre_load
    def fix_zip_code(self, data):
        # Prevent ZIP+4 codes from being published.
        if data['zip_code'] is not None:
            if re.search('-', data['zip_code']) is not None:
                data['zip_code'] = data['zip_code'].split('-')[0]
            if len(data['zip_code']) >= 7:
                data['zip_code'] = data['zip_code'][:5]

def get_calls_in_date_range(first_date, last_date):
    # See "https://www.referweb.net/cdbws/service.asmx?op=Service_ZipCode_Date_Count"
    # for documentation.
    url = "https://www.referweb.net/cdbws/service.asmx/Service_ZipCode_Date_Count"
    # Currently no ZIP code filter is being used, which means that any
    # request received by the SW PA 211 line, regardless of the origin, will
    # show up.
    payload = {'api_key': referweb_API_key,
            'date1': first_date.isoformat(),
            'date2': last_date.isoformat()}
    r = requests.post(url, data = payload)
    d = xmltodict.parse(r.text)
    new_dataset = d.get('NewDataSet')
    if new_dataset is None:
        calls = []
        print(f"No calls found between {first_date} and {last_date}.")
    else:
        calls = d['NewDataSet']['Table']
    # Records look something like this:
    #OrderedDict([('Count', '1'),
    #             ('transaction_id', '254051'),
    #             ('TaxCode', 'BD-1800.2000'),
    #             ('Date_of_Call', '2020/11/01'),
    #             ('ZIP_Code', '15068'),
    #             ('Age_Run', '48'),
    #             ('Gender_Run', 'M'),
    #             ('Type', 'R'),
    #             ('Target_Tax_Code', 'BD-1800.2000'),
    #             ('SearchType', 'C')])

    return calls

def get_calls_for_date(full_date):
    return get_calls_in_date_range(full_date, full_date)

def bin_age(age):
    if age in ['']:
        return None
    else:
        try:
            age_integer = int(age)
        except ValueError:
            print(f"Hey, what are we supposed to do with an age that looks like {age}?")
            return age
        else:
            if 0 <= age_integer <= 5:
                return "0 to 5"
            elif 6 <= age_integer <= 17:
                return "6 to 17"
            elif 18 <= age_integer <= 24:
                return "18 to 24"
            elif 25 <= age_integer <= 44:
                return "25 to 44"
            elif 45 <= age_integer <= 64:
                return "45 to 64"
            elif 65 <= age_integer:
                return "65 and over"

def clean_calls(calls):
    for call in calls:
        if type(call) != OrderedDict:
            ic(call)
            print("Since the type of call is incorrect, the parsing must be bogus, so an empty list will be returned instead.")
            return []
        if 'Age_Run' in call:
            call['age_range'] = bin_age(call['Age_Run'])
            call.pop('Age_Run')
        else:
            call['age_range'] = None

        if 'TaxCode' in call and call['TaxCode'] is not None and len(call['TaxCode']) > 0 and call['TaxCode'][0] == 'Y':
            # These codes that start with 'Y' represent target populations
            # and not needs (and are also only a small fraction of all
            # requests, so they will be scrubbed.
            call['TaxCode'] = None

    return calls

def decode_zip_code(call, zip_code_lookup):
    if 'ZIP_Code' in call:
        call['zip_code'] = call['ZIP_Code']
        z = call['ZIP_Code']
        if z in zip_code_lookup:
            zip_properties = zip_code_lookup[z]
            call['state'] = zip_properties['state']
            call['county'] = zip_properties['county']

def initialize_zip_code_lookup():
    from engine.parameters.local_parameters import REFERENCE_DIR
    with open(f'{REFERENCE_DIR}211__county_and_state_by_zip_code.csv', 'r') as f:
    # Generated from Census files like a ZCTA relationship file.
        reader = csv.DictReader(f)
        rows = list(reader)
        zip_code_lookup = {}
        for row in rows:
            zip_code_lookup[row['ZCTA5']] = {'state': row['concat_state_name'],
                    'county': row['concat_county_name']}
        return zip_code_lookup

def initialize_taxonomy_lookups():
    from engine.parameters.local_parameters import REFERENCE_DIR
    description_by_code = {}
    with open(f'{REFERENCE_DIR}211__taxonomy_categories.csv', 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        for row in rows:
            description_by_code[row['code']] = row['service_term']

    with open(f'{REFERENCE_DIR}211__Taxonomy_SW_PA_code_freq.csv', 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        for row in rows:
            description_by_code[row['code']] = row['service_term']

    with open(f'{REFERENCE_DIR}211__Extra_Taxonomy_SW_PA.csv', 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        for row in rows:
            description_by_code[row['code']] = row['service_term']

    #with open('Target_Population_Codes.csv', 'r') as f:
    #    # YF-30000.2193 is being added as an alias for YF-3000.2193 since
    #    # it looks like a simple typo.
    #    reader = csv.DictReader(f)
    #    rows = list(reader)
    #    description_by_population_code = {}
    #    for row in rows:
    #        description_by_population_code[row['code']] = row['service_term']
    return description_by_code #, description_by_population_code

def decode_calls(calls):
    zip_code_lookup = initialize_zip_code_lookup()
    description_by_code = initialize_taxonomy_lookups()
    for call in calls:
        # Handle the Type field.
        if 'Type' in call:
            if call['Type'] == 'R': # Referral made
                call['needs_met'] = True
            elif call['Type'] == 'U': # Unmet needs
                call['needs_met'] = False
            else:
                call['needs_met'] = None
        else:
            call['needs_met'] = None

        if 'Gender_Run' in call:
            if call['Gender_Run'] == 'M':
                call['gender'] = 'M'
            elif call['Gender_Run'] == 'F': # Unmet needs
                call['gender'] = 'F'
            else: # D codes for "Declined to answer"
                call['gender'] = None
        else:
            call['gender'] = None

        decode_zip_code(call, zip_code_lookup)

        call['needs_category'] = description_by_code.get(call['TaxCode'], None)
        if call['TaxCode'] is not None and len(call['TaxCode']) > 0:
            call['level_1_classification'] = description_by_code.get(call['TaxCode'][:1], None)
            if len(call['TaxCode']) > 1:
                call['level_2_classification'] = description_by_code.get(call['TaxCode'][:2], None)

        #if 'Target_Tax_Code' in call:
        #    extra_codes = call['Target_Tax_Code'].split(' * ')[1:]
        #    population_categories = []
        #    for extra in extra_codes:
        #        if extra in description_by_population_code:
        #            population_categories.append(description_by_population_code[extra])
        #        else:
        #            ic(extra_codes)
        #            population_categories.append(f'UNKNOWN CODE ({extra})')
        #    call['population_categories'] = '|'.join(population_categories)

        call['needs_code'] = call.get('TaxCode', None)
        call['contact_date'] = call.get('Date_of_Call', None)
    return calls

def pull_211_requests_and_save_to_file(job, **kwparameters):
    """Examine the extant resource to find the last date included in the
    published data."""
    if not kwparameters['use_local_input_file']:
        last_datetime = (datetime.now() - timedelta(days=1))
        _, datetime_of_latest_record = get_extant_time_range(job, **kwparameters)
        if datetime_of_latest_record is None:
            first_datetime = datetime(2020, 1, 1)
        else:
            first_datetime = datetime_of_latest_record + timedelta(days = 1)

        number_of_days = (last_datetime - first_datetime).days + 1
        dates = [last_datetime.date() - timedelta(days=x) for x in range(number_of_days)][::-1]
        calls = []
        for target_date in dates:
            print(f"Working on {target_date}")
            new_calls = get_calls_for_date(target_date)
            if type(new_calls) in [list]:
                calls += new_calls
            else:
                print(f"What is this if not a list of calls?")
                ic(new_calls)
            if target_date != dates[-1]:
                time.sleep(10)

        #filename = 'raw-211-requests-old-API.csv'
        #fields_to_write = ['transaction_id', 'TaxCode',
        #        'Date_of_Call', 'ZIP_Code', 'Gender_Run',
        #        'Type', 'Target_Tax_Code', 'SearchType']
        #write_to_csv(filename, calls, fields_to_write)

        cleaned_calls = clean_calls(calls)
        decoded_calls = decode_calls(cleaned_calls)
        filename = job.target
        fields_to_write = [
                'contact_date',
                'gender',
                'age_range',
                'zip_code', 'county', 'state',
                'needs_category',
                'needs_code',
                'level_1_classification', 'level_2_classification',
                'needs_met',
                ]
        write_to_csv(filename, decoded_calls, fields_to_write)


requests_211_package_id = "" # Production version of 211 Requests package
requests_211_package_id = "4e5ae9e1-36b8-45b6-98ad-6b50f4be099c" # Test version of 211 package

job_dicts = [
    {
        'job_code': '211',
        'source_type': 'local',
        'source_dir': '',
        'source_file': '211.csv',
        'encoding': 'utf-8-sig',
        'custom_processing': pull_211_requests_and_save_to_file,
        'schema': Requests211Schema,
        'time_field': 'contact_date',
        'always_wipe_data': False,
        #'primary_key_fields': None, # There is an ID which could be hashed,
        # but we opted to use the date field as a guideline and just do
        # inserts atomically (all requests from the same date should be 
        # inserted in one API request). This way, the date field can be
        # relied on to tell us what date range needs to be requested to 
        # fill in the gap between the last published date and the present
        # (yesterday's requests).
        'upload_method': 'insert',
        'package': requests_211_package_id,
        'resource_name': '2-1-1 Requests',
    },
]
