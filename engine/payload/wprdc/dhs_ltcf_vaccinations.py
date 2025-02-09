import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import local_file_and_dir
from engine.notify import send_to_slack
from engine.scraping_util import scrape_nth_link
from engine.parameters.local_parameters import SOURCE_DIR, REFERENCE_DIR

from bs4 import BeautifulSoup
from openpyxl import load_workbook
from fuzzywuzzy import fuzz
from engine.payload.wprdc.ltcf_vaccinations import sheet_to_csv

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def write_to_csv(filename, list_of_dicts, keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

class DHSNursingHomeVaccinationsSchema(pl.BaseSchema):
    job_code = 'nursing_homes'
    #survey_id_ = fields.Integer(load_from='SURVEY_ID_'.lower(), dump_to='survey_id_')
    facility_name = fields.String(load_from='Facility Name'.lower(), dump_to='facility_name', allow_none=True)
    type_of_service = fields.String(load_from='Type of Service'.lower(), dump_to='type_of_service')
    county = fields.String(load_from='County'.lower(), dump_to='county', allow_none=True)
    license = fields.String(load_from='License'.lower(), dump_to='license', allow_none=True)
    covid_19_positive_residents = fields.String(load_from='COVID-19 Positive Residents'.lower(), dump_to='resident_cases_to_display', allow_none=True)
    covid_19_resident_deaths = fields.String(load_from='COVID-19 Resident Deaths'.lower(), dump_to='resident_deaths_to_display', allow_none=True)
    covid_19_positive_staff = fields.String(load_from='COVID-19 Positive Staff'.lower(), dump_to='staff_cases_to_display', allow_none=True)
    dhs_data_last_updated = fields.Date(load_from='DHS_Data_Last_Updated'.lower(), dump_to='dhs_data_last_updated', allow_none=True)
    facility_name_fpp = fields.String(load_from='Facility Name FPP'.lower(), dump_to='facility_name_fpp', allow_none=True)
    federal_pharmacy_partner = fields.String(load_from='Federal Pharmacy Partner'.lower(), dump_to='federal_pharmacy_partner', allow_none=True)
    first_clinic_date = fields.Date(load_from='First Clinic Date '.lower(), dump_to='first_clinic_date', allow_none=True)
    second_clinic_date = fields.Date(load_from='Second Clinic Date'.lower(), dump_to='second_clinic_date', allow_none=True)
    third_clinic_date = fields.Date(load_from='Third Clinic Date'.lower(), dump_to='third_clinic_date', allow_none=True)
    fourth_clinic_date = fields.Date(load_from='Fourth Clinic Date'.lower(), dump_to='fourth_clinic_date', allow_none=True)
    fifth_clinic_date = fields.Date(load_from='Fifth Clinic Date'.lower(), dump_to='fifth_clinic_date', allow_none=True)
    sixth_clinic_date = fields.Date(load_from='Sixth Clinic Date'.lower(), dump_to='sixth_clinic_date', allow_none=True)
    seventh_clinic_date = fields.Date(load_from='Seventh Clinic Date'.lower(), dump_to='seventh_clinic_date', allow_none=True)
    eighth_clinic_date = fields.Date(load_from='Eighth Clinic Date'.lower(), dump_to='eighth_clinic_date', allow_none=True)
    ninth_clinic_date = fields.Date(load_from='Ninth Clinic Date'.lower(), dump_to='ninth_clinic_date', allow_none=True)
    tenth_clinic_date = fields.Date(load_from='Tenth Clinic Date'.lower(), dump_to='tenth_clinic_date', allow_none=True)
    total_doses_administered = fields.Integer(load_from='Total Doses Administered'.lower(), dump_to='total_doses_administered', allow_none=True)
    first_doses_administered = fields.Integer(load_from='First Doses Administered'.lower(), dump_to='first_doses_administered', allow_none=True)
    second_doses_adminstered = fields.Integer(load_from='Second Doses Adminstered'.lower(), dump_to='second_doses_adminstered', allow_none=True)
    total_resident_doses_administered = fields.Integer(load_from='Total Resident Doses Administered'.lower(), dump_to='total_resident_doses_administered', allow_none=True)
    total_staff_doses_administered = fields.Integer(load_from='Total Staff Doses Administered'.lower(), dump_to='total_staff_doses_administered', allow_none=True)
####
    #fpp_data_last_updated = fields.String(load_from='FPP_Data_Last_Updated'.lower(), dump_to='fpp_data_last_updated')

    class Meta:
        ordered = True

# dfg

class CovidData:
    def __init__(self, dhsData, fppData, soup):
        self.dhsdf = list(csv.DictReader(open(dhsData, 'r')))
        self.fppdf = list(csv.DictReader(open(fppData, 'r')))
        self.soup = soup
        self.fields = set()
        doh_dates = []
        for link in self.soup.find_all('a'):
            url = link.get('href', 'No Link Found')
            if re.search('xlsx', url) is not None:
                doh_dates.append(link.next_sibling)

        matches = re.search('(\d{1,2}/\d{1,2}/\d\d\d\d)', doh_dates[1])
        self.dhs_last_updated = matches.group(1)
        self.addFPPData()

    def addFPPData(self):
        print("Adding FPP Data\n")
        for index, fpp in enumerate(self.fppdf):
            tempStr = str(fpp['Long Term Care Facility Name'])

            for i, facility in enumerate(self.dhsdf):
                facility['COVID-19 Positive Residents'] = facility[' COVID-19 Positive Residents'] # Dealing with
                # an unnecessary leading space.
                Str1 = str(facility['Facility Name'])
                ratio = fuzz.ratio(Str1.lower(), tempStr.lower())
                if (ratio >= 90):
                    print(Str1.lower(), " ", tempStr.lower())
                    facility['Facility Name FPP'] = fpp['Long Term Care Facility Name']
                    facility['Federal Pharmacy Partner'] = fpp['Federal Pharmacy Partner']
                    facility['First Clinic Date '] = fpp['1st Clinic Date']
                    facility['Second Clinic Date'] = fpp['2nd Clinic Date']
                    facility['Third Clinic Date'] = fpp['3rd Clinic Date']
                    facility['Fourth Clinic Date'] = fpp['4th Clinic Date']
                    facility['Fifth Clinic Date'] = fpp['5th Clinic Date']
                    facility['Sixth Clinic Date'] = fpp['6th Clinic Date']
                    facility['Seventh Clinic Date'] = fpp['7th Clinic Date']
                    facility['Eighth Clinic Date'] = fpp['8th Clinic Date']
                    facility['Ninth Clinic Date'] = fpp['9th Clinic Date']
                    facility['Tenth Clinic Date'] = fpp['10th Clinic Date']
                    facility['Total Doses Administered'] = fpp['Total Doses Administered']
                    facility['First Doses Administered'] = fpp['First Doses Administered']
                    facility['Second Doses Adminstered'] = fpp['Second Doses Adminstered']
                    facility['Total Resident Doses Administered'] = fpp['Total Resident Doses Administered']
                    facility['Total Staff Doses Administered'] = fpp['Total Staff Doses Administered']
                    self.fields = set(facility.keys()) | self.fields
                    break


        print("Done Adding FPP Data")

    def writeToFile(self, output_path):
        #self.ltcfdf.drop('GEOCODING_', inplace=True, axis=1) # We don't need to drop any columns
        #self.ltcfdf.drop('LAT', inplace=True, axis=1) # since the ETL schema will just pluck out
        #self.ltcfdf.drop('LNG', inplace=True, axis=1) # the columns it needs and ignore the others.
        write_to_csv(output_path, self.dhsdf, list(self.fields))
        print(f"CovidData is writing to the output file at {output_path}")

def get_raw_data_and_save_to_local_csv_file(jobject, **kwparameters):
    if not kwparameters['use_local_input_file']:
        r = requests.get("https://www.health.pa.gov/topics/disease/coronavirus/Pages/LTCF-Data.aspx")
        soup = BeautifulSoup(r.text, 'html.parser')
        doc_urls = []
        dhs_dates = []
        for link in soup.find_all('a'):
            url = link.get('href', 'No Link Found')
            if re.search('xlsx', url) is not None:
                dhs_dates.append(link.next_sibling)
                doc_urls.append(url)

        assert len(doc_urls) == 3  # Verify that the web site only lists threeXSLX files.
        dhs_last_updated = dhs_dates[1]
        fullurl = "https://www.health.pa.gov" + doc_urls[1]

        r = requests.get(fullurl)
        open('DHS_Data.xlsx', 'wb').write(r.content)

        r = requests.get("https://data.pa.gov/api/views/iwiy-rwzp/rows.csv?accessType=DOWNLOAD&api_foundry=true")
        open('FPP_Data.csv', 'wb').write(r.content)

        wb2 = load_workbook('DHS_Data.xlsx')

        ws2 = wb2['Master List']
        sheet_to_csv(ws2, 'DHS_Data.csv', 2)

        a = CovidData('DHS_Data.csv', 'FPP_Data.csv', soup)
        a.writeToFile(jobject.target)

vaccinations_package_id = 'b0ecab9a-d056-4855-9992-bdfca608632b'

job_dicts = [
    {
        'job_code': DHSNursingHomeVaccinationsSchema().job_code, #'vaccinations'
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'COVID-19_Vaccine_Data_LTCF.csv',
        'custom_processing': get_raw_data_and_save_to_local_csv_file,
        'schema': DHSNursingHomeVaccinationsSchema,
        'filters': [['type_of_service', '!=', 'STATE TOTAL']],
        #'always_wipe_data': True,
        'primary_key_fields': ['license'],
        'destination': 'ckan',
        'destination_file': 'DHS_COVID-19_Vaccine_Data_LTCF.csv',
        'package': vaccinations_package_id,
        'resource_name': 'DHS Long-Term Care Facilites COVID-19 Cases and Vaccinations',
        'upload_method': 'upsert',
        'resource_description': f'Derived from "https://data.pa.gov/api/views/iwiy-rwzp/rows.csv?accessType=DOWNLOAD&api_foundry=true" and "https://www.health.pa.gov/topics/disease/coronavirus/Pages/LTCF-Data.aspx" ',
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
