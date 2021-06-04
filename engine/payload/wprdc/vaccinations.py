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
import pandas as pd
from fuzzywuzzy import fuzz

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def write_to_csv(filename, list_of_dicts, keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

class NursingHomeVaccinationsSchema(pl.BaseSchema):
    job_code = 'nursing_homes'
    #survey_id_ = fields.Integer(load_from='SURVEY_ID_'.lower(), dump_to='survey_id_')
    facility_n = fields.String(load_from='FACILITY_N'.lower(), dump_to='facility_name')
    facility_i = fields.String(load_from='FACILITY_I'.lower(), dump_to='facility_id')
    street = fields.String(load_from='STREET'.lower(), dump_to='street')
    city_or_bo = fields.String(load_from='CITY_OR_BO'.lower(), dump_to='city')
    zip_code = fields.String(load_from='ZIP_CODE'.lower(), dump_to='zip_code')
    #zip_code_e = fields.String(load_from='ZIP_CODE_E'.lower(), dump_to='zip_code_e')
    latitude = fields.Float(load_from='LATITUDE'.lower(), dump_to='latitude')
    longitude = fields.Float(load_from='LONGITUDE'.lower(), dump_to='longitude')
    facility_u = fields.String(load_from='FACILITY_U'.lower(), dump_to='facility_url')
    #geocoding_ = fields.Integer(load_from='GEOCODING_'.lower(), dump_to='geocoding_')
    area_code = fields.String(load_from='AREA_CODE'.lower(), dump_to='area_code')
    telephone_ = fields.String(load_from='TELEPHONE_'.lower(), dump_to='telephone')
    contact_na = fields.String(load_from='CONTACT_NA'.lower(), dump_to='contact_name')
    contact_nu = fields.String(load_from='CONTACT_NU'.lower(), dump_to='contact_phone')
    contact_fa = fields.String(load_from='CONTACT_FA'.lower(), dump_to='contact_fax')
    #contact_em = fields.String(load_from='CONTACT_EM'.lower(), dump_to='contact_em')
    #lat = fields.Float(load_from='LAT'.lower(), dump_to='lat')
    #lng = fields.Float(load_from='LNG'.lower(), dump_to='long')
    all_beds = fields.Integer(load_from='ALL_BEDS'.lower(), dump_to='all_beds', allow_none=True)
    current_census = fields.Integer(load_from='CURRENT_CENSUS'.lower(), dump_to='current_census', allow_none=True)
    resident_cases_to_display = fields.String(load_from='Resident_Cases_to_Display'.lower(), dump_to='resident_cases_to_display', allow_none=True)
    resident_deaths_to_display = fields.String(load_from='Resident_Deaths_to_Display'.lower(), dump_to='resident_deaths_to_display', allow_none=True)
    staff_cases_to_display = fields.String(load_from='Staff_Cases_to_Display'.lower(), dump_to='staff_cases_to_display', allow_none=True)
    doh_data_last_updated = fields.Date(load_from='DOH_Data_Last_Updated'.lower(), dump_to='doh_data_last_updated', allow_none=True)
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
    def __init__(self, pasdaData, dohData, fppData, soup):
        self.ltcfdf = list(csv.DictReader(open(pasdaData, 'r')))
        self.dohdf = list(csv.DictReader(open(dohData, 'r')))
        self.fppdf = list(csv.DictReader(open(fppData, 'r')))
        self.soup = soup
        self.fields = set()
        doh_dates = []
        for link in self.soup.find_all('a'):
            url = link.get('href', 'No Link Found')
            if re.search('xlsx', url) is not None:
                doh_dates.append(link.next_sibling)

        doh_last_updated = doh_dates[0]
        a = doh_last_updated.split()
        self.doh_last_updated = a[1]
        if dohData is not None:
            self.addDOHData()
        self.addFPPData()

    def addDOHData(self):
        print("\nAdding DOH COVID Data\n")

        for facility in self.ltcfdf:
            for index, doh_row in enumerate(self.dohdf):
                if doh_row['FACID'] == facility['FACILITY_I']:
                    facility['ALL_BEDS'] = doh_row['ALL_BEDS']
                    facility['CURRENT_CENSUS'] = doh_row['CURRENT_CENSUS']
                    facility['Resident_Cases_to_Display'] = doh_row['Resident Cases to Display']
                    facility['Resident_Deaths_to_Display'] = doh_row['Resident Deaths to Display']
                    facility['Staff_Cases_to_Display'] = doh_row['Staff Cases to Display']
                    facility['DOH_Data_Last_Updated'] = self.doh_last_updated

        print("Done Adding DOH COVID Data\n")
        
    def addFPPData(self):
        print("Adding FPP Data\n")
        for index, fpp in enumerate(self.fppdf):
            tempStr = str(fpp['Long Term Care Facility Name'])
    
            for i, facility in enumerate(self.ltcfdf):
                Str1 = str(facility['FACILITY_N'])
                ratio = fuzz.ratio(Str1.lower(), tempStr.lower())
                if (ratio >= 90):
                    facility['Facility Name FPP'] = fpp['Long Term Care Facility Name']
                    facility['Federal Pharmacy Partner'] = fpp['Federal Pharmacy Partner']
                    facility['First Clinic Date '] =  fpp['1st Clinic Date']
                    facility['Second Clinic Date'] =  fpp['2nd Clinic Date']
                    facility['Third Clinic Date'] =   fpp['3rd Clinic Date']
                    facility['Fourth Clinic Date'] =  fpp['4th Clinic Date']
                    facility['Fifth Clinic Date'] =   fpp['5th Clinic Date']
                    facility['Sixth Clinic Date'] =   fpp['6th Clinic Date']
                    facility['Seventh Clinic Date'] = fpp['7th Clinic Date']
                    facility['Eighth Clinic Date'] =  fpp['8th Clinic Date']
                    facility['Ninth Clinic Date'] =   fpp['9th Clinic Date']
                    facility['Tenth Clinic Date'] =   fpp['10th Clinic Date']
                    facility['Total Doses Administered'] = fpp['Total Doses Administered']
                    facility['First Doses Administered'] = fpp['First Doses Administered']
                    facility['Second Doses Adminstered'] = fpp['Second Doses Adminstered']
                    facility['Total Resident Doses Administered'] = fpp['Total Resident Doses Administered']
                    facility['Total Staff Doses Administered'] = fpp['Total Staff Doses Administered']
                    self.fields = set(facility.keys()) | self.fields
                    break
    
        #  self.ltcfdf['FPP_Data_Last_Updated'] = self.fpp_last_updated
        print("Done Adding FPP Data")

    def writeToFile(self, output_path):
        #self.ltcfdf.drop('GEOCODING_', inplace=True, axis=1) # We don't need to drop any columns
        #self.ltcfdf.drop('LAT', inplace=True, axis=1) # since the ETL schema will just pluck out
        #self.ltcfdf.drop('LNG', inplace=True, axis=1) # the columns it needs and ignore the others.
        write_to_csv(output_path, self.ltcfdf, list(self.fields))
        print("output file: "),
        print(output_path)

def get_raw_data_and_save_to_local_csv_file(jobject, **kwparameters):
    if not kwparameters['use_local_input_file']:
        r = requests.get("https://www.health.pa.gov/topics/disease/coronavirus/Pages/LTCF-Data.aspx")
        soup = BeautifulSoup(r.text, 'html.parser')

        doc_urls = []
        doh_dates = []
        for link in soup.find_all('a'):
                url = link.get('href', 'No Link Found')
                if re.search('xlsx', url) is not None:
                        doh_dates.append(link.next_sibling)
                        doc_urls.append(url)

        assert len(doc_urls) == 4  # Verify that the web site only lists four XSLX files.
        fullurl = "https://www.health.pa.gov" + doc_urls[0]

        r = requests.get("https://data.pa.gov/api/views/iwiy-rwzp/rows.csv?accessType=DOWNLOAD&api_foundry=true")
        open('FPP_Data.csv', 'wb').write(r.content)

        r = requests.get("https://www.pasda.psu.edu/spreadsheet/DOH_NursingHome201806.csv")
        open('PASDA_LTCF.csv', 'wb').write(r.content)

        r = requests.get(fullurl)
        open('DOH_Data.xlsx', 'wb').write(r.content)
        wb = load_workbook('DOH_Data.xlsx')
        ws = wb['Sheet1']
        df = pd.DataFrame(ws.values)
        df.rename(columns={0: u"FACID", 1: u"NAME", 2: u"CITY", 3: u"COUNTY", 4: u"ALL_BEDS", 5: u"CURRENT_CENSUS",
                                           6: u"Resident Cases to Display",
                                           7: u"Resident Deaths to Display", 8: u"Staff Cases to Display"}, inplace=True)
        df = df.iloc[1:]
        df.to_csv("DOH_Data.csv", encoding='utf-8')

        a = CovidData("PASDA_LTCF.csv", 'DOH_Data.csv', 'FPP_Data.csv', soup)
        a.writeToFile(jobject.target)


vaccinations_package_id = 'f89d5dd8-5dd3-46a3-9a17-8671f26153ae'

job_dicts = [
    {
        'job_code': NursingHomeVaccinationsSchema().job_code, #'vaccinations'
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'COVID-19_Vaccine_Data_LTCF.csv',
        'custom_processing': get_raw_data_and_save_to_local_csv_file,
        'schema': NursingHomeVaccinationsSchema,
        #'always_wipe_data': True,
        'primary_key_fields': ['facility_i'],
        'destination': 'ckan',
        'package': vaccinations_package_id,
        'resource_name': ' Long-Term Care Facilites COVID-19 Cases and Vaccinations Data',
        'upload_method': 'upsert',
        'resource_description': f'Derived from https://data.pa.gov/api/views/iwiy-rwzp/rows.csv?accessType=DOWNLOAD&api_foundry=true and https://www.pasda.psu.edu/spreadsheet/DOH_NursingHome201806.csv',
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
