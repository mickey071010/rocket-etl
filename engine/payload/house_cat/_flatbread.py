import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import fetch_city_file
from engine.arcgis_util import get_arcgis_data_url
from engine.notify import send_to_slack
from engine.scraping_util import scrape_nth_link
from engine.parameters.local_parameters import SOURCE_DIR

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa


# Network functions #
import socket

def get_an_ip_for_host(host):
    try:
        ips = socket.gethostbyname_ex(host)
    except socket.gaierror:
        return None
    return ips[0]

def site_is_up(site):
    """At present, geo.wprdc.org is only accessible from inside the Pitt network.
    Therefore, it's helpful to run this function to check whether this script
    should even try to do any geocoding."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2) # Add a two-second timeout to avoid interminable waiting.
    ip = get_an_ip_for_host(site)
    if ip is None:
        print("Unable to get an IP address for that host.")
        return False
    result = sock.connect_ex((ip,80))
    if result == 0:
        print('port OPEN')
        return True
    else:
        print('port CLOSED, connect_ex returned: '+str(result))
        return False

# End network functions #

class MultifamilyInsuredMortgagesSchema(pl.BaseSchema):
    job_code = 'mf_mortgages'
    hud_property_name = fields.String(load_from='property_name', dump_to='hud_property_name')
    property_street_address = fields.String(load_from='property_street', dump_to='property_street_address', allow_none=True)
    fha_loan_id = fields.String(load_from='hud_project_number', dump_to='fha_loan_id')
    city = fields.String(load_from='PROPERTY_CITY'.lower(), dump_to='city')
    state = fields.String(load_from='PROPERTY_STATE'.lower(), dump_to='state')
    zip_code = fields.String(load_from='PROPERTY_ZIP'.lower(), dump_to='zip_code')
    units = fields.Integer(load_from='UNITS'.lower(), dump_to='units')
    initial_endorsement_date = fields.Date(load_from='INITIAL ENDORSEMENT DATE'.lower(), dump_to='initial_endorsement_date')
    #final_endorsement_date = fields.String(load_from='FINAL ENDORSEMENT DATE'.lower(), allow_none=True)
    original_mortgage_amount = fields.Integer(load_from='ORIGINAL MORTGAGE AMOUNT'.lower(), dump_to='original_mortgage_amount')
    #first_payment_date = fields.String(load_from='FIRST PAYMENT DATE'.lower())
    maturity_date = fields.Date(load_from='MATURITY DATE'.lower(), dump_to='maturity_date')
    term_in_months = fields.Integer(load_from='TERM IN MONTHS'.lower(), dump_to='term_in_months')
    interest_rate = fields.Float(load_from='INTEREST RATE'.lower(), dump_to='interest_rate')
    current_principal_and_interest = fields.Float(load_from='CURRENT PRINCIPAL AND INTEREST'.lower(), dump_to='current_principal_and_interest')
    amoritized_principal_balance = fields.Float(load_from='AMORITIZED PRINCIPAL BALANCE'.lower(), dump_to='amoritized_principal_balance')
    holder_name = fields.String(load_from='HOLDER NAME'.lower(), dump_to='holder_name')
    holder_city = fields.String(load_from='HOLDER CITY'.lower(), dump_to='holder_city')
    holder_state = fields.String(load_from='HOLDER STATE'.lower(), dump_to='holder_state')
    servicer_name = fields.String(load_from='SERVICER NAME'.lower(), dump_to='servicer_name')
    servicer_city = fields.String(load_from='SERVICER CITY'.lower(), dump_to='servicer_city')
    servicer_state = fields.String(load_from='SERVICER STATE'.lower(), dump_to='servicer_state')
    section_of_act_code = fields.String(load_from='SECTION OF ACT CODE'.lower(), dump_to='section_of_act_code')
    soa_category_sub_category = fields.String(load_from='SOA_CATEGORY/SUB_CATEGORY'.lower(), dump_to='program_category')
    #te = fields.String(load_from='TE'.lower(), allow_none=True)
    #tc = fields.String(load_from='TC'.lower(), allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_dates(self, data):
        """Marshmallow doesn't know how to handle a datetime as input. It can only
        take strings that represent datetimes and convert them to datetimes.:
        https://github.com/marshmallow-code/marshmallow/issues/656
        So this is a workaround.
        """
        date_fields = ['maturity_date', 'initial_endorsement_date']
        for f in date_fields:
            if data[f] is not None:
                data[f] = data[f].date().isoformat()

    @post_load
    def handle_weird_field_name(self, data):
        data['program_category'] = data['soa_category_sub_category']


class MultifamilyProductionInitialCommitmentSchema(pl.BaseSchema):
    job_code = 'mf_init_commit'
    hud_property_name = fields.String(load_from='project_name', dump_to='hud_property_name')
    fha_loan_id = fields.String(load_from='fha_number', dump_to='fha_loan_id')
    city = fields.String(load_from='PROJECT_CITY'.lower(), dump_to='city')
    state = fields.String(load_from='PROJECT_STATE'.lower(), dump_to='state')
    date_of_initial_endorsement = fields.Date(load_from='Date of Initial Endorsement'.lower(), dump_to='initial_endorsement_date')
    mortgage_at_initial_endorsement = fields.Integer(load_from='Mortgage at Initial Endorsement'.lower(), dump_to='original_mortgage_amount')
    program_category = fields.String(load_from='Program Category'.lower())
    unit_or_bed_count = fields.Integer(load_from='Unit or Bed Count'.lower(), dump_to='units')
    basic_fha_risk_share_or_other = fields.String(load_from='basic_fha,_risk_share,_or_other', dump_to='basic_fha_risk_share_or_other')
    current_status = fields.String(load_from='Current Status'.lower(), dump_to='current_status_of_loan')
    date_of_firm_issue = fields.Date(load_from='Date of Firm Issue'.lower(), dump_to='date_of_firm_issue')
    firm_commitment_lender = fields.String(load_from='Firm Commitment Lender'.lower(), dump_to='firm_commitment_lender')
    final_endorsement_lender = fields.String(load_from='Final Endorsement Lender'.lower(), dump_to='holder_name', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_dates(self, data):
        """Marshmallow doesn't know how to handle a datetime as input. It can only
        take strings that represent datetimes and convert them to datetimes.:
        https://github.com/marshmallow-code/marshmallow/issues/656
        So this is a workaround.
        """
        date_fields = ['date_of_firm_issue', 'date_of_initial_endorsement']
        for f in date_fields:
            data[f] = data[f].date().isoformat()

class HousingInspectionScoresSchema(pl.BaseSchema):
    job_code = 'housing_inspections'
    state = fields.String(load_from='STATE_NAME'.lower(), dump_to='state')
    latitude = fields.Float(load_from='LATITUDE'.lower(), allow_none=True)
    longitude = fields.Float(load_from='LONGITUDE'.lower(), allow_none=True)
    county_fips_code = fields.String(load_from='COUNTY_CODE'.lower(), dump_to='county_fips_code', allow_none=True)
    hud_property_name = fields.String(load_from='development_name', dump_to='hud_property_name')
    property_street_address = fields.String(load_from='ADDRESS'.lower(), dump_to='property_street_address')
    inspection_id = fields.Integer(load_from='INSPECTION_ID'.lower())
    inspection_property_id = fields.String(load_from='DEVELOPMENT_ID'.lower(), dump_to='inspection_property_id')
    inspection_score = fields.Integer(load_from='INSPECTION_SCORE'.lower(), dump_to='inspection_score')
    inspection_date = fields.Date(load_from='INSPECTION DATE'.lower(), dump_to='inspection_date')
    pha_code = fields.String(load_from='PHA_CODE'.lower(), dump_to='participant_code')
    pha_name = fields.String(load_from='PHA_NAME'.lower(), dump_to='formal_participant_name')

    class Meta:
        ordered = True

    @pre_load
    def synthesize_fips_county_code(self, data):
        if data['county_code'] is None or data['state_code'] is None:
            data['county_fips_code'] = None
        else:
            data['county_fips_code'] = f"{str(data['state_code'])}{str(data['county_code'])}"
            assert len(data['county_fips_code']) == 5

    @pre_load
    def fix_dates(self, data):
        """Marshmallow doesn't know how to handle a datetime as input. It can only
        take strings that represent datetimes and convert them to datetimes.:
        https://github.com/marshmallow-code/marshmallow/issues/656
        So this is a workaround.
        """
        date_fields = ['inspection_date']
        for f in date_fields:
            if data[f] is not None:
                data[f] = parser.parse(data[f]).date().isoformat()


class HUDPublicHousingSchema(pl.BaseSchema):
    latitude = fields.Float(load_from='\ufeffX'.lower(), dump_to='latitude', allow_none=True)
    longitude = fields.Float(load_from='Y'.lower(), dump_to='longitude', allow_none=True)
    lvl2kx = fields.String(load_from='LVL2KX', dump_to='geocoding_accuracy')
        # 'R' - Interpolated rooftop (high degree of accuracy, symbolized as green)
        # '4' - ZIP+4 centroid (high degree of accuracy, symbolized as green)
        # 'B' - Block group centroid (medium degree of accuracy, symbolized as yellow)
        # 'T' - Census tract centroid (low degree of accuracy, symbolized as red)
        # '2' - ZIP+2 centroid (low degree of accuracy, symbolized as red)
        # 'Z' - ZIP5 centroid (low degree of accuracy, symbolized as red)
        # '5' - ZIP5 centroid (same as above, low degree of accuracy, symbolized as red)
        # Null - Could not be geocoded (does not appear on the map)
        # "For the purposes of displaying the location of an address on a map only use
        # addresses and their associated lat/long coordinates where the LVL2KX field is
        # coded 'R' or '4'. These codes ensure that the address is displayed on the
        # correct street segment and in the correct census block."
    county_level = fields.String(load_from='COUNTY_LEVEL'.lower(), dump_to='county_fips_code', allow_none=True)
    tract_level = fields.String(load_from='TRACT_LEVEL'.lower(),dump_to='census_tract', allow_none=True)
    curcosub = fields.String(load_from='CURCOSUB'.lower(),dump_to='municipality_fips', allow_none=True)
    curcosub_nm = fields.String(load_from='CURCOSUB_NM'.lower(),dump_to='municipality_name', allow_none=True)
    hud_property_name =  fields.String(load_from='PROJECT_NAME'.lower(), dump_to='hud_property_name')
    property_street_address = fields.String(load_from='STD_ADDR'.lower(), dump_to='property_street_address', allow_none=True)
    city = fields.String(load_from='STD_CITY'.lower(), dump_to='city')
    state = fields.String(load_from='STD_ST'.lower(), dump_to='state')
    zip_code = fields.String(load_from='STD_ZIP5'.lower(), dump_to='zip_code')
    units = fields.Integer(load_from='TOTAL_UNITS'.lower(), dump_to='units')
    owner_name = fields.String(load_from='FORMAL_PARTICIPANT_NAME'.lower(), dump_to='owner_name')

    # Public-Housing-Project-specific fields
    participant_code = fields.String(load_from='PARTICIPANT_CODE'.lower(), dump_to='participant_code')
    formal_participant_name = fields.String(load_from='FORMAL_PARTICIPANT_NAME'.lower(), dump_to='formal_participant_name')
    development_code = fields.String(load_from='DEVELOPMENT_CODE'.lower())
    project_name = fields.String(load_from='PROJECT_NAME'.lower(), dump_to='project_name')
    #scattered_site_ind = fields.String(load_from='SCATTERED_SITE_IND'.lower(), dump_to='scattered_site_ind') # Projects-only
    #pd_status_type_code = fields.String(load_from='PD_STATUS_TYPE_CODE'.lower(), dump_to='pd_status_type_code') # Projects-only
    total_dwelling_units = fields.Integer(load_from='TOTAL_DWELLING_UNITS'.lower(), dump_to='total_dwelling_units')
    acc_units = fields.Integer(load_from='ACC_UNITS'.lower(), dump_to='acc_units')
    total_occupied = fields.Integer(load_from='TOTAL_OCCUPIED'.lower(), dump_to='total_occupied')
    regular_vacant = fields.Integer(load_from='REGULAR_VACANT'.lower(), dump_to='regular_vacant')
    total_units = fields.Integer(load_from='TOTAL_UNITS'.lower(), dump_to='total_units')
    pha_total_units = fields.Integer(load_from='PHA_TOTAL_UNITS'.lower(), dump_to='pha_total_units')
    percent_occupied = fields.String(load_from='PCT_OCCUPIED'.lower(), dump_to='percent_occupied', allow_none=True)
    people_per_unit = fields.Float(load_from='PEOPLE_PER_UNIT'.lower(), dump_to='people_per_unit', allow_none=True)
    people_total = fields.Integer(load_from='PEOPLE_TOTAL'.lower(), dump_to='people_total', allow_none=True)
    rent_per_month = fields.Integer(load_from='RENT_PER_MONTH'.lower(), dump_to='rent_per_month', allow_none=True)
    median_inc_amnt = fields.Integer(load_from='MEDIAN_INC_AMNT'.lower(), dump_to='median_inc_amnt', allow_none=True)
    hh_income = fields.Integer(load_from='HH_INCOME'.lower(), dump_to='hh_income', allow_none=True)
    person_income = fields.Integer(load_from='PERSON_INCOME'.lower(), dump_to='person_income', allow_none=True)
    pct_lt5k = fields.Float(load_from='PCT_LT5K'.lower(), dump_to='pct_lt5k', allow_none=True)
    pct_5k_lt10k = fields.Float(load_from='PCT_5K_LT10K'.lower(), dump_to='pct_5k_lt10k', allow_none=True)
    pct_10k_lt15k = fields.Float(load_from='PCT_10K_LT15K'.lower(), dump_to='pct_10k_lt15k', allow_none=True)
    pct_15k_lt20k = fields.Float(load_from='PCT_15K_LT20K'.lower(), dump_to='pct_15k_lt20k', allow_none=True)
    pct_ge20k = fields.Float(load_from='PCT_GE20K'.lower(), dump_to='pct_ge20k', allow_none=True)
    pct_lt80_median = fields.String(load_from='PCT_LT80_MEDIAN'.lower(), dump_to='pct_lt80_median', allow_none=True)
    pct_lt50_median = fields.Float(load_from='PCT_LT50_MEDIAN'.lower(), dump_to='pct_lt50_median', allow_none=True)
    pct_lt30_median = fields.Float(load_from='PCT_LT30_MEDIAN'.lower(), dump_to='pct_lt30_median', allow_none=True)
    pct_bed1 = fields.Float(load_from='PCT_BED1'.lower(), dump_to='pct_bed1', allow_none=True)
    pct_bed2 = fields.Float(load_from='PCT_BED2'.lower(), dump_to='pct_bed2', allow_none=True)
    pct_bed3 = fields.Float(load_from='PCT_BED3'.lower(), dump_to='pct_bed3', allow_none=True)
    pct_overhoused = fields.Float(load_from='PCT_OVERHOUSED'.lower(), dump_to='pct_overhoused', allow_none=True)
    tminority = fields.String(load_from='TMINORITY'.lower(), dump_to='tminority', allow_none=True)
    tpoverty = fields.String(load_from='TPOVERTY'.lower(), dump_to='tpoverty', allow_none=True)
    tpct_ownsfd = fields.String(load_from='TPCT_OWNSFD'.lower(), dump_to='tpct_ownsfd', allow_none=True)
    chldrn_mbr_cnt = fields.Integer(load_from='CHLDRN_MBR_CNT'.lower(), dump_to='chldrn_mbr_cnt', allow_none=True)
    eldly_prcnt = fields.String(load_from='ELDLY_PRCNT'.lower(), dump_to='eldly_prcnt', allow_none=True)
    pct_disabled_lt62_all = fields.String(load_from='PCT_DISABLED_LT62_ALL'.lower(), dump_to='pct_disabled_lt62_all', allow_none=True)


    class Meta:
        ordered = True

    @pre_load
    def fix_obscured_values(self, data):
        """ 'In an effort to protect Personally Identifiable Information (PII), the characteristics
        for each building are suppressed with a -4 value when the "Number_Reported" is equal to,
        or less than 10.' - https://hudgis-hud.opendata.arcgis.com/datasets/public-housing-buildings
        """
        fields = ['pct_occupied', 'people_per_unit', 'people_total', 'rent_per_month',
                'hh_income', 'person_income', 'pct_lt5k', 'pct_5k_lt10k', 'pct_10k_lt15k',
                'pct_15k_lt20k', 'pct_ge20k', 'pct_lt50_median', 'pct_lt30_median',
                'pct_bed1', 'pct_bed2', 'pct_bed3', 'pct_overhoused', 'tminority',
                'tpoverty', 'tpct_ownsfd', 'chldrn_mbr_cnt', 'eldly_prcnt',
                'pct_disabled_lt62_all', 'pct_lt80_median', 'median_inc_amnt',
                ]
        for f in fields:
            if data[f] == '-4':
                data[f] = None

class HUDPublicHousingProjectsSchema(HUDPublicHousingSchema):
    job_code = 'hud_public_housing_projects'
    property_id = fields.String(load_from='DEVELOPMENT_CODE'.lower(), dump_to='property_id', allow_none=True) # Duplicated in "development_code" below.

    scattered_site_ind = fields.String(load_from='SCATTERED_SITE_IND'.lower(), dump_to='scattered_site_ind') # Projects-only
    pd_status_type_code = fields.String(load_from='PD_STATUS_TYPE_CODE'.lower(), dump_to='pd_status_type_code') # Projects-only

class HUDPublicHousingBuildingsSchema(HUDPublicHousingSchema):
    job_code = 'hud_public_housing_buildings'
    property_id = fields.String(load_from='PROPERTY_ID'.lower(), dump_to='property_id', allow_none=True)

# dfg

job_dicts = [
    {
        'update': 0,
        'job_code': MultifamilyInsuredMortgagesSchema().job_code, #'mf_mortgages'
        #This Excel 2018 file includes all active HUD Multifamily insured mortgages. The data is as of  January 4, 2021 and is updated monthly. It is extracted from MFIS and includes the following data elements:
        #   FHA Project Number
        #   Project Name
        #   Project Street
        #   Project City
        #   Project State
        #   Project Zip Code
        #   Number of total Units (or total beds for health and hospital care)
        #   Initial Endorsement Date
        #   Final Endorsement Date
        #   Original Mortgage Amount
        #   First Payment Date
        #   Maturity Date
        #   Term of loan in months
        #   Interest Rate
        #   Monthly Principal and Interest Payment
        #   Amortized Unpaid Principal Balance
        #   Holder Name
        #   Holder City
        #   Holder State
        #   Servicer Name
        #   Servicer City
        #   Servicer State
        #   Section of the Act Code (SoA)
        #   Section of the Act Category/Sub Category (Note: SOA codes are available on this list: SoA_list.xlsx.)
        #
        #   Tax Exempt Bond Financing Code (Mortgages financed by tax exempt bonds are indicated by "TE" code. This field was updated on new endorsements beginning in CY 2000).
        #
        #   Tax Credit Code (Mortgages that include Low Income Housing Tax Credits (LIHTC) are indicated by the "TC" code. This field was updated on endorsements beginning in the middle of 1998.)
        'source_type': 'http',
        'source_file': 'FHA_BF90_RM_A_01042021.xlsx',
        #'source_full_url': 'https://www.hud.gov/sites/dfiles/Housing/images/FHA_BF90_RM_A_01042021.xlsx',
        'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/comp/rpts/mfh/mf_f47', 'xlsx', 1, 2, 'FHA'),
        'encoding': 'binary',
        'schema': MultifamilyInsuredMortgagesSchema,
        'filters': [['property_state', '==', 'PA']], # Location information includes city, state, and ZIP code.
        'always_wipe_data': True,
        'primary_key_fields': ['hud_project_number'], # "HUD PROJECT NUMBER" seems pretty unique.
        'destinations': ['file'],
        'destination_file': 'mf_mortgages.csv',
    },
    {
        'update': 0,
        'job_code': MultifamilyProductionInitialCommitmentSchema().job_code, # 'mf_init_commit'
        'source_type': 'http',
        'source_file': 'Initi_Endores_Firm%20Comm_DB_FY21_Q1.xlsx',
        #'source_full_url': 'https://www.hud.gov/sites/dfiles/Housing/documents/Initi_Endores_Firm%20Comm_DB_FY21_Q1.xlsx',
        'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/mfh/mfdata/mfproduction', 'xlsx', 0, 2, 'Q'),
        'encoding': 'binary',
        'rows_to_skip': 3,
        'schema': MultifamilyProductionInitialCommitmentSchema,
        'filters': [['project_state', '==', 'PA']], # No county field. Just city and state. (And Pittsburgh is misspelled as "Pittsburg".)
        'always_wipe_data': True,
        'primary_key_fields': ['fha_number'], # "HUD PROJECT NUMBER" seems pretty unique.
        'destinations': ['file'],
        'destination_file': 'mf_init_commit.csv',
    },
    {
        'update': 0,
        'job_code': HousingInspectionScoresSchema().job_code, # 'housing_inspections'
        'source_type': 'http',
        'source_file': 'public_housing_physical_inspection_scores_0620.xlsx',
        #'source_full_url': 'https://www.huduser.gov/portal/sites/default/files/xls/public_housing_physical_inspection_scores_0620.xlsx',
        'source_full_url': scrape_nth_link('https://www.huduser.gov/portal/datasets/pis.html', 'xlsx', 0, None, 'public'), # The number of links increases each year.
        'encoding': 'binary',
        'rows_to_skip': 0,
        'schema': HousingInspectionScoresSchema,
        'filters': [['state_name', '==', 'PA']], # use 'county_fips_code == 42003' to limit to Allegheny County
        'always_wipe_data': True,
        'primary_key_fields': ['fha_number'], # "HUD PROJECT NUMBER" seems pretty unique.
        'destinations': ['file'],
        'destination_file': 'housing_inspections.csv',
    },
    {
        'update': 0,
        'job_code': HUDPublicHousingProjectsSchema().job_code, # 'hud_public_housing_projects'
        'source_type': 'http',
        'source_file': get_arcgis_data_url('https://hudgis-hud.opendata.arcgis.com/data.json', 'Public Housing Developments', 'CSV')[1],
        'source_full_url': get_arcgis_data_url('https://hudgis-hud.opendata.arcgis.com/data.json', 'Public Housing Developments', 'CSV')[0],
        'encoding': 'utf-8',
        'schema': HUDPublicHousingProjectsSchema,
        'filters': [['std_st', '==', 'PA']], # Coordinates could be used to filter to Allegheny County.
        'always_wipe_data': True,
        #'primary_key_fields': # DEVELOPMENT_CODE seems like a possible unique key.
        'destinations': ['file'],
        'destination_file': 'public_housing_projects.csv',
    },
    {
        'update': 0,
        'job_code': HUDPublicHousingBuildingsSchema().job_code, # 'hud_public_housing_buildings'
        'source_type': 'http',
        'source_file': get_arcgis_data_url('https://hudgis-hud.opendata.arcgis.com/data.json', 'Public Housing Buildings', 'CSV')[1], # The downside to this
            # is that it can not be run offline, even if the file is cached ins source_files/house_cat/.
        'source_full_url': get_arcgis_data_url('https://hudgis-hud.opendata.arcgis.com/data.json', 'Public Housing Buildings', 'CSV')[0],
        'encoding': 'utf-8',
        'schema': HUDPublicHousingBuildingsSchema,
        'filters': [['std_st', '==', 'PA']], # Coordinates could be used to filter to Allegheny County.
        'always_wipe_data': True,
        #'primary_key_fields': # PROJECT_ID seems like a possible unique key.
        'destinations': ['file'],
        'destination_file': 'public_housing_buildings.csv',
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
