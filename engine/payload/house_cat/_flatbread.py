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
from engine.parameters.local_parameters import SOURCE_DIR, PRODUCTION
from engine.post_processors import check_for_empty_table

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
    development_id = fields.String(load_from='DEVELOPMENT_ID'.lower(), dump_to='development_code')
    state = fields.String(load_from='STATE_NAME'.lower(), dump_to='state')
    latitude = fields.Float(load_from='LATITUDE'.lower(), allow_none=True)
    longitude = fields.Float(load_from='LONGITUDE'.lower(), allow_none=True)
    county = fields.String(load_from='COUNTY_NAME'.lower(), dump_to='county', allow_none=True)
    county_fips_code = fields.String(load_from='COUNTY_CODE'.lower(), dump_to='county_fips_code', allow_none=True)
    hud_property_name = fields.String(load_from='development_name', dump_to='hud_property_name')
    property_street_address = fields.String(load_from='ADDRESS'.lower(), dump_to='property_street_address')
    inspection_id = fields.Integer(load_from='INSPECTION_ID'.lower())
    inspection_property_id_multiformat = fields.String(load_from='DEVELOPMENT_ID'.lower(), dump_to='inspection_property_id_multiformat')
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
                if type(data[f]) == datetime:
                    data[f] = data[f].date().isoformat()
                else:
                    data[f] = parser.parse(data[f]).date().isoformat()

class HUDPublicHousingSchema(pl.BaseSchema):
    longitude = fields.Float(load_from='\ufeffX'.lower(), dump_to='longitude', allow_none=True)
    latitude = fields.Float(load_from='Y'.lower(), dump_to='latitude', allow_none=True)
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
    hud_property_name = fields.String(load_from='PROJECT_NAME'.lower(), dump_to='hud_property_name')
    property_street_address = fields.String(load_from='STD_ADDR'.lower(), dump_to='property_street_address', allow_none=True)
    city = fields.String(load_from='STD_CITY'.lower(), dump_to='city')
    state = fields.String(load_from='STD_ST'.lower(), dump_to='state')
    zip_code = fields.String(load_from='STD_ZIP5'.lower(), dump_to='zip_code')
    units = fields.Integer(load_from='TOTAL_UNITS'.lower(), dump_to='units')
    owner_name = fields.String(load_from='FORMAL_PARTICIPANT_NAME'.lower(), dump_to='owner_name')

    # Public-Housing-Project-specific fields
    participant_code = fields.String(load_from='PARTICIPANT_CODE'.lower(), dump_to='participant_code')
    formal_participant_name = fields.String(load_from='FORMAL_PARTICIPANT_NAME'.lower(), dump_to='formal_participant_name')
    development_code = fields.String(load_from='DEVELOPMENT_CODE'.lower()) # This is like a property ID.
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
    scattered_site_ind = fields.String(load_from='SCATTERED_SITE_IND'.lower(), dump_to='scattered_site_ind') # Projects-only
    pd_status_type_code = fields.String(load_from='PD_STATUS_TYPE_CODE'.lower(), dump_to='pd_status_type_code') # Projects-only

class HUDPublicHousingBuildingsSchema(HUDPublicHousingSchema):
    job_code = 'hud_public_housing_buildings'
    national_bldg_id = fields.String(load_from='NATIONAL_BLDG_ID'.lower(), dump_to='national_building_id', allow_none=True)

class MultifamilyProjectsSubsidySection8Schema(pl.BaseSchema):
    job_code = 'mf_subsidy_8'
    property_id = fields.String(load_from='property_id'.lower(), dump_to='property_id')
    county_code = fields.String(load_from='county_code'.lower(), dump_to='county_code')
    congressional_district_code = fields.String(load_from='congressional_district_code'.lower(), dump_to='congressional_district_code', allow_none=True)
    placed_base_city_name_text = fields.String(load_from='placed_base_city_name_text'.lower(), dump_to='municipality_name', allow_none=True)
    property_name_text = fields.String(load_from='property_name_text'.lower(), dump_to='hud_property_name')
    address_line1_text = fields.String(load_from='address_line1_text'.lower(), dump_to='property_street_address')
    city_name_text = fields.String(load_from='city_name_text'.lower(), dump_to='city')
    state_code = fields.String(load_from='state_code'.lower(), dump_to='state')
    zip_code = fields.Integer(load_from='zip_code'.lower(), dump_to='zip_code')
    # Should ZIP+4 be added for easy of matching with other datasets?
    property_total_unit_count = fields.Integer(load_from='property_total_unit_count'.lower(), dump_to='units')
    property_category_name = fields.String(load_from='property_category_name'.lower(), dump_to='property_category_name')
    owner_organization_name = fields.String(load_from='owner_organization_name'.lower(), dump_to='owner_organization_name', allow_none=True)
    owner_address = fields.String(load_from='owner_address_line1'.lower(), dump_to='owner_address')
    #owner_address_line1 = fields.String(load_from='owner_address_line1'.lower(), dump_to='owner_address_line1')
    #owner_address_line2 = fields.String(load_from='owner_address_line2'.lower(), dump_to='owner_address_line2')
    #owner_city_name = fields.String(load_from='owner_city_name'.lower(), dump_to='owner_city_name')
    #owner_state_code = fields.String(load_from='owner_state_code'.lower(), dump_to='owner_state_code')
    #owner_zip_code = fields.String(load_from='owner_zip_code'.lower(), dump_to='owner_zip_code')
    owner_main_phone_number_text = fields.String(load_from='owner_main_phone_number_text'.lower(), dump_to='owner_phone', allow_none=True)
    owner_company_type = fields.String(load_from='owner_company_type'.lower(), dump_to='owner_type', allow_none=True)
    ownership_effective_date = fields.Date(load_from='ownership_effective_date'.lower(), dump_to='ownership_effective_date', allow_none=True)
    owner_participant_id = fields.Integer(load_from='owner_participant_id'.lower(), dump_to='owner_id')

    mgmt_agent_full_name = fields.String(load_from='mgmt_agent_full_name'.lower(), dump_to='property_manager_name', allow_none=True)
    mgmt_agent_org_name = fields.String(load_from='mgmt_agent_org_name'.lower(), dump_to='property_manager_company', allow_none=True)
    mgmt_agent_address = fields.String(load_from='mgmt_agent_address_line1'.lower(), dump_to='mgmt_agent_address', allow_none=True)
    #mgmt_agent_address_line1 = fields.String(load_from='mgmt_agent_address_line1'.lower(), dump_to='mgmt_agent_address_line1')
    #mgmt_agent_address_line2 = fields.String(load_from='mgmt_agent_address_line2'.lower(), dump_to='mgmt_agent_address_line2')
    #mgmt_agent_city_name = fields.String(load_from='mgmt_agent_city_name'.lower(), dump_to='mgmt_agent_city_name')
    #mgmt_agent_state_code = fields.String(load_from='mgmt_agent_state_code'.lower(), dump_to='mgmt_agent_state_code')
    #mgmt_agent_zip_code = fields.String(load_from='mgmt_agent_zip_code'.lower(), dump_to='mgmt_agent_zip_code')
    mgmt_agent_main_phone_number = fields.String(load_from='mgmt_agent_main_phone_number'.lower(), dump_to='property_manager_phone', allow_none=True)
    mgmt_contact_email_text = fields.String(load_from='mgmt_contact_email_text'.lower(), dump_to='property_manager_email', allow_none=True)
    mgmt_agent_company_type = fields.String(load_from='mgmt_agent_company_type'.lower(), dump_to='property_manager_type', allow_none=True)
    servicing_site_name_text = fields.String(load_from='servicing_site_name_text'.lower(), dump_to='servicing_site_name')

    class Meta:
        ordered = True

    @pre_load
    def fix_dates(self, data):
        """Marshmallow doesn't know how to handle a datetime as input. It can only
        take strings that represent datetimes and convert them to datetimes.:
        https://github.com/marshmallow-code/marshmallow/issues/656
        So this is a workaround.
        """
        date_fields = ['ownership_effective_date']
        for f in date_fields:
            if data[f] is not None:
                data[f] = data[f].date().isoformat()

    @pre_load
    def transform_ints_to_strings(self, data):
        fields = ['property_id', 'county_code', 'congressional_district_code', 'mgmt_agent_main_phone_number',
                'owner_main_phone_number_text', 'placed_base_city_name_text']
        for f in fields:
            if data[f] is not None:
                data[f] = str(data[f])

    @pre_load
    def form_addresses(self, data):
        address = f"{data['owner_address_line1']}, {str(data['owner_address_line2'])+', ' if data['owner_address_line2'] is not None else ''}{data['owner_city_name']}, {data['owner_state_code']} {data['owner_zip_code']}"
        data['owner_address'] = address
        address = f"{data['mgmt_agent_address_line1']}, {str(data['mgmt_agent_address_line2'])+', ' if data['mgmt_agent_address_line2'] is not None else ''}{data['mgmt_agent_city_name']}, {data['mgmt_agent_state_code']} {data['mgmt_agent_zip_code']}"
        data['property_manager_address'] = address

class MultifamilyProjectsSection8ContractsSchema(pl.BaseSchema):
    job_code = 'mf_contracts_8'
    property_id = fields.String(load_from='property_id'.lower(), dump_to='property_id')
    property_name_text = fields.String(load_from='property_name_text'.lower(), dump_to='hud_property_name')
    assisted_units_count = fields.Integer(load_from='assisted_units_count'.lower(), dump_to='assisted_units')
    count_0br = fields.Integer(load_from='0BR_count'.lower(), dump_to='count_0br', allow_none=True)
    count_1br = fields.Integer(load_from='1BR_count'.lower(), dump_to='count_1br', allow_none=True)
    count_2br = fields.Integer(load_from='2BR_count'.lower(), dump_to='count_2br', allow_none=True)
    count_3br = fields.Integer(load_from='3BR_count'.lower(), dump_to='count_3br', allow_none=True)
    count_4br = fields.Integer(load_from='4BR_count'.lower(), dump_to='count_4br', allow_none=True)
    count_5plusbr = fields.Integer(load_from='5plusBR_count'.lower(), dump_to='count_5plusbr', allow_none=True)
    contract_number = fields.String(load_from='contract_number'.lower(), dump_to='contract_id')
    program_type_name = fields.String(load_from='program_type_name'.lower(), dump_to='program_type', allow_none=True)
    tracs_effective_date = fields.Date(load_from='tracs_effective_date'.lower(), dump_to='subsidy_start_date', allow_none=True)
    tracs_overall_expiration_date = fields.String(load_from='tracs_overall_expiration_date'.lower(), dump_to='subsidy_expiration_date', allow_none=True)
    contract_term_months_qty = fields.Integer(load_from='contract_term_months_qty'.lower(), dump_to='contract_duration_months')

    class Meta:
        ordered = True

    @pre_load
    def fix_dates(self, data):
        """Marshmallow doesn't know how to handle a datetime as input. It can only
        take strings that represent datetimes and convert them to datetimes.:
        https://github.com/marshmallow-code/marshmallow/issues/656
        So this is a workaround.
        """
        date_fields = ['tracs_effective_date',
                'tracs_overall_expiration_date']
        for f in date_fields:
            if data[f] is not None:
                data[f] = data[f].date().isoformat()

    @pre_load
    def transform_ints_to_strings(self, data):
        fields = ['property_id', 'assisted_units_count', '0br_count', '1br_count',
                '2br_count', '3br_count', '4br_count', '5plusbr_count',
                'contract_term_months_qty'
                ]
        for f in fields:
            if data[f] is not None:
                data[f] = str(data[f])

class MultifamilyProjectsSubsidyLoansSchema(pl.BaseSchema):
    # This schema is applied to the same file as mf_loans. While different fields are being used
    # to pull property_manager_name and property_manager_phone, the values are identical for
    # all 95 records. These two schema could therefore be easily merged into a single
    # one (though they pull some nonoverlapping fields), but we'll leave them as separate for now.
    job_code = 'mf_subsidy_loans'
    property_id = fields.String(load_from='property_id'.lower(), dump_to='property_id')
    longitude = fields.Float(load_from='\ufeffX'.lower(), dump_to='longitude', allow_none=True)
    latitude = fields.Float(load_from='Y'.lower(), dump_to='latitude', allow_none=True)
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
    cnty_nm2kx = fields.String(load_from='CNTY_NM2KX'.lower(), dump_to='county', allow_none=True)
    cnty2kx = fields.String(load_from='CNTY2KX'.lower(), dump_to='county_fips_code', allow_none=True)
    congressional_district_code = fields.String(load_from='congressional_district_code'.lower(), dump_to='congressional_district_code', allow_none=True)
    tract2kx = fields.String(load_from='TRACT2KX'.lower(),dump_to='census_tract', allow_none=True)
    curcosub = fields.String(load_from='CURCOSUB'.lower(),dump_to='municipality_fips', allow_none=True)
    placed_base_city_name_text = fields.String(load_from='placed_base_city_name_text'.lower(), dump_to='municipality_name', allow_none=True)
    property_name_text = fields.String(load_from='property_name_text'.lower(), dump_to='hud_property_name')
    address_line1_text = fields.String(load_from='address_line1_text'.lower(), dump_to='property_street_address')
    std_city = fields.String(load_from='std_city'.lower(), dump_to='city', allow_none=True)
    std_st = fields.String(load_from='std_st'.lower(), dump_to='state')
    std_zip5 = fields.String(load_from='std_zip5'.lower(), dump_to='zip_code')
    # Should ZIP+4 be added for easy of matching with other datasets?

    total_unit_count = fields.Integer(load_from='total_unit_count'.lower(), dump_to='units')
    total_assisted_unit_count = fields.Integer(load_from='total_assisted_unit_count'.lower(), dump_to='assisted_units')

    occupancy_date = fields.Date(load_from='OCCUPANCY_DATE'.lower(), dump_to='occupancy_date', allow_none=True)

    property_category_name = fields.String(load_from='property_category_name'.lower(), dump_to='property_category_name')
    project_manager_name_text = fields.String(load_from='MGMT_CONTACT_FULL_NAME'.lower(), dump_to='property_manager_name', allow_none=True) # uses different field from mf_loans
    mgmt_agent_org_name = fields.String(load_from='MGMT_AGENT_ORG_NAME'.lower(), dump_to='property_manager_company', allow_none=True)
    mgmt_contact_address_line1 = fields.String(load_from='MGMT_CONTACT_ADDRESS_LINE1'.lower(), dump_to='property_manager_address', allow_none=True)
    property_on_site_phone_number = fields.String(load_from='MGMT_CONTACT_MAIN_PHN_NBR'.lower(), dump_to='property_manager_phone', allow_none=True) # uses different field from mf_loans
    mgmt_contact_email_text = fields.String(load_from='mgmt_contact_email_text'.lower(), dump_to='property_manager_email', allow_none=True)

    # Subsidy Information (HUD)
    contract1 = fields.String(load_from='CONTRACT1'.lower(), dump_to='contract_id', allow_none=True)
    program_type1 = fields.String(load_from='PROGRAM_TYPE1'.lower(), dump_to='program_type', allow_none=True)
    units1 = fields.Integer(load_from='UNITS1'.lower(), dump_to='subsidy_units')
    expiration_date1 = fields.Date(load_from='EXPIRATION_DATE1'.lower(), dump_to='subsidy_expiration_date', allow_none=True)

    count_0br = fields.Integer(load_from='BD0_CNT1'.lower(), dump_to='count_0br', allow_none=True)
    count_1br = fields.Integer(load_from='BD1_CNT1'.lower(), dump_to='count_1br', allow_none=True)
    count_2br = fields.Integer(load_from='BD2_CNT1'.lower(), dump_to='count_2br', allow_none=True)
    count_3br = fields.Integer(load_from='BD3_CNT1'.lower(), dump_to='count_3br', allow_none=True)
    count_4br = fields.Integer(load_from='BD4_CNT1'.lower(), dump_to='count_4br', allow_none=True)
    count_5plusbr = fields.Integer(load_from='BD5_CNT1'.lower(), dump_to='count_5plusbr', allow_none=True)
    servicing_site_name_text = fields.String(load_from='servicing_site_name_text'.lower(), dump_to='servicing_site_name_loan', allow_none=True)

    reac_last_inspection_id = fields.Integer(load_from='REAC_LAST_INSPECTION_ID'.lower(), dump_to='inspection_id')
    reac_last_inspection_score = fields.String(load_from='REAC_LAST_INSPECTION_SCORE'.lower(), dump_to='inspection_score', allow_none=True)
    client_group_name = fields.String(load_from='CLIENT_GROUP_NAME'.lower(), dump_to='client_group_name', allow_none=True)
    client_group_type = fields.String(load_from='CLIENT_GROUP_TYPE'.lower(), dump_to='client_group_type', allow_none=True)

    #primary_fha_number = fields.String(load_from='PRIMARY_FHA_NUMBER'.lower(), dump_to='fha_loan_id') # Aren't these useful for linking?
    #associated_fha_number = fields.String(load_from='ASSOCIATED_FHA_NUMBER'.lower(), dump_to='associated_fha_loan_id') # Aren't these useful for linking?

    class Meta:
        ordered = True

class MultifamilyGuaranteedLoansSchema(pl.BaseSchema):
    job_code = 'mf_loans'
    property_id = fields.String(load_from='property_id'.lower(), dump_to='property_id')
    longitude = fields.Float(load_from='\ufeffX'.lower(), dump_to='longitude', allow_none=True)
    latitude = fields.Float(load_from='Y'.lower(), dump_to='latitude', allow_none=True)
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
    cnty_nm2kx = fields.String(load_from='CNTY_NM2KX'.lower(), dump_to='county', allow_none=True)
    cnty2kx = fields.String(load_from='CNTY2KX'.lower(), dump_to='county_fips_code', allow_none=True)
    congressional_district_code = fields.String(load_from='congressional_district_code'.lower(), dump_to='congressional_district_code', allow_none=True)
    tract2kx = fields.String(load_from='TRACT2KX'.lower(),dump_to='census_tract', allow_none=True)
    curcosub = fields.String(load_from='CURCOSUB'.lower(),dump_to='municipality_fips', allow_none=True)
    placed_base_city_name_text = fields.String(load_from='placed_base_city_name_text'.lower(), dump_to='municipality_name', allow_none=True)
    property_name_text = fields.String(load_from='property_name_text'.lower(), dump_to='hud_property_name')
    address_line1_text = fields.String(load_from='address_line1_text'.lower(), dump_to='property_street_address')
    std_city = fields.String(load_from='std_city'.lower(), dump_to='city', allow_none=True)
    std_st = fields.String(load_from='std_st'.lower(), dump_to='state')
    std_zip5 = fields.String(load_from='std_zip5'.lower(), dump_to='zip_code')
    # Should ZIP+4 be added for easy of matching with other datasets?

    total_unit_count = fields.Integer(load_from='total_unit_count'.lower(), dump_to='units')
    total_assisted_unit_count = fields.Integer(load_from='total_assisted_unit_count'.lower(), dump_to='assisted_units')
    property_category_name = fields.String(load_from='property_category_name'.lower(), dump_to='property_category_name')
    project_manager_name_text = fields.String(load_from='project_manager_name_text'.lower(), dump_to='property_manager_name', allow_none=True)
    property_on_site_phone_number = fields.String(load_from='property_on_site_phone_number'.lower(), dump_to='property_manager_phone', allow_none=True)
    primary_fha_number = fields.String(load_from='PRIMARY_FHA_NUMBER'.lower(), dump_to='fha_loan_id')
    associated_fha_number = fields.String(load_from='ASSOCIATED_FHA_NUMBER'.lower(), dump_to='associated_fha_loan_id')
    initial_endorsement_date = fields.Date(load_from='INITIAL_ENDORSEMENT_DATE'.lower(), dump_to='initial_endorsement_date', allow_none=True)
    original_loan_amount = fields.Integer(load_from='ORIGINAL_LOAN_AMOUNT'.lower(), dump_to='original_loan_amount')
    loan_maturity_date = fields.Date(load_from='LOAN_MATURITY_DATE'.lower(), dump_to='maturity_date', allow_none=True)
    program_type1 = fields.String(load_from='PROGRAM_TYPE1'.lower(), dump_to='program_category', allow_none=True)
    program_type2 = fields.String(load_from='PROGRAM_TYPE2'.lower(), dump_to='program_category_2', allow_none=True)
    unit_mrkt_rent_cnt = fields.Integer(load_from='UNIT_MRKT_RENT_CNT'.lower(), dump_to='loan_units') # The number of market-rate units.
    client_group_name = fields.String(load_from='CLIENT_GROUP_NAME'.lower(), dump_to='client_group_name', allow_none=True)
    client_group_type = fields.String(load_from='CLIENT_GROUP_TYPE'.lower(), dump_to='client_group_type', allow_none=True)
    soacode1 = fields.String(load_from='SOACODE1'.lower(), dump_to='section_of_act_code', allow_none=True)
    servicing_site_name_text = fields.String(load_from='servicing_site_name_text'.lower(), dump_to='servicing_site_name_loan', allow_none=True)

    class Meta:
        ordered = True

class LIHTCSchema(pl.BaseSchema):
    # Changes to the LIHTCPUB.csv schema between the 2018/2019 version of the data and the May 2021 update:
    # It abandoned "contact", "company", "co_add", "co_cty", "co_st", "co_zip", and "co_tel".
    #   This orphaned the fields "owner_organization_name", "property_manager_company", "property_manager_address", and "property_manager_phone".
    # It added "place1990", "place2000", "place2010", "st2010", and "cnty2010".
    job_code = 'lihtc'
    hud_id = fields.String(load_from='\ufeffhud_id'.lower(), dump_to='lihtc_project_id')
    latitude = fields.Float(load_from='latitude'.lower(), dump_to='latitude', allow_none=True)
    longitude = fields.Float(load_from='longitude'.lower(), dump_to='longitude', allow_none=True)

    fips2010 = fields.String(load_from='fips2010'.lower(), dump_to='census_tract')
    fips2000 = fields.String(load_from='fips2000'.lower(), dump_to='fips2000') # Included because it can often
    # have the correct code when the 2010 version includes a long XXXXXXX string.
    county_fips_code = fields.String(load_from='fips2010'.lower(), dump_to='county_fips_code')
    place2010 = fields.Integer(load_from='place2010'.lower(), dump_to='municipality_fips', allow_none=True)
    project = fields.String(load_from='project'.lower(), dump_to='hud_property_name')
    proj_add = fields.String(load_from='proj_add'.lower(), dump_to='property_street_address', allow_none=True)
    proj_cty = fields.String(load_from='proj_cty'.lower(), dump_to='city', allow_none=True)
    proj_st = fields.String(load_from='proj_st'.lower(), dump_to='state')
    proj_zip = fields.String(load_from='proj_zip'.lower(), dump_to='zip_code', allow_none=True)
    n_units = fields.Integer(load_from='n_units'.lower(), dump_to='units', allow_none=True)
    li_units = fields.Integer(load_from='li_units'.lower(), dump_to='assisted_units', allow_none=True)
    n_0br = fields.Integer(load_from='n_0br'.lower(), dump_to='count_0br', allow_none=True)
    n_1br = fields.Integer(load_from='n_1br'.lower(), dump_to='count_1br', allow_none=True)
    n_2br = fields.Integer(load_from='n_2br'.lower(), dump_to='count_2br', allow_none=True)
    n_3br = fields.Integer(load_from='n_3br'.lower(), dump_to='count_3br', allow_none=True)
    n_4br = fields.Integer(load_from='n_4br'.lower(), dump_to='count_4br', allow_none=True)
    ##contact = fields.String(load_from='contact'.lower(), dump_to='owner_organization_name', allow_none=True)
    ##property_manager_address = fields.String(dump_only=True, dump_to='property_manager_address', allow_none=True)
    ##co_add = fields.String(load_from='co_add', load_only=True, allow_none=True)
    ##co_cty = fields.String(load_from='co_cty', load_only=True, allow_none=True)
    ##co_st = fields.String(load_from='co_st', load_only=True, allow_none=True)
    ##co_zip = fields.String(load_from='co_zip', load_only=True, allow_none=True)
    ##company = fields.String(load_from='company'.lower(), dump_to='property_manager_company', allow_none=True)
    ##co_tel = fields.String(load_from='co_tel'.lower(), dump_to='property_manager_phone', allow_none=True)

    lihtc_federal_id = fields.String(load_from='hud_id'.lower(), dump_to='federal_id')
    state_id = fields.String(load_from='state_id'.lower(), dump_to='state_id', allow_none=True)
    credit = fields.Integer(load_from='credit'.lower(), dump_to='lihtc_credit', allow_none=True)

    construction_type = fields.String(load_from='type'.lower(), dump_to='lihtc_construction_type', allow_none=True)
    yr_alloc = fields.Integer(load_from='yr_alloc'.lower(), dump_to='lihtc_year_allocated')
    yr_pis = fields.Integer(load_from='yr_pis'.lower(), dump_to='lihtc_year_placed_into_service')
    allocamt = fields.String(load_from='allocamt'.lower(), dump_to='lihtc_amount', allow_none=True)
    fmha_514 = fields.Boolean(load_from='fmha_514'.lower(), dump_to='fmha_514_loan', allow_none=True)
    fmha_515 = fields.Boolean(load_from='fmha_515'.lower(), dump_to='fmha_515_loan', allow_none=True)
    fmha_538 = fields.Boolean(load_from='fmha_538'.lower(), dump_to='fmha_538_loan', allow_none=True)
    scattered_site_cd = fields.String(load_from='scattered_site_cd'.lower(), dump_to='scattered_site_ind', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def future_proof(self, data):
        global fips2020_notified
        if not fips2020_notified and 'fips2020' in data:
            msg = "The fips2020 variable was found in LIHTCPUB.CSV. Edit house_cat/_flatbread.py, switching LIHTCSchema to use fips2020 rather than fips2010 for extracting census_tract and county_fips_code. Also re-evaluate the methods for filtering these properties to Allegheny County (as the 42XXX issue caused both fips2010 and fips2000 to be previously incorporated)."
            print(msg)
            channel = "@david" if (not PRODUCTION) else "#etl-hell"
            if channel != "@david":
                msg = f"@david {msg}"
            send_to_slack(msg, username='house_cat schema monitor', channel=channel, icon=':tessercat:')
            fips2020_notified = True

    @pre_load
    def pre_decode_booleans(self, data):
        fs = ['fmha_514', 'fmha_515', 'fmha_538']
        for f in fs:
            if f in data and data[f] is not None:
                if str(data[f]) == '1':
                    data[f] = True
                elif str(data[f]) == '2':
                    data[f] = False

    @post_load
    def decode_fields(self, data):
        f = 'lihtc_construction_type'
        if f in data and data[f] is not None:
            construction_type_lookup = {'1': 'New construction',
                    '2': 'Acquisition and Rehab',
                    '3': 'Both new construction and A/R',
                    '4': 'Existing',
                    '': None}
            if data[f] in construction_type_lookup:
                data[f] = construction_type_lookup[data[f]]
        f = 'lihtc_credit'
        if f in data and data[f] is not None:
            credit_type_lookup = {'1': '30 percent present value',
                    '2': '70 percent present value',
                    '3': 'Both',
                    '4': 'Tax Credit Exchange Program only',
                    '': None}
            if data[f] in credit_type_lookup:
                data[f] = credit_type_lookup[data[f]]
        f = 'scattered_site_cd'
        if f in data and data[f] is not None:
            lookup = {'1': 'Y',
                    '2': 'N',
                    '': None}
            if data[f] in lookup:
                data[f] = lookup[data[f]]

    @pre_load
    def transform_ints_to_strings(self, data):
        fields = ['place2010', 'n_units', 'li_units',
                'n_0br', 'n_1br', 'n_2br', 'n_3br',
                'n_4br', 'credit', 'type',
                'yr_pis', 'yr_alloc',
                ]
        for f in fields:
            if data[f] is not None:
                data[f] = str(data[f])

    @post_load
    def truncate_to_county_fips_code(self, data):
        # In many cases where the county_fips_code is 42XXX, if the fips2000
        # code is used instead of the fips2010 code, a better determination
        # of the county can be made.
        f = 'county_fips_code'
        if data[f] is not None:
            data[f] = str(data[f])[:5]

class LIHTC2019Schema(LIHTCSchema):
    job_code = 'lihtc_2019'
    contact = fields.String(load_from='contact'.lower(), dump_to='owner_organization_name', allow_none=True)
    property_manager_address = fields.String(dump_only=True, dump_to='property_manager_address', allow_none=True)
    co_add = fields.String(load_from='co_add', load_only=True, allow_none=True)
    co_cty = fields.String(load_from='co_cty', load_only=True, allow_none=True)
    co_st = fields.String(load_from='co_st', load_only=True, allow_none=True)
    co_zip = fields.String(load_from='co_zip', load_only=True, allow_none=True)
    company = fields.String(load_from='company'.lower(), dump_to='property_manager_company', allow_none=True)
    co_tel = fields.String(load_from='co_tel'.lower(), dump_to='property_manager_phone', allow_none=True)

    lihtc_federal_id = fields.String(load_from='\ufeffhud_id'.lower(), dump_to='federal_id') # The older file had an extra FEFF character in the field name.

    @post_load
    def form_address(self, data): # [ ] Is there any chance we want to keep these parts as separate fields?
        address = ''
        if data['co_add'] not in ['', None]:
            address += f"{data['co_add']}, "
        if data['co_cty'] not in ['', None]:
            address += f"{data['co_cty']}, "
        if data['co_st'] not in ['', None]:
            address += f"{data['co_st']} "
        if data['co_zip'] not in ['', None]:
            address += f"{data['co_zip']}"
        address = address.strip()
        if address in ['', 'PA']:
            address = None
        data['property_manager_address'] = address

class LIHTCBuildingSchema(pl.BaseSchema):
    job_code = 'lihtc_building'
    hud_id = fields.String(load_from='\ufeffhud_id'.lower(), dump_to='lihtc_project_id')
    project = fields.String(load_from='project'.lower(), dump_to='hud_property_name')
    proj_add = fields.String(load_from='proj_add'.lower(), dump_to='property_street_address', allow_none=True)
    proj_cty = fields.String(load_from='proj_cty'.lower(), dump_to='city', allow_none=True)
    proj_st = fields.String(load_from='proj_st'.lower(), dump_to='state')
    proj_zip = fields.String(load_from='proj_zip'.lower(), dump_to='zip_code', allow_none=True)
    state_id = fields.String(load_from='state_id'.lower(), dump_to='state_id', allow_none=True)

    class Meta:
        ordered = True

class BaseMultifamilyInspectionsSchema(pl.BaseSchema):
    job_code = 'mf_inspections'
    rems_property_id = fields.String(load_from='REMS Property Id'.lower(), dump_to='property_id')
    property_name = fields.String(load_from='Property Name'.lower(), dump_to='hud_property_name')
    inspection_property_id_multiformat = fields.String(load_from='REMS_Property_Id'.lower(), dump_to='inspection_property_id_multiformat')
    city = fields.String(load_from='city'.lower(), dump_to='city')
    state_code = fields.String(load_from='state_code'.lower(), dump_to='state')

    class Meta:
        ordered = True

    @pre_load
    def transform_floats_to_strings(self, data):
        fields = ['rems_property_id', 'inspection_id_1',
                'inspection_id_2', 'inspection_id_3',
                ]
        for f in fields:
            if data[f] is not None:
                data[f] = str(int(data[f]))

    @pre_load
    def remove_trailing_spaces(self, data):
        fields = ['property_name', 'city']
        for f in fields:
            if data[f] is not None:
                data[f] = data[f].strip()

#    @pre_load
#    def fix_dates(self, data):
#        """Marshmallow doesn't know how to handle a datetime as input. It can only
#        take strings that represent datetimes and convert them to datetimes.:
#        https://github.com/marshmallow-code/marshmallow/issues/656
#        So this is a workaround.
#        """
#        date_fields = ['release_date_1',
#                'release_date_2', 'release_date_3']
#        for f in date_fields:
#            if data[f] is not None:
#                data[f] = data[f].date().isoformat()

class MultifamilyInspectionsSchema1(BaseMultifamilyInspectionsSchema):
    job_code = 'mf_inspections_1'
    inspection_id = fields.String(load_from='inspection_id_1'.lower(), dump_to='inspection_id', allow_none=True)
    inspection_score = fields.String(load_from='inspection_score1'.lower(), dump_to='inspection_score', allow_none=True)
    inspection_date = fields.Date(load_from='release_date_1'.lower(), dump_to='inspection_date', allow_none=True)

class MultifamilyInspectionsSchema2(BaseMultifamilyInspectionsSchema):
    job_code = 'mf_inspections_2'
    inspection_id = fields.String(load_from='inspection_id_2'.lower(), dump_to='inspection_id', allow_none=True)
    inspection_score = fields.String(load_from='inspection_score2'.lower(), dump_to='inspection_score', allow_none=True)
    inspection_date = fields.Date(load_from='release_date_2'.lower(), dump_to='inspection_date', allow_none=True)

class MultifamilyInspectionsSchema3(BaseMultifamilyInspectionsSchema):
    job_code = 'mf_inspections_3'
    inspection_id = fields.String(load_from='inspection_id_3'.lower(), dump_to='inspection_id', allow_none=True)
    inspection_score = fields.String(load_from='inspection_score3'.lower(), dump_to='inspection_score', allow_none=True)
    inspection_date = fields.Date(load_from='release_date_3'.lower(), dump_to='inspection_date', allow_none=True)

# All the schemas below are being commented out because the desired fields have not yet been identified.
#
#class USDAProgramExitSchema(pl.BaseSchema):
#    job_code = 'usda_exit'
#    property_name = fields.String(load_from='Property_Name'.lower(), dump_to='hud_property_name')
#    main_address_1 = fields.String(load_from='Main_Address_1'.lower(), dump_to='property_street_address', allow_none=True)
#
#    state_county_fips_code = fields.String(load_from='State_County_FIPS_Code'.lower(), dump_to='county_fips_code', allow_none=True)
#    city = fields.String(load_from='City'.lower(), dump_to='city', allow_none=True)
#    state = fields.String(load_from='State'.lower(), dump_to='state')
#    zip_code = fields.String(load_from='Zip_Code'.lower(), dump_to='zip_code', allow_none=True)
#    latitude = fields.Float(load_from='latitude'.lower(), dump_to='latitude', allow_none=True)
#    longitude = fields.Float(load_from='longitude'.lower(), dump_to='longitude', allow_none=True)
#
#    class Meta:
#        ordered = True
#
#    @pre_load
#    def transform_ints_to_string(self, data):
#        fields = ['main_address_1', 'state_county_fips_code',
#                'zip_code']
#        for f in fields:
#            if data[f] is not None:
#                data[f] = str(data[f])
#
#class USDA514515Schema(pl.BaseSchema):
#    project_name = fields.String(load_from='Project_Name'.lower(), dump_to='hud_property_name')
#    main_address_line1 = fields.String(load_from='Main_Address_Line1'.lower(), dump_to='property_street_address', allow_none=True)
#
#    state_county_fips_code = fields.String(load_from='State_County_FIPS_Code'.lower(), dump_to='county_fips_code', allow_none=True)
#    city = fields.String(load_from='City'.lower(), dump_to='city', allow_none=True)
#    state_abbreviation = fields.String(load_from='State_Abbreviation'.lower(), dump_to='state', allow_none=True)
#    zip_code = fields.String(load_from='Zip_Code'.lower(), dump_to='zip_code', allow_none=True)
#
#    class Meta:
#        ordered = True
#
#    @pre_load
#    def transform_ints_to_string(self, data):
#        fields = ['main_address_line1', 'state_county_fips_code',
#                'zip_code']
#        for f in fields:
#            if data[f] is not None:
#                data[f] = str(data[f])
#
#class USDA514515ActiveSchema(USDA514515Schema):
#    job_code = 'usda_active'
#    latitude = fields.Float(load_from='latitude'.lower(), dump_to='latitude', allow_none=True)
#    longitude = fields.Float(load_from='longitude'.lower(), dump_to='longitude', allow_none=True)
#
#class USDA514515TenantSchema(USDA514515Schema):
#    job_code = 'usda_tenant'
#
#class USDA538Schema(pl.BaseSchema):
#    job_code = 'usda_538'
#    project_name = fields.String(load_from='Project_Name'.lower(), dump_to='hud_property_name')
#    main_address_line1 = fields.String(load_from='Main_Address_Line1'.lower(), dump_to='property_street_address', allow_none=True)
#
#    city = fields.String(load_from='City'.lower(), dump_to='city', allow_none=True)
#    state_abbreviation = fields.String(load_from='State_Abbreviation'.lower(), dump_to='state', allow_none=True)
#    zip_code = fields.String(load_from='Zip_Code'.lower(), dump_to='zip_code', allow_none=True) # This is a ZIP+4 code, which we will truncate.
#
#    latitude = fields.Float(load_from='latitude'.lower(), dump_to='latitude', allow_none=True)
#    ongitude = fields.Float(load_from='longitude'.lower(), dump_to='longitude', allow_none=True)
#
#    class Meta:
#        ordered = True
#
#    @pre_load
#    def transform_ints_to_string(self, data):
#        fields = ['main_address_line1', 'zip_code']
#        for f in fields:
#            if data[f] is not None:
#                data[f] = str(data[f])
#
#    @pre_load
#    def fix_zip_code(self, data):
#        f = 'zip_code'
#        if data[f] is not None:
#            data[f] = str(data[f])[:5]
#
#class HousingInventoryCountSchema(pl.BaseSchema):
#    job_code = 'hic'
#    coc_id = fields.Integer(load_from='Coc\\ID'.lower(), load_only=True, allow_none=True)
#    project_name = fields.String(load_from='Project_Name'.lower(), dump_to='hud_property_name') # There are also Project ID and HMIS Project ID values.
#    address1 = fields.String(load_from='address1'.lower(), dump_to='property_street_address', allow_none=True)

# dfg

fips2020_notified = False
housecat_package_id = 'bb77b955-b7c3-4a05-ac10-448e4857ade4'

job_dicts = [
    {
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
        'updates': 'Monthly',
        'encoding': 'binary',
        'schema': MultifamilyInsuredMortgagesSchema,
        'filters': [['property_state', '==', 'PA']], # Location information includes city, state, and ZIP code.
        'always_wipe_data': True,
        #'primary_key_fields': ['hud_project_number'], # "HUD PROJECT NUMBER" seems pretty unique.
        'destination': 'ckan',
        'destination_file': 'mf_mortgages_pa.csv',
        'package': housecat_package_id,
        'resource_name': 'Active HUD Multifamily Insured Mortgages (Pennsylvania)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://www.hud.gov/program_offices/housing/comp/rpts/mfh/mf_f47', #\n\njob code: {MultifamilyInsuredMortgagesSchema().job_code}',
    },
    {
        'job_code': MultifamilyProductionInitialCommitmentSchema().job_code, # 'mf_init_commit'
        'source_type': 'http',
        'source_file': 'Initi_Endores_Firm%20Comm_DB_FY21_Q1.xlsx',
        #'source_full_url': 'https://www.hud.gov/sites/dfiles/Housing/documents/Initi_Endores_Firm%20Comm_DB_FY21_Q1.xlsx',
        'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/mfh/mfdata/mfproduction', 'xlsx', 0, 2, 'Q'),
        'updates': 'Quarterly',
        'encoding': 'binary',
        'rows_to_skip': 3,
        'schema': MultifamilyProductionInitialCommitmentSchema,
        'filters': [['project_state', '==', 'PA']], # No county field. Just city and state. (And Pittsburgh is misspelled as "Pittsburg".)
        'always_wipe_data': True,
        #'primary_key_fields': ['fha_number'], # "HUD PROJECT NUMBER" seems pretty unique.
        'destination': 'ckan',
        'destination_file': 'mf_init_commit_pa.csv',
        'package': housecat_package_id,
        'resource_name': 'HUD Multifamily Fiscal Year Production (Pennsylvania)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://www.hud.gov/program_offices/housing/mfh/mfdata/mfproduction', #\n\njob code: {MultifamilyProductionInitialCommitmentSchema().job_code},'
    },
    {
        'job_code': f'unzip_{LIHTCSchema().job_code}', # 'unzip_lihtc'
        'source_type': 'http',
        'source_full_url': 'https://lihtc.huduser.gov/lihtcpub.zip',
        'source_file': 'lihtcpub.zip',
        'compressed_file_to_extract': 'LIHTCPUB.CSV',
        'encoding': 'binary',
        'always_wipe_data': True,
        'destination': 'file',
        'destination_file': f'{SOURCE_DIR}house_cat/LIHTCPUB.csv', # Needing to specify the
        # job directory makes this more of a hack than I would like.
        # Alternatives: Set the destination file in the Extractor or in configure_pipeline_with_options.
    },
    {
        'job_code': LIHTCSchema().job_code, # 'lihtc'
        'source_type': 'local',
        'source_file': 'LIHTCPUB.csv',
        'schema': LIHTCSchema,
        'filters': [['proj_st', '==', 'PA']], # It would seem that the county FIPS
        # code could be used to narrow this table to just Allegheny County, but
        # the presence of 42XXX codes and also the truncation required to get
        # the 5-digit code make this complicated.
        'always_wipe_data': True,
        'destination': 'ckan',
        'destination_file': f'lihtc_projects_pa.csv',
        'package': housecat_package_id,
        'resource_name': 'LIHTC (Pennsylvania)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://lihtc.huduser.gov/lihtcpub.zip', #\n\njob_code: {LIHTCSchema().job_code}',
    },
#    {   # This is a job to preserve the old LIHTC data for internal use.
#        'job_code': LIHTC2019Schema().job_code, # 'lihtc_2019'
#        'source_type': 'local',
#        'source_file': 'archive/LIHTCPUB_2019.csv',
#        'schema': LIHTC2019Schema,
#        'filters': [['proj_st', '==', 'PA']], # It would seem that the county FIPS
#        # code could be used to narrow this table to just Allegheny County, but
#        # the presence of 42XXX codes and also the truncation required to get
#        # the 5-digit code make this complicated.
#        #'always_wipe_data': True,
#        #'destination': 'ckan',
#        'destination': 'file',
#        'destination_file': f'lihtc_projects_pa_2019.csv',
#        #'package': housecat_package_id,
#        #'resource_name': 'LIHTC (Pennsylvania)',
#        #'upload_method': 'insert',
#        #'resource_description': f'Derived from https://lihtc.huduser.gov/lihtcpub.zip', #\n\njob_code: {LIHTCSchema().job_code}',
#    },
    {   # This job is a two-step job. Step 1: Get the buildings from the file that
        # either has to be manually pulled from lihtc.huduser.gov or extracted from
        # the Access database.
        'job_code': LIHTCBuildingSchema().job_code, # 'lihtc_building'
        'source_type': 'local',
        'source_file': 'lihtc-huduser-gov-extract-buildings.csv',
        'schema': LIHTCBuildingSchema,
        'filters': [['proj_st', '==', 'PA']], # Can't be limited, except by city name.
        'always_wipe_data': True,
        'destination': 'ckan',
        'destination_file': f'lihtc_building_pa.csv',
        'package': housecat_package_id,
        'resource_name': 'All Buildings from LIHTC Projects (Pennsylvania)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://lihtc.huduser.gov/', #\n\njob_code: {LIHTCBuildingSchema().job_code}',
    },
    {   # Step 2: Get the buildings which are in the original project-level file
        # and (probably) not in the multi-address building-level extraction.
        'job_code': LIHTCBuildingSchema().job_code + '2', # 'lihtc_building' + '2'
        'source_type': 'local',
        'source_file': 'LIHTCPUB.csv',
        'schema': LIHTCBuildingSchema,
        'filters': [['proj_st', '==', 'PA']], # Hard to geographically limit
        # because of issues with the LIHTC data discussed above.
        'always_wipe_data': False,
        'destination': 'ckan',
        'destination_file': f'lihtc_building_pa.csv',
        'package': housecat_package_id,
        'resource_name': 'All Buildings from LIHTC Projects (Pennsylvania)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://lihtc.huduser.gov/', #\n\njob_code: {LIHTCBuildingSchema().job_code}',
    },
    {
        'job_code': HousingInspectionScoresSchema().job_code, # 'housing_inspections'
        'source_type': 'http',
        'source_file': 'public_housing_physical_inspection_scores_0321.xlsx',
        #'source_full_url': 'https://www.huduser.gov/portal/sites/default/files/xls/public_housing_physical_inspection_scores_0620.xlsx',
        'source_full_url': scrape_nth_link('https://www.huduser.gov/portal/datasets/pis.html', 'xlsx', 0, None, 'public'), # The number of links increases each year.
        'encoding': 'binary',
        'rows_to_skip': 0,
        'schema': HousingInspectionScoresSchema,
        'filters': [['county_name', '==', 'Allegheny'], ['state_name', '==', 'PA']],
        'always_wipe_data': True,
        #'primary_key_fields': ['fha_number'], # "HUD PROJECT NUMBER" seems pretty unique.
        'destination': 'ckan',
        'destination_file': 'housing_inspections_ac.csv',
        'package': housecat_package_id,
        'resource_name': 'HUD Inspection Scores (Allegheny County)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://www.huduser.gov/portal/datasets/pis.html', #\n\njob code: {HousingInspectionScoresSchema().job_code}',
    },
    {
        'job_code': HUDPublicHousingProjectsSchema().job_code, # 'hud_public_housing_projects'
        'source_type': 'http',
        'source_full_url': get_arcgis_data_url('https://hudgis-hud.opendata.arcgis.com/data.json', 'Public Housing Developments', 'CSV')[0],
        'encoding': 'utf-8',
        'schema': HUDPublicHousingProjectsSchema,
        'filters': [['county_level', '==', '42003'], ['std_st', '==', 'PA']],
        'always_wipe_data': True,
        #'primary_key_fields': # DEVELOPMENT_CODE seems like a possible unique key.
        'destination': 'ckan',
        'destination_file': 'public_housing_projects_ac.csv',
        'package': housecat_package_id,
        'resource_name': 'HUD Public Housing Developments (Allegheny County)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://hudgis-hud.opendata.arcgis.com/datasets/public-housing-developments', #\n\njob code: {HUDPublicHousingProjectsSchema().job_code}',
    },
    {
        'job_code': HUDPublicHousingBuildingsSchema().job_code, # 'hud_public_housing_buildings'
        'source_type': 'http',
        'source_full_url': get_arcgis_data_url('https://hudgis-hud.opendata.arcgis.com/data.json', 'Public Housing Buildings', 'CSV')[0],
        'encoding': 'utf-8',
        'schema': HUDPublicHousingBuildingsSchema,
        'filters': [['county_level', '==', '42003'], ['std_st', '==', 'PA']],
        'always_wipe_data': True,
        #'primary_key_fields': # DEVELOPMENT_CODE seems like a possible unique key.
        'destination': 'ckan',
        'destination_file': 'public_housing_buildings_ac.csv',
        'package': housecat_package_id,
        'resource_name': 'HUD Public Housing Buildings (Allegheny County)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://hudgis-hud.opendata.arcgis.com/datasets/public-housing-buildings', #\n\njob code: {HUDPublicHousingBuildingsSchema().job_code}',

    },
    {
        'job_code': MultifamilyProjectsSubsidyLoansSchema().job_code, # 'mf_subsidy_loans'
        'source_type': 'http',
        'source_full_url': get_arcgis_data_url('https://hudgis-hud.opendata.arcgis.com/data.json', 'HUD Insured Multifamily Properties', 'CSV')[0], # HUD_Insured_Multifamily_Properties.csv
        # The downside to pulling the filename from the data.json file is that there is currently no support for offline caching
        # for testing purposes, but this could be remedied.
        'encoding': 'utf-8',
        'schema': MultifamilyProjectsSubsidyLoansSchema,
        'filters': [['std_st', '==', 'PA'], ['cnty_nm2kx', '==', 'Allegheny']], # cnty2kx could be used to filter to Allegheny County.
        'always_wipe_data': True,
        #'primary_key_fields': # POTENTIAL PRIMARY KEY FIELDS: ['PROPERTY_ID', 'PRIMARY_FHA_NUMBER', 'ASSOCIATED_FHA_NUMBER', 'FHA_NUM1']
        'destination': 'ckan',
        'destination_file': 'mf_subsidy_loans_ac.csv',
        'package': housecat_package_id,
        'resource_name': 'Subsidy extract from HUD Insured Multifamily Properties (Allegheny County)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://hudgis-hud.opendata.arcgis.com/datasets/hud-insured-multifamily-properties', # \nnjob code: {MultifamilyGuaranteedLoansSchema().job_code}', # 'mf_subsidy_loans'
    },
    {
        'job_code': MultifamilyProjectsSubsidySection8Schema().job_code, # 'mf_subsidy_8'
        'source_type': 'http',
        'source_file': 'MF_Properties_with_Assistance_\&_Sec8_Contracts.xlsx',
        'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/mfh/exp/mfhdiscl', 'xlsx', 0, 2, 'roperties'),
        'encoding': 'binary',
        'rows_to_skip': 0,
        'schema': MultifamilyProjectsSubsidySection8Schema,
        'filters': [['state_code', '==', 'PA'], ['county_code', '==', 3]], # use 'county_code == 3' to limit to Allegheny County
        'always_wipe_data': True,
        #'primary_key_fields': ['property_id'],
        'destination': 'ckan',
        'destination_file': 'mf_subsidy_8_ac.csv',
        'package': housecat_package_id,
        'resource_name': 'Subsidy extract from Multifamily Assistance & Section 8 Contracts (Allegheny County)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://www.hud.gov/program_offices/housing/mfh/exp/mfhdiscl', #'\n\njob code: {MultifamilyProjectsSubsidySection8Schema().job_code}',
        'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
    {
        'job_code': MultifamilyProjectsSection8ContractsSchema().job_code, # 'mf_contracts_8'
        'source_type': 'http',
        'source_file': 'MF_Assistance_&_Sec8_Contracts.xlsx',
        'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/mfh/exp/mfhdiscl', 'xlsx', 1, 2, 'ontracts'),
        'encoding': 'binary',
        'rows_to_skip': 0,
        'schema': MultifamilyProjectsSection8ContractsSchema,
        #'filters': # Nothing to directly filter on here. The property_id needs to be joined to mf_subsidy_8 to determine the property locations.
        'always_wipe_data': True,
        #'primary_key_fields': ['property_id'],
        'destination': 'ckan',
        'destination_file': 'mf_8_contracts_us.csv',
        'package': housecat_package_id,
        'resource_name': 'Multifamily Assistance & Section 8 Contracts (All)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://www.hud.gov/program_offices/housing/mfh/exp/mfhdiscl\n\n', #job code: {MultifamilyProjectsSection8ContractsSchema().job_code}',
    },
    {
        'job_code': MultifamilyGuaranteedLoansSchema().job_code, # 'mf_loans'
        'source_type': 'http',
        'source_full_url': get_arcgis_data_url('https://hudgis-hud.opendata.arcgis.com/data.json', 'HUD Insured Multifamily Properties', 'CSV')[0], # HUD_Insured_Multifamily_Properties.csv
        # The downside to pulling the filename from the data.json file is that there is currently no support for offline caching
        # for testing purposes, but this could be remedied.
        'encoding': 'utf-8',
        'schema': MultifamilyGuaranteedLoansSchema,
        'filters': [['std_st', '==', 'PA'], ['cnty_nm2kx', '==', 'Allegheny']], # cnty2kx could be used to filter to Allegheny County.
        'always_wipe_data': True,
        #'primary_key_fields': # POTENTIAL PRIMARY KEY FIELDS: ['PROPERTY_ID', 'PRIMARY_FHA_NUMBER', 'ASSOCIATED_FHA_NUMBER', 'FHA_NUM1']
        'destination': 'ckan',
        'destination_file': 'mf_loans_ac.csv',
        'package': housecat_package_id,
        'resource_name': 'HUD Insured Multifamily Properties (Allegheny County)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://hudgis-hud.opendata.arcgis.com/datasets/hud-insured-multifamily-properties', #\n\njob code: {MultifamilyGuaranteedLoansSchema().job_code}',
    },
    { # The source file is in a weird wide format, listing three different columns
      # for each of the three last inspections.
      # To convert this into a narrow format (with properties possibly appearing on
      # multiple rows), we run slight variants of the job three times, plucking
      # the correct inspection columns and filtering out the empty ones.
        'job_code': MultifamilyInspectionsSchema1().job_code, # 'mf_inspections_1'
        'source_type': 'http',
        'source_file': 'MF_Inspection_Report02252021.xls',
        'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/mfh/rems/remsinspecscores/remsphysinspscores', 'xls', 0, 1, 'nspection'),
        'encoding': 'binary',
        'rows_to_skip': 0,
        'schema': MultifamilyInspectionsSchema1,
        'filters': [['state_code', '==', 'PA']], # city and REMS_Property_ID are the only fields that could be used to geographically narrow this filter.
        'always_wipe_data': True,
        #'primary_key_fields': ['rems_property_id'],
        'destination': 'ckan',
        'destination_file': 'mf_inspections_pa.csv',
        'package': housecat_package_id,
        'resource_name': 'HUD Multifamily Inspection Scores (Pennsylvania)',
        'upload_method': 'insert',
        'resource_description': f'Derived from https://www.hud.gov/program_offices/housing/mfh/rems/remsinspecscores/remsphysinspscores', # \n\njob_code: {MultifamilyInspectionsSchema1().job_code[:-2]}'
    },
    {
        'job_code': MultifamilyInspectionsSchema2().job_code, # 'mf_inspections_2'
        'source_type': 'http',
        'source_file': 'MF_Inspection_Report02252021.xls',
        'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/mfh/rems/remsinspecscores/remsphysinspscores', 'xls', 0, 1, 'nspection'),
        'encoding': 'binary',
        'rows_to_skip': 0,
        'schema': MultifamilyInspectionsSchema2,
        'filters': [['state_code', '==', 'PA'], ['inspection_id_2', '!=', None]], # city and REMS_Property_ID are the only fields that could be used to geographically narrow this filter.
        #'primary_key_fields': ['rems_property_id'],
        'destination': 'ckan',
        'destination_file': 'mf_inspections_pa.csv',
        'package': housecat_package_id,
        'resource_name': 'HUD Multifamily Inspection Scores (Pennsylvania)',
        'upload_method': 'insert',
    },
    {
        'job_code': MultifamilyInspectionsSchema3().job_code, # 'mf_inspections_3'
        'source_type': 'http',
        'source_file': 'MF_Inspection_Report02252021.xls',
        'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/mfh/rems/remsinspecscores/remsphysinspscores', 'xls', 0, 1, 'nspection'),
        'encoding': 'binary',
        'rows_to_skip': 0,
        'schema': MultifamilyInspectionsSchema3,
        'filters': [['state_code', '==', 'PA'], ['inspection_id_3', '!=', None]], # city and REMS_Property_ID are the only fields that could be used to geographically narrow this filter.
        #'primary_key_fields': ['rems_property_id'],
        'destination': 'ckan',
        'destination_file': 'mf_inspections_pa.csv',
        'package': housecat_package_id,
        'resource_name': 'HUD Multifamily Inspection Scores (Pennsylvania)',
        'upload_method': 'insert',
    },
# All the jobs below are being commented out because the desired fields have not yet been identified.
#    {
#        'job_code': USDAProgramExitSchema().job_code, # 'usda_exit' # I only see one record that is in Allegheny County (based on State County FIPS Code).
#        'source_type': 'http',
#        #'source_file': 'USDA_RD_MHF_Program_Exit-2020-12-31.xlsx',
#        'source_full_url': scrape_nth_link('https://www.sc.egov.usda.gov/data/MFH.html', 'xlsx', 0, 2, regex='xit', verify=False),
#        # This web page has incorrectly configured certificates, so we'll need to route around that with requests.get(url, verify=False).
#        'ignore_certificate_errors': True,
#        'encoding': 'binary',
#        'rows_to_skip': 0,
#        'schema': USDAProgramExitSchema,
#        'filters': [['state_county_fips_code', '==', 42003], ['state', '==', 'PA']],
#        'always_wipe_data': True,
#        #'primary_key_fields': "Individual properties can be identified across databases
#        # by Borrower ID, followed by Project (Property?) ID, followed by Project Check Digit."
#        'destination': 'ckan',
#        'destination_file': 'usda_exit_ac.csv',
#        'resource_description': 'USDA Rural Program Exit (Allegheny County)',
#        'package': housecat_package_id,
#        'resource_name': USDAProgramExitSchema().job_code, # 'usda_exit'
#        'upload_method': 'insert',
#    },
#    {
#        'job_code': USDA514515ActiveSchema().job_code, # 'usda_active'
#        'source_type': 'http',
#        #'source_file': 'USDA_RD_MFH_Active_Projects-2021-02-17.xlsx'
#        'source_full_url': scrape_nth_link('https://www.sc.egov.usda.gov/data/MFH_section_515.html', 'xlsx', 0, 3, regex='Active', verify=False),
#        # This web page has incorrectly configured certificates, so we'll need to route around that with requests.get(url, verify=False).
#        'ignore_certificate_errors': True,
#        'encoding': 'binary',
#        'rows_to_skip': 0,
#        'schema': USDA514515ActiveSchema,
#        'filters': [['state_county_fips_code', '==', 42003], ['state_abbreviation', '==', 'PA']],
#        'always_wipe_data': True,
#        #'primary_key_fields': "Individual properties can be identified across databases
#        # by Borrower ID, followed by Project (Property?) ID, followed by Project Check Digit."
#        'destination': 'ckan',
#        'destination_file': 'usda_active_ac.csv',
#        'resource_description': 'Derived from https://www.sc.egov.usda.gov/data/MFH_section_515.html',
#        'package': housecat_package_id,
#        'resource_name': 'USDA Rural Development Multi-Family Section 514 and 515 Active (Allegheny County)',
#        'upload_method': 'insert',
#    },
#    {
#        'job_code': USDA514515TenantSchema().job_code, # 'usda_tenant'
#        'source_type': 'http',
#        #'source_file': 'USDA_RD_MFH_Tenant-2021-02-17.xlsx'
#        'source_full_url': scrape_nth_link('https://www.sc.egov.usda.gov/data/MFH_section_515.html', 'xlsx', 2, 3, regex='Tenant', verify=False),
#        # This web page has incorrectly configured certificates, so we'll need to route around that with requests.get(url, verify=False).
#        'ignore_certificate_errors': True,
#        'encoding': 'binary',
#        'rows_to_skip': 0,
#        'schema': USDA514515TenantSchema,
#        'filters': [['state_county_fips_code', '==', 42003], ['state_abbreviation', '==', 'PA']],
#        'always_wipe_data': True,
#        #'primary_key_fields': "Individual properties can be identified across databases
#        # by Borrower ID, followed by Project (Property?) ID, followed by Project Check Digit."
#        'destination': 'ckan',
#        'destination_file': 'usda_tenant_ac.csv',
#        'resource_description': f'Derived from https://www.sc.egov.usda.gov/data/MFH_section_515.html', #\n\njob_code: {USDA514515TenantSchema().job_code}',
#        'package': housecat_package_id,
#        'resource_name': 'USDA Rural Development Multi-Family Section 514 and 515 Tenant (Allegheny County)',
#        'upload_method': 'insert',
#    },
#    {
#        'job_code': USDA538Schema().job_code, # 'usda_538' # I'm not sure that any of these are in Allegheny County.
#        'source_type': 'http',
#        #'source_file': 'USDA_RD_MFHG538_2021-02-18.xls',
#        'source_full_url': scrape_nth_link('https://www.sc.egov.usda.gov/data/MFH.html', 'xls', 0, 1, regex='538', verify=False),
#        # This web page has incorrectly configured certificates, so we'll need to route around that with requests.get(url, verify=False).
#        'ignore_certificate_errors': True,
#        'encoding': 'binary',
#        'rows_to_skip': 0,
#        'schema': USDA538Schema,
#        'filters': [['state_abbreviation', '==', 'PA']], # Latitude, longitude, city, and ZIP code seem to be the available options for geographic filtering.
#        'always_wipe_data': True,
#        #'primary_key_fields': "Individual properties can be identified across databases
#        # by Borrower ID, followed by Project (Property?) ID, followed by Project Check Digit."
#        'destination': 'ckan',
#        'destination_file': 'usda_538_pa.csv',
#        'resource_description': f'Derived from file at https://www.sc.egov.usda.gov/data/MFH.html', #\n\njob_code: {USDA538Schema().job_code}',
#        'package': housecat_package_id,
#        'resource_name': 'USDA Rural Program Multi-Family Housing 538 (Pennsylvania)',
#        'upload_method': 'insert',
#    },
#    {
#        'job_code': HousingInventoryCountSchema().job_code, # 'hic' (related to homelessness)
#        'source_type': 'http',
#        #'source_file': '2019-Housing-Inventory-County-RawFile.xlsx',
#        'source_full_url': scrape_nth_link('https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/', 'xlsx', 4, None, regex='RawFile'),
#        'encoding': 'binary',
#        'rows_to_skip': 0,
#        'schema': HousingInventoryCountSchema,
#        'filters': [['coc\\id', '==', 1080]], # The "Continuum of Care" ID limits records to Allegheny County.
#        'always_wipe_data': True,
#        #'primary_key_fields': "Individual properties can be identified across databases
#        # by Borrower ID, followed by Project (Property?) ID, followed by Project Check Digit."
#        'destination': 'ckan',
#        'destination_file': 'usda_538_ac.csv',
#        'resource_description': f'Derived from file at https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/', #\n\njob_code: {HousingInventoryCountSchema().job_code}',
#        'package': housecat_package_id,
#        'resource_name': 'HUD Exchange Housing Inventory Count (Allegheny County)',
#        'upload_method': 'insert',
#    },
#    { # This one is incomplete because the desired fields have not been identified yet.
#        'job_code': MultifamilyTerminatedMortgagesSchema().job_code, #'terminated_mortgages'
#        # This Excel 2018 file includes all terminated HUD Multifamily insured mortgages. It includes the Holder and Servicer at the time the mortgage was terminated. The data is as of  March 1, 2021 and is updated monthly.
#        'source_type': 'http',
#        'source_file': 'CopyofFHA_BF90_RM_T_03012021.xlsx',
#        'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/comp/rpts/mfh/mf_f47t', 'xlsx', 1, 2, 'FHA'),
#        'updates': 'Monthly',
#        'encoding': 'binary',
#        'schema': MultifamilyTerminatedMortgagesSchema,
#        'filters': [['property_state', '==', 'PA']], # Location information includes city, state, and ZIP code.
#        'always_wipe_data': True,
#        #'primary_key_fields': ['hud_project_number'], # "HUD PROJECT NUMBER" seems pretty unique.
#        'destination': 'ckan',
#        'destination_file': 'terminated_mortgages_pa.csv',
#        'package': housecat_package_id,
#        'resource_name': 'HUD Terminated Multifamily Mortgages (Pennsylvania)',
#        'upload_method': 'insert',
#        'resource_description': f'Derived from https://www.hud.gov/program_offices/housing/comp/rpts/mfh/mf_f47t', #\n\njob code: {MultifamilyTerminatedMortgagesSchema().job_code}',
#    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
