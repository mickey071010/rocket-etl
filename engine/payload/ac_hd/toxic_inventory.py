import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from unidecode import unidecode

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class ToxicTemplate(pl.BaseSchema):
    tri_facility_id = fields.String(load_from='TRI_FACILITY_ID'.lower(), dump_to='tri_facility_id')
    doc_ctrl_num = fields.String(load_from='DOC_CTRL_NUM'.lower(), dump_to='doc_ctrl_num')
    facility_name = fields.String(load_from='FACILITY_NAME'.lower(), dump_to='facility_name')
    street_address = fields.String(load_from='STREET_ADDRESS'.lower(), dump_to='street_address')
    city_name = fields.String(load_from='CITY_NAME'.lower(), dump_to='city_name')
    county_name = fields.String(load_from='COUNTY_NAME'.lower(), dump_to='county_name')
    state_county_fips_code = fields.String(load_from='STATE_COUNTY_FIPS_CODE'.lower(), dump_to='state_county_fips_code')
    state_abbr = fields.String(load_from='STATE_ABBR'.lower(), dump_to='state_abbr')
    zip_code = fields.String(load_from='ZIP_CODE'.lower(), dump_to='zip_code')
    region = fields.String(load_from='REGION'.lower(), dump_to='region')
    fac_closed_ind = fields.Boolean(load_from='FAC_CLOSED_IND'.lower(), dump_to='fac_closed_ind')
    asgn_federal_ind = fields.String(load_from='ASGN_FEDERAL_IND'.lower(), dump_to='asgn_federal_ind')
    asgn_agency = fields.String(load_from='ASGN_AGENCY'.lower(), dump_to='asgn_agency', allow_none=True)
    parent_co_db_num = fields.String(load_from='PARENT_CO_DB_NUM'.lower(), dump_to='parent_co_db_num', allow_none=True)
    parent_co_name = fields.String(load_from='PARENT_CO_NAME'.lower(), dump_to='parent_co_name')
    standardized_parent_company = fields.String(load_from='STANDARDIZED_PARENT_COMPANY'.lower(), dump_to='standardized_parent_company', allow_none=True)
    epa_registry_id = fields.String(load_from='EPA_REGISTRY_ID'.lower(), dump_to='epa_registry_id', allow_none=True)
    trade_secret_ind = fields.Boolean(load_from='TRADE_SECRET_IND'.lower(), dump_to='trade_secret_ind')
    reporting_year = fields.Integer(load_from='REPORTING_YEAR'.lower(), dump_to='reporting_year')
    cas_num = fields.String(load_from='CAS_NUM'.lower(), dump_to='cas_num', allow_none=True)
    elemental_metal_included = fields.String(load_from='ELEMENTAL_METAL_INCLUDED'.lower(), dump_to='elemental_metal_included', allow_none=True)
    chem_name = fields.String(load_from='CHEM_NAME'.lower(), dump_to='chem_name')
    list_3350 = fields.String(load_from='LIST_3350'.lower(), dump_to='list_3350')
    carcinogen = fields.String(load_from='CARCINOGEN'.lower(), dump_to='carcinogen')
    clean_air = fields.String(load_from='CLEAN_AIR'.lower(), dump_to='clean_air')
    primary_sic_code = fields.String(load_from='PRIMARY_SIC_CODE'.lower(), dump_to='primary_sic_code', allow_none=True)
    sic_codes = fields.String(load_from='SIC_CODES'.lower(), dump_to='sic_codes', allow_none=True)
    primary_naics_code = fields.Integer(load_from='PRIMARY_NAICS_CODE'.lower(), dump_to='primary_naics_code')
    naics_codes = fields.String(load_from='NAICS_CODES'.lower(), dump_to='naics_codes')
    industry_code = fields.Integer(load_from='INDUSTRY_CODE'.lower(), dump_to='industry_code') # Offsite matches up until this point
    srs_id = fields.String(load_from='SRS_ID'.lower(), dump_to='srs_id', allow_none=True) # Moved this one up to match the Offset file.

    class Meta:
        ordered = True

class ToxicAirSchema(ToxicTemplate):
    environmental_medium = fields.String(load_from='ENVIRONMENTAL_MEDIUM'.lower(), dump_to='environmental_medium')
    release_range_code = fields.String(load_from='RELEASE_RANGE_CODE'.lower(), dump_to='release_range_code', allow_none=True)
    total_release = fields.Float(load_from='TOTAL_RELEASE'.lower(), dump_to='total_release', allow_none=True)
    rel_est_amt = fields.Float(load_from='REL_EST_AMT'.lower(), dump_to='rel_est_amt')
    release_na = fields.String(load_from='RELEASE_NA'.lower(), dump_to='release_na')
    release_basis_est_code = fields.String(load_from='RELEASE_BASIS_EST_CODE'.lower(), dump_to='release_basis_est_code', allow_none=True)

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k.lower() == 'PARENT_CO_NAME' and v == 'NA':
                data[k] = 'No US Parent'
            elif k.lower() in ['asgn_agency', 'parent_co_db_num', 'standardized_parent_company', 'epa_registry_id', 'cas_num', 'primary_sic_code', 'sic_codes', 'release_range_code', 'total_release', 'release_basis_est_code', 'srs_id']:
                if v in ['NA']:
                    data[k] = None

    @pre_load
    def boolify(self, data):
        for k, v in data.items():
            if k.lower() in ['fac_closed_ind', 'trade_secret_ind']:
                if v in ['0', 0]:
                    data[k] = False
                elif v in ['1', 1]:
                    data[k] = True
                else:
                    data[k] = None

class ToxicWaterSchema(ToxicAirSchema):
    water_sequence_num = fields.Integer(load_from='WATER_SEQUENCE_NUM'.lower(), dump_to='water_sequence_num')
    stream_name = fields.String(load_from='STREAM_NAME'.lower(), dump_to='stream_name', allow_none=True)
    storm_water_na = fields.String(load_from='STORM_WATER_NA'.lower(), dump_to='storm_water_na')
    storm_water_percent = fields.Float(load_from='STORM_WATER_PERCENT'.lower(), dump_to='storm_water_percent', allow_none=True)
    srs_id = fields.String(load_from='SRS_ID'.lower(), dump_to='srs_id', allow_none=True)
    reach_code = fields.String(load_from='REACH_CODE'.lower(), dump_to='reach_code', allow_none=True)

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k.lower() == 'PARENT_CO_NAME' and v =='NA':
                data[k] = 'No US Parent'
            elif k.lower() in ['asgn_agency', 'parent_co_db_num', 'standardized_parent_company', 'epa_registry_id', 'elemental_metal_included', 'primary_sic_code', 'sic_codes', 'release_range_code', 'total_release', 'release_basis_est_code', 'stream_name', 'storm_water_percent', 'srs_id', 'reach_code']:
                if v in ['NA']:
                    data[k] = None

    @pre_load
    def fix_false_floats(self, data):
        for k, v in data.items():
            if k.lower() in ['srs_id', 'storm_water_na', 'reach_code', 'water_sequence_num']:
                if v not in ['NA', '', None]:
                    data[k] = str(int(float(data[k])))

class ToxicOffsiteSchema(ToxicTemplate):
    transfer_loc_num = fields.Integer(load_from='TRANSFER_LOC_NUM'.lower(), dump_to='transfer_loc_num')
    off_site_amount_sequence = fields.Integer(load_from='OFF_SITE_AMOUNT_SEQUENCE'.lower(), dump_to='off_site_amount_sequence')
    type_of_waste_management = fields.String(load_from='TYPE_OF_WASTE_MANAGEMENT'.lower(), dump_to='type_of_waste_management', allow_none=True)
    transfer_range_code = fields.String(load_from='TRANSFER_RANGE_CODE'.lower(), dump_to='transfer_range_code', allow_none=True)
    transfer_amt = fields.Float(load_from='TRANSFER_AMT'.lower(), dump_to='transfer_amt')
    transfer_est_na = fields.String(load_from='TRANSFER_EST_NA'.lower(), dump_to='transfer_est_na')
    transfer_basis_est_code = fields.String(load_from='TRANSFER_BASIS_EST_CODE'.lower(), dump_to='transfer_basis_est_code', allow_none=True)
    off_site_name = fields.String(load_from='OFF_SITE_NAME'.lower(), dump_to='off_site_name', allow_none=True)
    off_site_street_address = fields.String(load_from='OFF_SITE_STREET_ADDRESS'.lower(), dump_to='off_site_street_address', allow_none=True)
    transfer_city_name = fields.String(load_from='TRANSFER_CITY_NAME'.lower(), dump_to='transfer_city_name', allow_none=True)
    transfer_county_name = fields.String(load_from='TRANSFER_COUNTY_NAME'.lower(), dump_to='transfer_county_name', allow_none=True)
    transfer_state_abbr = fields.String(load_from='TRANSFER_STATE_ABBR'.lower(), dump_to='transfer_state_abbr', allow_none=True)
    transfer_zip_code = fields.String(load_from='TRANSFER_ZIP_CODE'.lower(), dump_to='transfer_zip_code', allow_none=True)
    transfer_country_code = fields.String(load_from='TRANSFER_COUNTRY_CODE'.lower(), dump_to='transfer_country_code', allow_none=True)
    rcra_num = fields.String(load_from='RCRA_NUM'.lower(), dump_to='rcra_num', allow_none=True)
    controlled_loc = fields.String(load_from='CONTROLLED_LOC'.lower(), dump_to='controlled_loc')
    transfer_loc_epa_registry_id = fields.String(load_from='TRANSFER_LOC_EPA_REGISTRY_ID'.lower(), dump_to='transfer_loc_epa_registry_id', allow_none=True)

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k.lower() == 'PARENT_CO_NAME' and v == 'NA':
                data[k] = 'No US Parent'
            elif k.lower() in ['asgn_agency', 'parent_co_db_num', 'standardized_parent_company', 'epa_registry_id', 'elemental_metal_included', 'primary_sic_code', 'sic_codes', 'type_of_waste_management', 'transfer_range_code', 'transfer_basis_est_code', 'off_site_name', 'off_site_street_address', 'transfer_city_name', 'transfer_county_name', 'transfer_state_abbr', 'transfer_zip_code', 'transfer_country_code', 'rcra_num', 'transfer_loc_epa_registry_id', 'srs_id']:
                if v in ['NA']:
                    data[k] = None

class ToxicFacilitiesSchema(pl.BaseSchema):
    _id = fields.String(load_from='ID'.lower(), dump_to='id')
    facility_name = fields.String(load_from='FACILITY_NAME'.lower(), dump_to='facility_name')
    street_address = fields.String(load_from='STREET_ADDRESS'.lower(), dump_to='street_address')
    city_name = fields.String(load_from='CITY_NAME'.lower(), dump_to='city_name')
    county_name = fields.String(load_from='COUNTY_NAME'.lower(), dump_to='county_name')
    state_county_fips_code = fields.String(load_from='STATE_COUNTY_FIPS_CODE'.lower(), dump_to='state_county_fips_code')
    state_abbr = fields.String(load_from='STATE_ABBR'.lower(), dump_to='state_abbr')
    zip_code = fields.String(load_from='ZIP_CODE'.lower(), dump_to='zip_code')
    region = fields.String(load_from='REGION'.lower(), dump_to='region')
    fac_closed_ind = fields.Boolean(load_from='FAC_CLOSED_IND'.lower(), dump_to='fac_closed_ind')
    mail_name = fields.String(load_from='MAIL_NAME'.lower(), dump_to='mail_name', allow_none=True)
    mail_street_address = fields.String(load_from='MAIL_STREET_ADDRESS'.lower(), dump_to='mail_street_address')
    mail_city = fields.String(load_from='MAIL_CITY'.lower(), dump_to='mail_city')
    mail_state_abbr = fields.String(load_from='MAIL_STATE_ABBR'.lower(), dump_to='mail_state_abbr')
    mail_province = fields.String(load_from='MAIL_PROVINCE'.lower(), dump_to='mail_province', allow_none=True)
    mail_country = fields.String(load_from='MAIL_COUNTRY'.lower(), dump_to='mail_country', allow_none=True)
    mail_zip_code = fields.String(load_from='MAIL_ZIP_CODE'.lower(), dump_to='mail_zip_code')
    asgn_federal_ind = fields.String(load_from='ASGN_FEDERAL_IND'.lower(), dump_to='asgn_federal_ind')
    asgn_agency = fields.String(load_from='ASGN_AGENCY'.lower(), dump_to='asgn_agency', allow_none=True)
    frs_id = fields.String(load_from='FRS_ID'.lower(), dump_to='frs_id', allow_none=True)
    parent_co_db_num = fields.String(load_from='PARENT_CO_DB_NUM'.lower(), dump_to='parent_co_db_num', allow_none=True)
    parent_co_name = fields.String(load_from='PARENT_CO_NAME'.lower(), dump_to='parent_co_name', allow_none=True)
    fac_latitude = fields.String(load_from='FAC_LATITUDE'.lower(), dump_to='fac_latitude', allow_none=True)
    fac_longitude = fields.String(load_from='FAC_LONGITUDE'.lower(), dump_to='fac_longitude', allow_none=True)
    pref_latitude = fields.Float(load_from='PREF_LATITUDE'.lower(), dump_to='pref_latitude', allow_none=True)
    pref_longitude = fields.Float(load_from='PREF_LONGITUDE'.lower(), dump_to='pref_longitude', allow_none=True)
    pref_accuracy = fields.Integer(load_from='PREF_ACCURACY'.lower(), dump_to='pref_accuracy', allow_none=True)
    pref_collect_meth = fields.String(load_from='PREF_COLLECT_METH'.lower(), dump_to='pref_collect_meth', allow_none=True)
    pref_desc_category = fields.String(load_from='PREF_DESC_CATEGORY'.lower(), dump_to='pref_desc_category', allow_none=True)
    pref_horizontal_datum = fields.String(load_from='PREF_HORIZONTAL_DATUM'.lower(), dump_to='pref_horizontal_datum', allow_none=True)
    pref_source_scale = fields.String(load_from='PREF_SOURCE_SCALE'.lower(), dump_to='pref_source_scale', allow_none=True)
    pref_qa_code = fields.String(load_from='PREF_QA_CODE'.lower(), dump_to='pref_qa_code', allow_none=True)
    asgn_partial_ind = fields.Boolean(load_from='ASGN_PARTIAL_IND'.lower(), dump_to='asgn_partial_ind')
    asgn_public_contact = fields.String(load_from='ASGN_PUBLIC_CONTACT'.lower(), dump_to='asgn_public_contact', allow_none=True)
    asgn_public_phone = fields.String(load_from='ASGN_PUBLIC_PHONE'.lower(), dump_to='asgn_public_phone', allow_none=True)
    asgn_public_phone_ext = fields.String(load_from='ASGN_PUBLIC_PHONE_EXT'.lower(), dump_to='asgn_public_phone_ext', allow_none=True)
    asgn_public_contact_email = fields.String(load_from='ASGN_PUBLIC_CONTACT_EMAIL'.lower(), dump_to='asgn_public_contact_email', allow_none=True)
    bia_code = fields.String(load_from='BIA_CODE'.lower(), dump_to='bia_code', allow_none=True)
    standardized_parent_company = fields.String(load_from='STANDARDIZED_PARENT_COMPANY'.lower(), dump_to='standardized_parent_company', allow_none=True)
    epa_registry_id = fields.String(load_from='EPA_REGISTRY_ID'.lower(), dump_to='epa_registry_id', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k.lower() == 'PARENT_CO_NAME' and v == 'NA':
                data[k] = 'No US Parent'
            elif k.lower() in ['mail_name', 'mail_province', 'mail_country', 'asgn_agency', 'frs_id', 'parent_co_db_num', 'parent_co_name', 'fac_latitude', 'fac_longitude', 'pref_latitude', 'pref_longitude', 'pref_accuracy', 'pref_collect_meth', 'pref_desc_category', 'pref_horizontal_datum', 'pref_source_scale', 'pref_qa_code', 'asgn_public_contact', 'asgn_public_phone', 'asgn_public_contact_email', 'bia_code', 'standardized_parent_company', 'asgn_public_phone_ext', 'epa_registry_id']:
                if v in ['NA']:
                    data[k] = None

    @pre_load
    def boolify(self, data):
        for k, v in data.items():
            if k.lower() in ['fac_closed_ind', 'asgn_partial_ind']:
                if v in ['0', 0]:
                    data[k] = False
                elif v in ['1', 1]:
                    data[k] = True
                else:
                    data[k] = None

toxic_releases_package_id = TEST_PACKAGE_ID

job_dicts = [
    {
        'job_code': 'tri_air',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'tri_air.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': ToxicAirSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': toxic_releases_package_id,
        'resource_name': f'TRI Air',
    },
    {
        'job_code': 'tri_water',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'tri_water.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': ToxicWaterSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': toxic_releases_package_id,
        'resource_name': f'TRI Water',
    },
    {
        'job_code': 'tri_land',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'tri_land.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': ToxicAirSchema, # ToxicAir and ToxicLand schemas are identical.
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': toxic_releases_package_id,
        'resource_name': f'TRI Land',
    },
    {
        'job_code': 'tri_underground',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'tri_underground.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': ToxicAirSchema, # ToxicAir, ToxicLand, and ToxicUnderground schemas are identical.
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': toxic_releases_package_id,
        'resource_name': f'TRI Underground',
    },
    {
        'job_code': 'tri_offsite',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'tri_offsite.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': ToxicOffsiteSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': toxic_releases_package_id,
        'resource_name': f'TRI Offsite',
    },
    {
        'job_code': 'tri_facilities',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'tri_facilities.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': ToxicFacilitiesSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'package': toxic_releases_package_id,
        'resource_name': f'TRI Facilities',
    },
]
