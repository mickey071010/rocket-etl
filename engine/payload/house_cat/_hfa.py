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


class HFALIHTCSchema(pl.BaseSchema):
    job_code = 'hfa_lihtc'
    pmindx = fields.String(load_from='PMINDX'.lower(), dump_to='pmindx')
    property_name = fields.String(load_from='Property Name'.lower(), dump_to='hud_property_name')
    lihtc_units = fields.Integer(load_from='LIHTC Units'.lower(), dump_to='assisted_units')
    lihtc_allocation = fields.Integer(load_from='LIHTC Allocation'.lower(), dump_to='lihtc_allocation') # There can be multiple
    # LIHTC records in the PHFA data. For instance "CONSTANTIN BUILDING" has two records, and the LIHTC Allocation
    # amounts add up to the lihtc_amount we have in lihtc_projects_pa.csv. 
    # In this case, the lihtc_projects_pa value for lihtc_year_allocated is 1996,
    # while the phfa-lihtc file has 1996 for the Allocation Year in one record and 1997 
    # for the Allocation Year in another.

    # phfa-lihtc: Placed in Service = 1998 for both records
    # lihtc_projects_pa: lihtc_year_allocated = 1999

    # The datanote field in LIHTCPUB.csv says "PREVIOUSLY LISTED AS PAA1998070." which may
    # explain these discrepancies.

    # phfa-lihtc has this last_yr_of_rca field, which in 92% of cases is 29 or 30, but ca be 28 through
    # 31, and also sometimes last_yr_of_rca == 0.

    # ADDISON TERRACE PHASE 2 discrepancy: ours (lihtc_amount == 774108), phfa-lithc (LIHTC Allocation == 0)
    # ALLEQUIPPA TERRACE PHASE 1A: ours (lihtc_amount = None), phfa-lihtc (LIHTC Allocation == 665017)
    #                               ours (assisted_units = None), phfa-lihtc (LIHTC Units == 196).
    # Others where PHFA has more data in its record than we get from LIHTC:
    # 3RD EAST HILLS, ALLEGHENY COMMONS EAST, ALLEQUIPPA TERRACE PHASE 1B, CARSON RETIREMENT RESIDENCE
    # 
    unit_restriction = fields.String(load_from='Unit Restriction'.lower(), dump_to='unit_restriction')
    #"While there are other distinctions, any development financed by either the 9% or 4% tax credit 
    # must comply with federal income limits and set-aside rules (20% at 50% AMI or 40% at 60% AMI)."

    # "40  at 60" == "40% at 60% AMI"
    # "20  at 50" == "20% at 50% AMI"
    # "0  at 0"   == "0% at 0% AMI"
    tax_credit_rate = fields.Float(load_from='4%_or_9%'.lower(), dump_to='tax_credit_rate') # The 9% tax credit tends to generate around 70% of a development’s equity while a 4% tax credit will generate around 30% of a development’s equity.
    allocation_year = fields.Integer(load_from='Allocation Year'.lower(), dump_to='lihtc_year_allocated', allow_none=True)
    placed_in_service_year = fields.Integer(load_from='Placed in Service Year'.lower(), dump_to='lihtc_year_placed_into_service', allow_none=True)
    last_yr_of_rca = fields.Integer(load_from='Last Yr of RCA'.lower(), dump_to='last_year_of_rca', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_ur(self, data):
        f = 'unit_restriction'
        ur_lookup = {
                "40  at 60": "40% at 60% AMI",
                "20  at 50": "20% at 50% AMI",
                "0  at 0": "0% at 0% AMI"
                }
        if f in data and data[f] != '':
            data[f] = ur_lookup[data[f]]

    @pre_load
    def fix_zero_years(self, data):
        fs = ['allocation_year', 'last_yr_of_rca', 'placed_in_service_year']
        for f in fs:
            if f in data and data[f] == '0':
                data[f] = None

class HFADemographics(pl.BaseSchema):
    job_code = 'hfa_demographics'
    pmindx = fields.String(load_from='PMINDX'.lower(), dump_to='pmindx')
    application_number = fields.String(load_from='Application Number'.lower(), dump_to='application_number') # Sometimes this is the state_id, but other times it isn't.
    s8cnid = fields.String(load_from='S8CNID'.lower(), dump_to='contract_id', allow_none=True)
    hud_rems = fields.String(load_from='HUD REMS'.lower(), dump_to='property_id', allow_none=True)
    fha_loan_id = fields.String(load_from='fha_#', dump_to='fha_loan_id', allow_none=True)
    project_name = fields.String(load_from='Project Name'.lower(), dump_to='hud_property_name')
    address = fields.String(load_from='Address'.lower(), dump_to='property_street_address')
    city_state_zip = fields.String(load_from='City, State, Zip'.lower(), load_only=True)
    city = fields.String(dump_to='city')
    state = fields.String(dump_to='state')
    zip_code = fields.String(dump_to='zip_code')
    legal_owner_entity = fields.String(load_from='Legal Owner Entity'.lower(), dump_to='owner_organization_name', allow_none=True)
    units = fields.Integer(load_from='Units'.lower(), dump_to='units')
    individuals_with_a_occ_type = fields.String(load_from='individuals_with_a:_occtype', dump_to='demographic')
    physical = fields.Boolean(load_from='Physical'.lower(), dump_to='physical', allow_none=False)
    mental = fields.Boolean(load_from='Mental'.lower(), dump_to='mental', allow_none=False)
    homeless = fields.Boolean(load_from='Homeless'.lower(), dump_to='homeless', allow_none=False)
    owner_representative = fields.String(load_from='Owner Representative'.lower(), dump_to='owner_representative')
    # For 800018475, the "Owner Representative" field maps to the property_manager_company in mf_subsidy_8_ac.csv.
    # but for 800237654, it is the "Management Agent" field that maps to the property_manager_company in mf_subsidy_8_ac.csv.
    non_profit = fields.Boolean(load_from='Non-Profit'.lower(), dump_to='non_profit', allow_none=True)
    management_agent = fields.String(load_from='Management Agent'.lower(), dump_to='management_agent')

    class Meta:
        ordered = True

    @pre_load
    def set_city_state_and_zip_code(self, data):
        f = 'city,_state,_zip'
        if f in data and data[f] not in ['', None]:
            city, state_zip = data[f].strip().split(', ')
            data['state'], data['zip_code'] = state_zip.split('  ')
            data['city'] = city

    @pre_load
    def trim_strings(self, data):
        fs = ['project_name', 'address', 'legal_owner_entity',
              'owner_representative', 'management_agent']
        for f in fs:
            if f in data and data[f] is not None:
                data[f] = data[f].strip()

    @pre_load
    def boolify(self, data):
        fs = ['physical', 'mental', 'homeless', 'non_profit']
        for f in fs:
            if f in data:
                if data[f] is None:
                    data[f] = False
                else:
                    data[f] = data[f] == 'X'

    @pre_load
    def fix_known_errors(self, data):
        f = 'huds_rems'
        if f in data and data[f] == '800018558':
            f2 = 'project_name'
            if f2 in data and data[f2].strip() == 'JOHN PAUL PLAZA':
                data[f] = '800018554'

        f = 'fha_#'
        if f in data and data[f] == '033-11076':
            f2 = 'project_name'
            if f2 in data and data[f2].strip() == 'ST JUSTIN PLAZA':
                data[f] = '03311061'

        f = 's8cnid'
        if f in data and data[f] == 'PAT28T861013':
            data[f] = 'PA28T861013'
        elif f in data and data[f] == 'PAT28T791005':
            data[f] = 'PA28T791005'

#housecat_package_id = 'bb77b955-b7c3-4a05-ac10-448e4857ade4'

job_dicts = [
    {
        'job_code': HFALIHTCSchema().job_code, #'hfa_lihtc'
        'source_type': 'local',
        'source_file': 'phfa_lihtc.csv',
        'schema': HFALIHTCSchema,
        'always_wipe_data': True,
        'destination': 'file',
        #'package': housecat_package_id,
        #'resource_name': 'Active HUD Multifamily Insured Mortgages (Pennsylvania)',
        'upload_method': 'insert',
        #'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
    {
        'job_code': HFADemographics().job_code, #'hfa_demographics'
        'source_type': 'local',
        'source_file': 'phfa_pgh_demographics.csv',
        'schema': HFADemographics,
        'always_wipe_data': True,
        'destination': 'file',
        #'package': housecat_package_id,
        #'resource_name': 'Active HUD Multifamily Insured Mortgages (Pennsylvania)',
        'upload_method': 'insert',
        #'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
