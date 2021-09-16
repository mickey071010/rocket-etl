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
    lihtc_allocation = fields.Integer(load_from='LIHTC Allocation'.lower(), dump_to='total_lihtc_allocation') # There can be multiple
    # LIHTC records in the PHFA data. For instance "CONSTANTIN BUILDING" has two records, and the LIHTC Allocation
    # amounts add up to the lihtc_amount we have in lihtc_projects_pa.csv.
    # In this case, the lihtc_projects_pa value for lihtc_year_allocated is 1996,
    # while the phfa-lihtc file has 1996 for the Allocation Year in one record and 1997
    # for the Allocation Year in another.

    # phfa-lihtc: Placed in Service = 1998 for both records
    # lihtc_projects_pa: lihtc_year_allocated = 1999

    # The datanote field in LIHTCPUB.csv says "PREVIOUSLY LISTED AS PAA1998070." which may
    # explain these discrepancies.

    # phfa-lihtc has this last_yr_of_rca field, which in 92% of cases is 29 or 30, but can be 28 through
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
    tax_credit_rate = fields.Float(load_from='4%_or_9%'.lower(), load_only=True) # The 9% tax credit tends to generate around 70% of a development’s equity while a 4% tax credit will generate around 30% of a development’s equity.
    # "The LIHTC is designed to subsidize either 30 percent or 70 percent of the low-income unit costs in a project. The 30 percent subsidy, which is known as the so-called automatic 4 percent tax credit, covers new construction that uses additional subsidies or the acquisition cost of existing buildings. The 70 percent subsidy, or 9 percent tax credit, supports new construction without any additional federal subsidies."
    # This can be mapped to the 1/2 options in the existing lihtc_credit field.
    lihtc_credit = fields.String(dump_to='lihtc_credit', allow_none=True)
    allocation_year = fields.Integer(load_from='Allocation Year'.lower(), dump_to='lihtc_year_allocated', allow_none=True)
    placed_in_service_year = fields.Integer(load_from='Placed in Service Year'.lower(), dump_to='lihtc_year_in_service', allow_none=True)
    last_yr_of_rca = fields.Integer(load_from='Last Yr of RCA'.lower(), dump_to='last_year_of_rca', allow_none=True)

    class Meta:
        ordered = True

    @post_load
    def set_credit(self, data):
        if 'tax_credit_rate' in data:
            value = data['tax_credit_rate']
            if str(value) == '0.04':
                data['lihtc_credit'] = '70 percent present value'
            elif str(value) == '0.09':
                data['lihtc_credit'] = '30 percent present value'
            elif value in [None, '', ' ']:
                data['lihtc_credit'] = None
            else:
                raise ValueError(f"set_credit() does not know how to translate a tax_credit_rate value of {data['tax_credit_rate']}.")

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

    @pre_load
    def trim_strings(self, data):
        fs = ['property_name']
        for f in fs:
            if f in data and data[f] is not None:
                data[f] = data[f].strip()


class HFADemographics(pl.BaseSchema):
    job_code = 'hfa_demographics'
    pmindx = fields.String(load_from='PMINDX'.lower(), dump_to='pmindx')
    application_number = fields.String(load_from='Application Number'.lower(), dump_to='application_number') # Sometimes this is the state_id, but other times it isn't.
    state_id = fields.String(dump_only=True, dump_to='state_id', allow_none=True)
    normalized_state_id = fields.String(dump_only=True, dump_to='normalized_state_id', allow_none=True)
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
    assisted_units = fields.Integer(load_from='Units'.lower(), dump_to='assisted_units') # This has been checked to match up with our "assisted_units" field for a few cases.
    individuals_with_a_occ_type = fields.String(load_from='individuals_with_a:_occtype', dump_to='demographic')
    physical = fields.Boolean(load_from='Physical'.lower(), dump_to='physical_disability_housing', allow_none=False) # The expansion to e.g.,
    mental = fields.Boolean(load_from='Mental'.lower(), dump_to='mental_disability_housing', allow_none=False) # "physically_disability_housing"
    homeless = fields.Boolean(load_from='Homeless'.lower(), dump_to='homeless_housing', allow_none=False) # is a guess at what the PHFA is thinking.
    # These fields have strong but incomplete fit with the client_group_[name|type] fields, which have values like
    # "Partially elderly handicapped", "Partially physically handicapped", "Wholly physically disabled",
    # "Chronically Mentally Ill", but nothing about homelessness (that I can find).
    owner_representative = fields.String(load_from='Owner Representative'.lower(), dump_to='owner_representative')
    # Sometimes this is the same as the owner_organization_name, sometimes it matches the property_manager_company,
    # but sometimes it's something else. This doesn't cleanly map to anything, so I'm leaving it as its own field.

    non_profit = fields.Boolean(load_from='Non-Profit'.lower(), load_only=True, allow_none=True) #, dump_to='is_non_profit'
    #is_non_profit = fields.String(load_from='Non-Profit'.lower(), load_only=True, allow_none=True) # I tried merging the PHFA
    #owner_type = fields.String(dump_to='owner_type', allow_none=True) # Non-Profit value into the owner_type but
    # it didn't work so well. 1) It's not clear if it should be owner_type or property_manager_type. 2) The
    # Non-Profit value conflicted with the existing owner_type in the one record that was checkable against
    # mf_subsidy_8.
    management_agent = fields.String(load_from='Management Agent'.lower(), dump_to='property_manager_company') # These values
    # match best (but not perfectly) with the property_management_company values obtained from mf_subsidy_8.
    #scattered_site_ind = fields.Boolean(load_from='address', dump_to='scattered_site_ind')
    scattered_sites = fields.Boolean(load_from='scattered_sites', dump_to='scattered_sites')

    class Meta:
        ordered = True

    #@pre_load
    #def set_owner_type(self, data):
    #    f = 'non_profit'
    #    if f in data and data[f] not in [None, '', ' ']:
    #        if data[f] == 'X':
    #            data['owner_type'] = 'Non-Profit'

    @pre_load
    def fix_scattered_site_ind(self, data):
        f2 = 'scattered_sites'

        fs = ['address']
        for i in fs:
            if i in data and data[i] is not None:
                if re.search('scattered', data[i], re.IGNORECASE) is not None:
                    data[f2] = True

    @pre_load
    def standardize_fha_loan_id(self, data):
        f = 'fha_#'
        if f in data and data[f] is not None and re.search('-', data[f]) is not None:
            data[f] = re.sub('-', '', data[f])

    @post_load
    def fix_state_id(self, data):
        f0 = 'application_number'
        f = 'state_id'
        if f0 in data and data[f0] not in ['', None]:
            state_ids = data[f0].split('/')
            state_id = None
            for s in state_ids:
                if s[:2] == 'TC':
                    state_id = s
            data[f] = state_id
            data['normalized_state_id'] = state_id

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

class ApartmentDistributionSchema(pl.BaseSchema):
    job_code = 'hfa_apartment_distributions'
    pmindx = fields.Integer(load_from='PMINDX'.lower(), dump_to='pmindx') # [ ] Figure out how to change this to phfa_id
    # in all the tango_with_django.py code.
    project_name = fields.String(load_from='Project Name'.lower(), dump_to='hud_property_name')
    total_units = fields.Integer(load_from='Total Units'.lower(), dump_to='total_units')
    count_0br = fields.Integer(load_from='EFF'.lower(), dump_to='count_0br', allow_none=True)
    count_1br = fields.Integer(load_from='apartment_distribution:_1br', dump_to='count_1br', allow_none=True)
    count_2br = fields.Integer(load_from='2BR'.lower(), dump_to='count_2br', allow_none=True)
    count_3br = fields.Integer(load_from='3BR'.lower(), dump_to='count_3br', allow_none=True)
    count_4br = fields.Integer(load_from='4BR'.lower(), dump_to='count_4br', allow_none=True)
    count_5br = fields.Integer(load_from='5BR'.lower(), dump_to='count_5br', allow_none=True)
    count_6br = fields.Integer(load_from='6BR'.lower(), dump_to='count_6br', allow_none=True)
    income_targeting_1 = fields.Integer(load_from='income_targeting:_+1', dump_to='plus_1_manager_unit', allow_none=True) # "+1" indicates the unit(s) is a plus one unit type.
    _20 = fields.Integer(load_from='20'.lower(), dump_to='20percent_ami_limit', allow_none=True)
    _30 = fields.Integer(load_from='30'.lower(), dump_to='30percent_ami_limit', allow_none=True)
    _40 = fields.Integer(load_from='40'.lower(), dump_to='40percent_ami_limit', allow_none=True)
    _50 = fields.Integer(load_from='50'.lower(), dump_to='50percent_ami_limit', allow_none=True)
    _60 = fields.Integer(load_from='60'.lower(), dump_to='60percent_ami_limit', allow_none=True)
    _80 = fields.Integer(load_from='80'.lower(), dump_to='80percent_ami_limit', allow_none=True)
    mr = fields.Integer(load_from='MR'.lower(), dump_to='market_rate', allow_none=True)
    o = fields.Integer(load_from='O'.lower(), dump_to='other_income_limit', allow_none=True) # An Other unit type, not between market rate - 80% median income
    uncategorized = fields.Integer(load_from='Uncategorized'.lower(), dump_to='uncategorized_income_limit', allow_none=True)
    subsidies_81 = fields.Integer(load_from='subsidies:_81', dump_to='units_w_section_811_subsidy', allow_none=True) # Units with Section 811 subsidy
    fm = fields.Integer(load_from='FM'.lower(), dump_to='units_w_section_8_fair_market_rent', allow_none=True) # Section 8 Fair Market Rent
    hv = fields.Integer(load_from='HV'.lower(), dump_to='units_w_housing_vouchers', allow_none=True) # Housing Vouchers
    mu = fields.Integer(load_from='MU'.lower(), dump_to='units_w_staff_unit', allow_none=True) # Manager/Staff Unit
    o2 = fields.Integer(load_from='O'.lower(), dump_to='units_w_other_subsidy_type', allow_none=True) # Other subsidy type
    pb = fields.Integer(load_from='PB'.lower(), dump_to='units_w_project_based_section_8_certificate', allow_none=True) # Project Based Section 8 Certificate
    uncategorized_subsidy = fields.Integer(load_from='Uncategorized'.lower(), dump_to='units_w_uncategorized_subsidy', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def trim_strings(self, data):
        fs = ['project_name']
        for f in fs:
            if f in data and data[f] is not None:
                data[f] = data[f].strip()


phfa_package_id = '06ea7b14-3d37-4fa9-8a27-ed8d4fcb6d3e'

job_dicts = [
    {
        'job_code': HFALIHTCSchema().job_code, #'hfa_lihtc'
        'source_type': 'local',
        'source_file': 'phfa_lihtc.csv',
        'schema': HFALIHTCSchema,
        'always_wipe_data': True,
        'destination': 'ckan',
        'package': phfa_package_id,
        'resource_name': 'LIHTC Data from PHFA (Allegheny County)',
        'upload_method': 'insert',
        'resource_description': 'Derived from a file sent by the PHFA',
        #'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
    {
        'job_code': HFADemographics().job_code, #'hfa_demographics'
        'source_type': 'local',
        'source_file': 'phfa_pgh_demographics.csv',
        'schema': HFADemographics,
        'always_wipe_data': True,
        'destination': 'ckan',
        'package': phfa_package_id,
        'resource_name': 'Demographics by Housing Project from PHFA (Allegheny County)',
        'upload_method': 'insert',
        'resource_description': 'Derived from a file sent by the PHFA',
        #'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
    {
        'job_code': ApartmentDistributionSchema().job_code, # 'hfa_apartment_distributions'
        'source_type': 'local',
        'source_file': 'phfa_pgh_apartment_distribution.csv',
        'schema': ApartmentDistributionSchema,
        'always_wipe_data': True,
        'destination': 'ckan',
        'package': phfa_package_id,
        'resource_name': 'Apartment Distributions, Income Limits, and Subsidies by Housing Project from PHFA (Allegheny County)',
        'upload_method': 'insert',
        'resource_description': 'Derived from a file sent by the PHFA',
        #'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
