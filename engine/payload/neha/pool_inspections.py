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

def extract_matched_values(pattern, string):
    potential_values = []
    if re.search(pattern, string, re.IGNORECASE):
        result = re.search(pattern, string, re.IGNORECASE)
        if result is not None:
            for group in result.groups():
                potential_values.append(group)
    return potential_values

# End network functions #

class ACPoolInspectionsSchema(pl.BaseSchema):
    job_code = 'ac'
    sr_num = fields.String(load_from='SR_NUM'.lower(), dump_to='_serial_number')
    facility_name = fields.String(load_from='FACILITY_NAME'.lower(), dump_to='Name of Aquatic Facility', allow_none=True)
    address = fields.String(load_from='STREET_NUMBER'.lower(), dump_to='Address', allow_none=True)
    street_number = fields.String(load_from='STREET_NUMBER'.lower(), load_only=True, dump_to='street_number', allow_none=True)
    street_name = fields.String(load_from='STREET_NAME'.lower(), load_only=True, dump_to='street_name', allow_none=True)
    apt_floor = fields.String(load_from='APT_FLOOR'.lower(), load_only=True, dump_to='apt_floor', allow_none=True)
    address_2 = fields.String(load_from='ADDRESS_2'.lower(), load_only=True, dump_to='address_2', allow_none=True)
    city = fields.String(load_from='CITY'.lower(), load_only=True, dump_to='city', allow_none=True)
    state = fields.String(load_from='STATE'.lower(), load_only=True, dump_to='state', allow_none=True)
    _zip = fields.String(load_from='ZIP'.lower(), load_only=True, dump_to='zip', allow_none=True)
    free_cl = fields.String(dump_only=True, dump_to='Free chlorine (ppm)', allow_none=True)
    municode = fields.Integer(load_from='MUNICODE'.lower(), dump_to='municode', allow_none=True)
    muniname = fields.String(load_from='MUNINAME'.lower(), dump_to='muniname', allow_none=True)
    ward = fields.Integer(load_from='WARD'.lower(), dump_to='ward', allow_none=True)
    lot_block = fields.String(load_from='LOT_BLOCK'.lower(), dump_to='lot_block', allow_none=True)
    entry_date = fields.String(load_from='ENTRY_DATE'.lower(), dump_to='entry_date', allow_none=True)
    abated_date = fields.String(load_from='ABATED_DATE'.lower(), dump_to='_abated_date', allow_none=True)
    is_planned = fields.String(load_from='IS_PLANNED'.lower(), dump_to='is_planned', allow_none=True)
    inspect_id = fields.String(load_from='INSPECT_ID'.lower(), dump_to='inspect_id', allow_none=True)
    beg_date = fields.String(load_from='BEG_DATE'.lower(), dump_to='beg_date', allow_none=True)
    end_date = fields.String(load_from='END_DATE'.lower(), dump_to='end_date', allow_none=True)
    inspect_number = fields.Integer(load_from='INSPECT_NUMBER'.lower(), dump_to='inspect_number', allow_none=True)
    action_id = fields.String(load_from='ACTION_ID'.lower(), dump_to='action_id', allow_none=True)
    beg_time = fields.String(load_from='BEG_TIME'.lower(), dump_to='beg_time', allow_none=True)
    end_time = fields.String(load_from='END_TIME'.lower(), dump_to='end_time', allow_none=True)
    insp_name = fields.String(load_from='INSP_NAME'.lower(), dump_to='insp_name', allow_none=True)
    nca_yn = fields.String(load_from='NCA_YN'.lower(), dump_to='nca_yn', allow_none=True)
    comments = fields.String(load_from='COMMENTS'.lower(), dump_to='comments', allow_none=True)
    violations = fields.String(load_from='VIOLATIONS'.lower(), dump_to='violations', allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def synthesize_address(self, data):
        address_parts = []
        f = 'street_number'
        if f in data and data[f] not in [None, '']:
            address_parts.append(data[f].strip())
        f = 'street_name'
        if f in data and data[f] not in [None, '']:
            address_parts.append(f'{data[f].strip()},')
        f = 'apt_floor'
        if f in data and data[f] not in [None, '']:
            address_parts.append(f'{data[f].strip()},')
        f = 'address_2'
        if f in data and data[f] not in [None, '']:
            address_parts.append(f'{data[f].strip()},')
        f = 'city'
        if f in data and data[f] not in [None, '']:
            if data[f].upper() in ['PGH']:
                address_parts.append('PITTSBURGH,')
            else:
                address_parts.append(f'{data[f].strip()},')
        f = 'state'
        if f in data and data[f] not in [None, '']:
            address_parts.append(data[f].strip())
        f = 'zip'
        if f in data and data[f] not in [None, '']:
            address_parts.append(data[f].strip())
        data['address'] = (' '.join(address_parts)).upper()

    @post_load
    def extract_free_cl(self, data):
        f_i = 'comments'
        f_o = 'free_cl' #'Free chlorine (ppm)'
        if f_i not in data or data[f_i] in [None, '']:
            data[f_o] = None
        else:
            pattern = re.compile("(?:FREE|F)\s*(?:CHLORINE|CL|C)\s*(?::|=|-|)?\s*(\d+.\d)\s*(?:PPM)?", re.IGNORECASE)
            matches = pattern.findall(data[f_i])
            failed = False
            if matches == [] and not failed:
                # Deal with this possibility:
                # Chlorine
                #   Free 5.0ppm
                #   Combined <0.2ppm
                pattern = re.compile("(?:CHLORINE|CL|C)\s*(?:FREE)\s*(?::|=|-|)?\s*(\d+.\d)\s*(?:PPM)?", re.IGNORECASE)
                matches = pattern.findall(data[f_i])
            if matches== [] and re.search('free', data[f_i], re.IGNORECASE) and not failed:
                # Failed to extract free chlorine value even though the word "free"
                # is present.
                print(f"Failed to extract free chlorine value even though the word 'free' is present in the text: {data[f_i]}.")
                data[f_o] = None
                failed = True
            if matches == [] and not failed:
                # Fallback search ("Cl" instead of "Free Cl")

                # Good clarity, safety equipment, permit, BPM.  pH 7.4, cl 4.0, cc 0, 150 GPM
                pattern = re.compile("(?:CHLORINE|CL|C)\s*(?::|=|-|)?\s*(\d+.\d)\s*(?:PPM)?", re.IGNORECASE)
                matches = pattern.findall(data[f_i])



            # [ ] How can we deal with deep-end/shallow-end readings?:
            # PH: 7.2 shallow 7.4 deep, FC:3.8 (SE)  3.4 (DE) CC:0 Flow


            if matches == [] and not failed:
                # Try splitting string on commas and parsing substrings to handle cases like these:
                    # Good clarity, permit, bac-T reports, safety equipment and 1st aid kit, BPM.  pH 7.6, cl 5, cc 0.
                    #Good clarity,permit,1st aid kit,aed,saety equipment.  7.2 pH, 5.0 cl, 0.2 cc, 180 gpm.
                    # This catches 3 more measurements.
                    parts = data[f_i].split(', ')
                    potential_values = []
                    value_in_back = "(?:CHLORINE|CL)\s*(?::|=|-|)?\s*(\d+.*\d*)\s*(?:PPM)?"
                    value_in_front = "(\d+.*\d*)\s*(?:PPM)?\s*(?::|=|-|)?\s*(?:CHLORINE|CL)"
                    for part in parts:
                        potential_values += extract_matched_values(value_in_back, part)
                        potential_values += extract_matched_values(value_in_front, part)
                    if len(potential_values) == 0:
                        print(f'No matches found in "{data[f_i]}')
                    elif len(potential_values) == 1:
                        data[f_o] = potential_values[0]
                    else:
                        print(f'Multiple matches found in "{data[f_i]}": {potential_values}')
            elif not failed:
                if len(matches) > 1:
                    print(f'Multiple matches found in "{data[f_i]}"')
                else:
                    data[f_o] = matches[0]
            if f_o in data and data[f_o] is not None:
                assert len(data[f_o]) < 6

        # FREE CHLORINE: 7.2 PPM FLOW RATE: 70 GPM
        #POOL READINGS: FREE CL 1.0 COMBINED CL 0.0 TOTAL CL 1.0...

# Good clarity, permit, bac-T reports, safety equipment and 1st aid kit, BPM.  pH 7.6, cl 5, cc 0.


# dfg

#_package_id = 'bb77b955-b7c3-4a05-ac10-448e4857ade4'

job_dicts = [
    {
        'job_code': ACPoolInspectionsSchema().job_code, # 'ac'
        'source_type': 'local',
        'source_file': 'NEHA_POOL_INSPECTIONS_08302021_VIOLATIONS.csv',
        #'source_full_url': 'https://www.hud.gov/sites/dfiles/Housing/documents/Initi_Endores_Firm%20Comm_DB_FY21_Q1.xlsx',
        #'source_full_url': scrape_nth_link('https://www.hud.gov/program_offices/housing/mfh/mfdata/mfproduction', 'xlsx', 0, 2, 'Q'),
        #'updates': 'Quarterly',
        'encoding': 'ISO-8859-1',
        'schema': ACPoolInspectionsSchema,
        #'filters': [['project_state', '==', 'PA']], # No county field. Just city and state. (And Pittsburgh is misspelled as "Pittsburg".)
        'always_wipe_data': True,
        #'primary_key_fields': ['fha_number'], # "HUD PROJECT NUMBER" seems pretty unique.
        'destination': 'local',
        'destination_file': 'ac_pool_inspections.csv',
        #'package': housecat_package_id,
        #'resource_name': 'HUD Multifamily Fiscal Year Production (Pennsylvania)',
        'upload_method': 'insert',
        #'resource_description': f'Derived from https://www.hud.gov/program_offices/housing/mfh/mfdata/mfproduction', #\n\njob code: {MultifamilyProductionInitialCommitmentSchema().job_code},'
        'custom_post_processing': check_for_empty_table, # This is necessary since an upstream change to filter values can easily result in zero-record tables.
    },
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
