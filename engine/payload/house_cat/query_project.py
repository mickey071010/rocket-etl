import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import write_to_csv
from engine.ckan_util import query_resource
from engine.notify import send_to_slack
from engine.parameters.local_parameters import SOURCE_DIR, PRODUCTION
from engine.post_processors import check_for_empty_table

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

from engine.payload.house_cat.data_sources import HouseCatFieldsSchema

class FieldsAndQuerySchema(HouseCatFieldsSchema):
    job_code = 'query_results'
    value = fields.String(load_from='value'.lower(), dump_to='value', allow_none=True)

# dfg
def find_all_linked_keys(id_field_name, id_field_value):
    # Load up deduplicated_index.csv.
    from engine.payload.house_cat._deduplicate import deduplicated_index_filename
    with open(deduplicated_index_filename, 'r') as h:
        reader = csv.DictReader(h)
        for row in reader:
            if row[id_field_name] == id_field_value:
                return row
    return None

def query_housing_project(id_field_name, id_field_value):
    # Get dict of IDs ({'property_id': 'WHATEVER', ...}) by 
    # combining user-entered field name and value (e.g., property_id = '803018501398')
    # with deduplicated_index.csv (which has ALL the project ID values).
    from engine.credentials import site, API_key
    from engine.leash_util import fill_bowl, empty_bowl

    deduplicated_index_record = find_all_linked_keys(id_field_name, id_field_value)
    # Find the other ID values associated with the one the user entered.
    assert deduplicated_index_record is not None
    id_fields_and_values = dict([t for t in deduplicated_index_record.items()])

    from engine.payload.house_cat.data_sources import id_fields_by_code
    utilized_ids_fields = list(set([item for sublist in id_fields_by_code.values() for item in sublist]))
    id_fields_and_values = dict([(k,v) for k,v in id_fields_and_values.items() if k in utilized_ids_fields and v != ''])

    records_by_job_code = {}
    rows_with_values = []
    with open('/Users/drw/WPRDC/etl/rocket-etl/output_files/house_cat/sources.csv') as g:
        reader = csv.DictReader(g)
        for source in reader:
            ic(source['job_code'])
            table_id_fields = source['id_fields'].split('|')
            ic(table_id_fields)
            list_of_dicts = []
            where_clauses = []
            resource_id = source['resource_id']

            for id_f, id_value in id_fields_and_values.items():
                if id_f in table_id_fields:
                    ic(id_f)
                    # Run queries
                    where_clauses.append(f"{id_f} = '{id_value}'")
                    
                    #q = f'SELECT * FROM "{resource_id}" WHERE {id_f} = \'{id_value}\''
            if len(where_clauses) > 0:
                fill_bowl(resource_id)
                q = f'SELECT * FROM "{resource_id}" WHERE {" OR ".join(where_clauses)}'
                print(q)
                list_of_dicts += query_resource(site, q, API_key)
                # Combine results to eliminate duplicates.
                ic(source['job_code'])
                ic(list_of_dicts)
                if len(list_of_dicts) > 1: 
                    ic(list_of_dicts)
                    print("Combining these results into one (if possible) based on _id fields.")
                    reduced_list__ids = list(set([d['_id'] for d in list_of_dicts]))
                    assert len(reduced_list__ids) == len(list_of_dicts)
                    reduced_list = []

            # Store the data so it can be tacked onto the fields.csv file.
            records_by_job_code[source['job_code']] = list_of_dicts

            for job_code, list_of_records in records_by_job_code.items():
                for record in list_of_records:
                    for field, value in record.items():
                        if field not in ['_id', '_geom', '_the_geom_webmercator', '_full_text']:
                            source['field'] = field
                            source['value'] = value
                            rows_with_values.append(dict(source))

    return rows_with_values

def generate_housing_project_file_from_multitable_query(job, **kwparameters):
    # Get field name and value (interactively?)
    id_field_name = input("Enter name of property ID field: ")
    id_field_value = input(f"Search for {id_field_name} = ")

    rows_with_values = query_housing_project(id_field_name, id_field_value)
    write_to_csv(job.target, rows_with_values)

from engine.payload.house_cat._parameters import housecat_tango_with_django_package_id #'3f6411c8-d03d-45b2-8225-673841e5c2b3'

job_dicts = [
    { # Really the simplest solution might be to modify _flatbread.py to first save the output locally
        # and then in a second stage upload them to CKAN, but the flaw in this plan is that some of
        # those jobs need CKAN's upsert capabilites (or at least a local SQLite database) to work correctly,
        # so it's really not simple. Another option would be a post-processing step that pulls the table
        # back down to the output directory, as that would take care of getting the file name right.
        'job_code': FieldsAndQuerySchema().job_code, # 'query_results'
        'source_type': 'local',
        'source_file': 'fields_with_values.csv',
        'schema': FieldsAndQuerySchema,
        #'filters': [['property_id', '!=', None]], # Might it be necessary to iterate through all the property fields
        # like this, upserting on each one?
        'custom_processing': generate_housing_project_file_from_multitable_query,
        'always_wipe_data': True, # This can be wiped since this is a local-only job.
        'destination': 'file',
        'destination_file': 'housing_project_sample.csv',
    }
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
