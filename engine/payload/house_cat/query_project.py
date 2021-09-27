import csv, json, requests, sys, traceback, re, time
from datetime import datetime
from dateutil import parser
from pprint import pprint
from collections import OrderedDict, defaultdict

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import write_to_csv
from engine.ckan_util import query_resource, get_resource_fields
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
    id_fields_and_values_dict = dict([t for t in deduplicated_index_record.items()])

    from engine.payload.house_cat.data_sources import id_fields_by_code
    utilized_ids_fields = list(set([item for sublist in id_fields_by_code.values() for item in sublist]))
    pipe_delimited_id_fields_and_values = [(k,v) for k,v in id_fields_and_values_dict.items() if k in utilized_ids_fields and v != '']

    # Deal with the possibility of a case like this: SELECT * FROM "a6b93b7b-e04e-42c9-96f9-ee788e4f0978" WHERE pmindx = '394|9731'
    id_fields_and_values = []
    for id_f, id_v in pipe_delimited_id_fields_and_values:
        if re.match('|', id_v) is not None:
            id_values = id_v.split('|')
        else:
            id_values = [id_v]
        for value in id_values:
            id_fields_and_values.append( (id_f, value) )

    value_list_by_job_code_and_field = defaultdict(lambda: defaultdict(list))
    fields_by_job_code = {}

    with open('/Users/drw/WPRDC/etl/rocket-etl/output_files/house_cat/sources.csv') as g:
        reader = csv.DictReader(g)
        for source in reader:
            job_code = source['job_code']
            table_id_fields = source['id_fields'].split('|')
            list_of_dicts = []
            resource_id = source['resource_id']

            fields_by_job_code[job_code] = get_resource_fields(site, resource_id, API_key)[0]

            where_clauses = []
            for id_f, id_value in id_fields_and_values:
                if id_f in table_id_fields:
                    # Combine clauses to eliminate duplicates.
                    where_clauses.append(f"{id_f} = '{id_value}'")

            if len(where_clauses) > 0:
                fill_bowl(resource_id)
                q = f'SELECT * FROM "{resource_id}" WHERE {" OR ".join(where_clauses)}'
                list_of_dicts += query_resource(site, q, API_key)
                if len(list_of_dicts) > 1:
                    print(f"List of length {len(list_of_dicts)} detected for query '{q}' and job_code {job_code}.")

            for record in list_of_dicts:
                for field, value in record.items():
                    if field not in ['_id', '_geom', '_the_geom_webmercator', '_full_text']:
                        source['field'] = field
                        source['value'] = value
                        value_list_by_job_code_and_field[job_code][field].append(value)

    return value_list_by_job_code_and_field

def generate_housing_project_file_from_multitable_query(job, **kwparameters):
    # Get field name and value (interactively)
    id_field_name = input("Enter name of property ID field: ")
    id_field_value = input(f"Search for {id_field_name} = ")

    value_list_by_job_code_and_field = query_housing_project(id_field_name, id_field_value)
    #write_to_csv(job.target, rows_with_values)
    other_path = re.sub('fields', 'ordered_fields', job.target)

    with open('/Users/drw/WPRDC/etl/rocket-etl/output_files/house_cat/fields.csv') as g:
        reader = csv.DictReader(g)
        all_rows_with_values = []
        for row in reader:
            job_code = row['job_code']
            value_list_by_field = value_list_by_job_code_and_field.get(job_code, defaultdict(None))
            value_list = value_list_by_field.get(row['field'], [])
            row['value'] = '|'.join([str(v) for v in value_list]) # Serialize lists of results for CSV output
            all_rows_with_values.append(dict(row))

        ic(other_path)
        write_to_csv(other_path, all_rows_with_values)

def pull_fields_for_project(project, fields_to_query_by_resource_id, table_details_by_resource_id, id_fields_and_values, fill):
    from engine.credentials import site, API_key
    from engine.leash_util import fill_bowl
    for resource_id, fields in fields_to_query_by_resource_id.items():
        # For each resource_id, figure out which fields to query and do a
        # SELECT [list of fields] FROM {resource_id}
        if fields != []:
            table_details = table_details_by_resource_id[resource_id]
            print(f"Querying {table_details['job_code']}...")
            source_name = table_details['source_name']
            table_id_fields = table_details['id_fields']
            # Find a matching id_field.


            fields_to_pull = fields + table_id_fields
            if fill:
                fill_bowl(resource_id)

            where_clauses = []
            for id_f, id_value in id_fields_and_values:
                if id_f in table_id_fields:
                    # Combine clauses to eliminate duplicates.
                    where_clauses.append(f"{id_f} = '{id_value}'")

            if len(where_clauses) > 0:
                q = f"SELECT {', '.join(fields_to_pull)}, '{source_name}' as source FROM \"{resource_id}\" WHERE {' OR '.join(where_clauses)}"

            #query_housing_project(id_field_name, id_field_value) already does something similar
                results = query_resource(site, q, API_key)
                # Merge each query result with the project dict.
                #if len(results) == 1:
                #    project = {**project, **results[0]}
                if len(results) > 0:
                    pipe_delimited_dict = dict(results[0])
                    for result in results[1:]:
                        for k, v in result.items():
                            if k not in ['source'] + table_id_fields:
                                pipe_delimited_dict[k] = str(pipe_delimited_dict[k]) + '|' + str(v)

                    project = {**project, **pipe_delimited_dict}
    return project

def query_all_projects(job, **kwparameters):
    # Join house_cat_projectindex with a few other fields
    # like subsidy_expiration_date, program_type, and subsidy_data_source (all connected to make subsidies_ac.csv)
    # [subsidy_expiration_date and program_type from mf_subsidy_loans and ] table name converts to subsidy_data_source
    # but also LIHTC dates
    # and inspection scores
    # since these are the fields that Bob wants,
    # but collected for ALL properties.

    # Get field names (interactively)
    field_names_string = input("Enter a space-delimited list of field names to query: ").strip()
    field_names = field_names_string.split(' ')

    # Where do we get each field from?
    # Review fields.csv to find out where they can be found.

    #write_to_csv(job.target, rows_with_values)
    #other_path = re.sub('fields', 'ordered_fields', job.target)

    table_details_by_resource_id = defaultdict(dict)
    fields_to_query_by_resource_id = defaultdict(list)
    source_name_by_resource_id = {}
    with open('/Users/drw/WPRDC/etl/rocket-etl/output_files/house_cat/fields.csv') as g:
        reader = csv.DictReader(g)
        #value_list_by_job_code_and_field = query_fields(field_names)
        all_rows_with_values = []
        for row in reader:
            resource_id = row['resource_id']
            d = {'id_fields': row['id_fields'].split('|'),
                'source_name': row['source_name'],
                'job_code': row['job_code']}
            table_details_by_resource_id[resource_id] = d
            field = row['field']
            if field in field_names:
                fields_to_query_by_resource_id[resource_id].append(row['field'])
            source_name_by_resource_id[resource_id] = row['source_name']

    # Iterate through deduplicated_index.csv and use all those ID fields to query the tables
    # pulling the relevant code out of query_housing_project.
    from engine.payload.house_cat.data_sources import id_fields_by_code
    utilized_ids_fields = list(set([item for sublist in id_fields_by_code.values() for item in sublist]))

    with open('/Users/drw/WPRDC/etl/rocket-etl/engine/payload/house_cat/deduplicated_index.csv') as g:
        reader = csv.DictReader(g)
        results = []
        fill = True
        customized_records = []
        for n, project in enumerate(reader):
            pipe_delimited_id_fields_and_values = [(k,v) for k,v in project.items() if k in utilized_ids_fields and v != '']
            id_fields_and_values = []
            for id_f, id_v in pipe_delimited_id_fields_and_values:
                if re.match('|', id_v) is not None:
                    id_values = id_v.split('|')
                else:
                    id_values = [id_v]
                for value in id_values:
                    id_fields_and_values.append( (id_f, value) )

            result = pull_fields_for_project(project, fields_to_query_by_resource_id, table_details_by_resource_id, id_fields_and_values, fill)

            ic(result)
            time.sleep(0.05)
            customized_records.append(result)
            fill = False
            print(n)

    ic(job.destination_file_path)
    actual_destination_file_path = '/'.join(job.destination_file_path.split('/')[:-1]) + '/custom_table.csv'

    ordered_keys = ['index', 'hud_property_name', 'property_street_address', 'municipality_name', 'city', 'zip_code']
    ordered_keys += ['latitude', 'longitude', 'census_tract', 'units', 'scattered_sites']
    ordered_keys += ['property_id', 'normalized_state_id', 'development_code', 'pmindx', 'lihtc_project_id']
    ordered_keys += ['contract_id', 'fha_loan_id', 'crowdsourced_id', 'status']
    ordered_keys += field_names
    ordered_keys += ['source', 'source_file']

    extant_keys = set()
    for record in customized_records:
        extant_keys.update(list(record.keys()))

    for key in ordered_keys:
        assert key in extant_keys

    write_to_csv(actual_destination_file_path, customized_records, ordered_keys)

    print("Next step: Fix the 'source' and 'source_file' fields, which I think are being overwritten by the latest query, rather than accumulating across queries.")

from engine.payload.house_cat._parameters import housecat_tango_with_django_package_id #'3f6411c8-d03d-45b2-8225-673841e5c2b3'

job_dicts = [
    { # Really the simplest solution might be to modify _flatbread.py to first save the output locally
        # and then in a second stage upload them to CKAN, but the flaw in this plan is that some of
        # those jobs need CKAN's upsert capabilites (or at least a local SQLite database) to work correctly,
        # so it's really not simple. Another option would be a post-processing step that pulls the table
        # back down to the output directory, as that would take care of getting the file name right.
        'job_code': FieldsAndQuerySchema().job_code, # 'query_one_project'
        'source_type': 'local',
        'source_file': 'fields_with_values.csv',
        'schema': FieldsAndQuerySchema,
        #'filters': [['property_id', '!=', None]], # Might it be necessary to iterate through all the property fields
        # like this, upserting on each one?
        'custom_processing': generate_housing_project_file_from_multitable_query,
        'always_wipe_data': True, # This can be wiped since this is a local-only job.
        'destination': 'file',
        'destination_file': 'housing_project_sample.csv',
    },
    { # A terribly inefficient way to query all housing projects for certain fields.
        'job_code': 'query_all_projects',
        'source_type': 'local',
        'source_file': 'fields_with_values.csv',
        #'schema': FieldsAndQuerySchema,
        #'filters': [['property_id', '!=', None]], # Might it be necessary to iterate through all the property fields
        # like this, upserting on each one?
        'custom_processing': query_all_projects,
        'always_wipe_data': True, # This can be wiped since this is a local-only job.
        'destination': 'file',
        'destination_file': 'empty_file.csv',
    }
]

assert len(job_dicts) == len({d['job_code'] for d in job_dicts}) # Verify that all job codes are unique.
