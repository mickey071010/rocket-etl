import os, ckanapi, re, sys, requests, csv, json, decimal
from datetime import datetime
# It's also possible to do this in interactive mode:
# > sudo su -c "sftp -i /home/sds25/keys/pitt_ed25519 pitt@ftp.pittsburghpa.gov" sds25
from engine.wprdc_etl import pipeline as pl
from engine.wprdc_etl.pipeline.schema import NullSchema
from engine.leash_util import fill_bowl
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.parameters.local_parameters import SETTINGS_FILE

from icecream import ic

from engine.credentials import site, API_key as API_KEY
from engine.parameters.local_parameters import SOURCE_DIR, WAITING_ROOM_DIR, DESTINATION_DIR

from engine.ckan_util import (set_resource_parameters_to_values,
        set_package_parameters_to_values, find_resource_id, resource_exists,
        datastore_exists, get_package_parameter, get_resource_parameter,
        set_resource_description
)

BASE_URL = 'https://data.wprdc.org/api/3/action/'

def write_to_csv(filename, list_of_dicts, keys=None):
    if keys is None: # Extract fieldnames if none were passed.
        print(f'Since keys == None, write_to_csv is inferring the fields to write from the list of dicts.')
        keys = set()
        for row in list_of_dicts:
            keys = set(row.keys()) | keys
        keys = sorted(list(keys)) # Sort them alphabetically, in the absence of any better idea.
        # [One other option would be to extract the field names from the schema and send that
        # list as the third argument to write_to_csv.]
        print(f'Extracted keys: {keys}')
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def write_or_append_to_csv(filename, list_of_dicts, keys):
    if not os.path.isfile(filename):
        with open(filename, 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
            dict_writer.writeheader()
            dict_writer.writerows(list_of_dicts)
    with open(filename, 'a') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        #dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def simplify_string(s):
    return  ''.join(filter(str.isalnum, s))

def download_file_to_path(url, local_dir, path = SOURCE_DIR):
    """Stream the file to disk without using excessive memory."""
    # From https://stackoverflow.com/a/39217788
    import shutil

    local_filepath = f"{local_dir}/{url.split('/')[-1]}"
    with requests.get(url, stream=True) as r:
        with open(local_filepath, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    return local_filepath

def save_to_waiting_room(list_of_dicts, resource_id, resource_name):
    # data_dictionary is a list of dicts:
        #  [{'id': '_id', 'type': 'int'},
        #    {'id': 'license_number',
        #     'info': {'label': 'Number please', 'notes': '875goo47B', 'type_override': ''},
        #     'type': 'text'},
    filepath = f"{WAITING_ROOM_DIR}/{resource_id}-{simplify_string(resource_name)}-data-dictionary.json"
    if not os.path.isdir(WAITING_ROOM_DIR): # Create local directory if necessary
        os.makedirs(WAITING_ROOM_DIR)
    with open(filepath, 'w') as f:
        f.write(json.dumps(list_of_dicts) + '\n')
    return filepath

def get_data_dictionary(resource_id):
    from engine.credentials import site, API_key
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results = ckan.action.datastore_search(resource_id=resource_id)
        return results['fields']
    except ckanapi.errors.NotFound: # Either the resource doesn't exist, or it doesn't have a datastore.
        return None

def set_data_dictionary(resource_id, old_fields):
    # Here "old_fields" needs to be in the same format as the data dictionary
    # returned by get_data_dictionary: a list of type dicts and info dicts.
    # Though the '_id" field needs to be removed for this to work.
    from engine.credentials import site, API_key
    if old_fields[0]['id'] == '_id':
        old_fields = old_fields[1:]

    # Note that a subset can be sent, and they will update part of
    # the integrated data dictionary.
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    present_fields = get_data_dictionary(resource_id)
    new_fields = []
    # Attempt to restore data dictionary, taking into account the deletion and addition of fields, and ignoring any changes in type.
    # Iterate through the fields in the data dictionary and try to apply them to the newly created data table.
    for field in present_fields:
        if field['id'] != '_id':
            definition = next((f.get('info', None) for f in old_fields if f['id'] == field['id']), None)
            if definition is not None:
                nf = dict(field)
                nf['info'] = definition
                new_fields.append(nf)

    results = ckan.action.datastore_create(resource_id=resource_id, fields=new_fields, force=True)
    # The response without force=True is
    # ckanapi.errors.ValidationError: {'__type': 'Validation Error', 'read-only': ['Cannot edit read-only resource. Either pass"force=True" or change url-type to "datastore"']}
    # With force=True, it works.

    return results

def scientific_notation_to_integer(s):
    # Source files may contain scientific-notation representations of
    # what should be integers (e.g., '2e+05'). This function can be
    # used to convert such strings to integers.
    return int(decimal.Decimal(s))

def add_datatable_view(resource, job):
    r = requests.post(
        BASE_URL + 'resource_create_default_resource_views',
        json={
            'resource': resource,
            'create_datastore_views': True
        },
        headers={
            'Authorization': API_KEY,
            'Content-Type': 'application/json'
        },
        verify=job.verify_requests
    )
    print(r.json())
    return r.json()['result']

def configure_datatable(view, job):
    # setup new view
    view['col_reorder'] = True
    view['export_buttons'] = True
    view['responsive'] = False
    r = requests.post(BASE_URL + 'resource_view_update', json=view, headers={"Authorization": API_KEY}, verify=job.verify_requests)

def reorder_views(resource, views, job):
    resource_id = resource['id']

    temp_view_list = [view_item['id'] for view_item in views if
                      view_item['view_type'] not in ('datatables_view',)]

    new_view_list = [datatable_view['id']] + temp_view_list
    r = requests.post(BASE_URL + 'resource_view_reorder', json={'id': resource_id, 'order': new_view_list},
                      headers={"Authorization": API_KEY}, verify=job.verify_requests)

def deactivate_datastore(resource):
    from engine.credentials import site, API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    set_resource_parameters_to_values(site, resource['id'], ['datastore_active'], [False], API_key)
    # How does this differ from deleting the datastore?

def query_resource(site,query,API_key=None):
    """Use the datastore_search_sql API endpoint to query a CKAN resource."""
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search_sql(sql=query)
    # A typical response is a dictionary like this
    #{u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'_full_text', u'type': u'tsvector'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'records': [{u'_full_text': u"'0001b00010000000':1 '11':2 '13585.47':3",
    #               u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_full_text': u"'0001c00058000000':3 '2':2 '7827.64':1",
    #               u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_full_text': u"'0001c01661006700':3 '1':1 '3233.59':2",
    #               u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}]
    # u'sql': u'SELECT * FROM "d1e80180-5b2e-4dab-8ec3-be621628649e" LIMIT 3'}
    data = response['records']
    return data

def get_package_id(job, test_mode):
    return job.production_package_id if not test_mode else TEST_PACKAGE_ID

def delete_datatable_views(resource_id):
    from engine.credentials import site, API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    resource = get_resource_by_id(resource_id)
    extant_views = ckan.action.resource_view_list(id=resource_id)
    if len(extant_views) > 0:
        if resource['format'].lower() == 'csv' and resource['url_type'] in ('datapusher', 'upload') and resource['datastore_active']:
            if 'datatables_view' not in [v['view_type'] for v in extant_views]:
                print(f"Unable to find a Data Table view to delete from {resource['name']}.")
            else: # Delete all views in that resource
                for view in extant_views:
                    if view['view_type'] == 'datatables_view':
                        print(f"Deleting the view with name {view['title']} and type {view['view_type']}.")
                        ckan.action.resource_view_delete(id = view['id'])

def create_data_table_view(resource, job):
#    [{'col_reorder': False,
#      'description': '',
#      'export_buttons': False,
#      'filterable': True,
#      'fixed_columns': False,
#      'id': '05a49a72-cb9b-4bfd-aa10-ab3655b541ac',
#      'package_id': '812527ad-befc-4214-a4d3-e621d8230563',
#      'resource_id': '117a09dd-dbe2-44c2-9553-46a05ad3f73e',
#      'responsive': False,
#      'show_fields': ['_id',
#                      'facility',
#                      'run_date',
#                      'gender_race_group',
#                      'patient_count',
#                      'gender',
#                      'race'],
#      'title': 'Data Table',
#      'view_type': 'datatables_view'}]

#    from engine.credentials import site, API_key
#    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
#    extant_views = ckan.action.resource_view_list(id=resource_id)
#    title = 'Data Table'
#    if title not in [v['title'] for v in extant_views]:
#        # CKAN's API can't accept nested JSON to enable the config parameter to be
#        # used to set export_buttons and col_reorder options.
#        # https://github.com/ckan/ckan/issues/2655
#        config_dict = {'export_buttons': True, 'col_reorder': True}
#        #result = ckan.action.resource_view_create(resource_id = resource_id, title="Data Table", view_type='datatables_view', config=json.dumps(config_dict))
#        result = ckan.action.resource_view_create(resource_id=resource_id, title="Data Table", view_type='datatables_view')

        #r = requests.get(BASE_URL + 'package_show', params={'id': package_id})
        #resources = r.json()['result']['resources']

        #good_resources = [resource for resource in resources
        #                  if resource['format'].lower() == 'csv' and resource['url_type'] in ('datapusher', 'upload')]
    from engine.credentials import site, API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    resource_id = resource['id']
    extant_views = ckan.action.resource_view_list(id=resource_id)
    title = 'Data Table'

    if resource['format'].lower() == 'csv' and resource['url_type'] in ('datapusher', 'upload') and resource['datastore_active']:
        if 'datatables_view' not in [v['view_type'] for v in extant_views]:
            print("Adding view for {}".format(resource['name']))
            datatable_view = add_datatable_view(resource, job)[0]
            # A view will be described like this:
            #    {'col_reorder': False,
            #    'description': '',
            #    'export_buttons': False,
            #    'filterable': True,
            #    'fixed_columns': False,
            #    'id': '3181357a-d130-460f-ac86-e54ae800f574',
            #    'package_id': '812527ad-befc-4214-a4d3-e621d8230563',
            #    'resource_id': '9fc62eb0-10b3-4e76-ba01-8883109a0693',
            #    'responsive': False,
            #    'title': 'Data Table',
            #    'view_type': 'datatables_view'}
            if 'id' in datatable_view.keys():
                configure_datatable(datatable_view, job)

            # reorder_views(resource, views, job)

    # [ ] Integrate previous attempt which avoids duplicating views with the same name:
    #if title not in [v['title'] for v in extant_views]:
    #    # CKAN's API can't accept nested JSON to enable the config parameter to be
    #    # used to set export_buttons and col_reorder options.
    #    # https://github.com/ckan/ckan/issues/2655
    #    config_dict = {'export_buttons': True, 'col_reorder': True}
    #    #result = ckan.action.resource_view_create(resource_id = resource_id, title="Data Table", view_type='datatables_view', config=json.dumps(config_dict))
    #    result = ckan.action.resource_view_create(resource_id=resource_id, title="Data Table", view_type='datatables_view')

def add_tag(package, tag='_etl'):
    tag_dicts = package['tags']
    tags = [td['name'] for td in tag_dicts]
    if tag not in tags:
        from engine.credentials import site, API_key
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        new_tag_dict = {'name': tag}
        tag_dicts.append(new_tag_dict)
        set_package_parameters_to_values(site,package['id'],['tags'],[tag_dicts],API_key)

def convert_extras_dict_to_list(extras):
    extras_list = [{'key': ekey, 'value': evalue} for ekey,evalue in extras.items()]
    return extras_list

def set_extra_metadata_field(package,key,value):
    if 'extras' in package:
        extras_list = package['extras']
        # Keep definitions and uses of extras metadata updated here:
        # https://github.com/WPRDC/data-guide/blob/master/docs/metadata_extras.md

        # The format as obtained from the CKAN API is like this:
        #       u'extras': [{u'key': u'dcat_issued', u'value': u'2014-01-07T15:27:45.000Z'}, ...
        # not a dict, but a list of dicts.
        extras = {d['key']: d['value'] for d in extras_list}
    else:
        extras = {}

    extras[key] = value
    extras_list = convert_extras_dict_to_list(extras)
    from engine.credentials import site, API_key
    set_package_parameters_to_values(site,package['id'],['extras'],[extras_list],API_key)

def add_time_field(package, resource, job):
    if job.time_field is None:
        # Note that if the job does not specify a time_field or gives a time_field of None,
        # add_time_field is currently not checking this against what's in the CKAN package
        # metadata. This is for the best since it will be necessary to track down all
        # resources with time_fields and add them to the ETL jobs (which in some cases
        # will necessitate migrating the job to rocket-etl, which may not be trivial).
        return
    if 'extras' in package:
        extras_list = package['extras']
        # Keep definitions and uses of extras metadata updated here:
        # https://github.com/WPRDC/data-guide/blob/master/docs/metadata_extras.md

        # The format as obtained from the CKAN API is like this:
        #       u'extras': [{u'key': u'dcat_issued', u'value': u'2014-01-07T15:27:45.000Z'}, ...
        # not a dict, but a list of dicts.
        extras = {d['key']: d['value'] for d in extras_list}
    else:
        extras = {}

    if 'time_field' in extras:
        time_field_by_resource_id = json.loads(extras['time_field'])
        # The time_field metadata is a dict where resource IDs are the keys, and
        # the values are the names of the fields representing a good time field
        # for the corresponding resource.
        # Example: {"76fda9d0-69be-4dd5-8108-0de7907fc5a4": "CREATED_ON"}
        if resource['id'] in time_field_by_resource_id:
            assert job.time_field == time_field_by_resource_id[resource['id']]
        else:
            time_field_by_resource_id[resource['id']] = job.time_field
    else:
        time_field_by_resource_id = {resource['id']: job.time_field}

    print(f"Setting time_field to {time_field_by_resource_id}.")
    set_extra_metadata_field(package, 'time_field', json.dumps(time_field_by_resource_id))

def update_etl_timestamp(package,resource):
    from engine.credentials import site, API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    set_extra_metadata_field(package,key='last_etl_update',value=datetime.now().isoformat())
    # Keep definitions and uses of extras metadata updated here:
    # https://github.com/WPRDC/data-guide/blob/master/docs/metadata_extras.md

def get_resource_by_id(resource_id):
    """Get all metadata for a given resource."""
    from engine.credentials import site, API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    return ckan.action.resource_show(id=resource_id)

def get_package_by_id(package_id):
    """Get all metadata for a given resource."""
    from engine.credentials import site, API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    return ckan.action.package_show(id=package_id)

def create_data_table_view_if_needed(resource_id, job):
    """Create a DataTable view if the resource has a datastore."""
    import time
    try:
        resource = get_resource_by_id(resource_id)
    except ckanapi.errors.NotFound:
        time.sleep(10)
        try:
            resource = get_resource_by_id(resource_id)
        except ckanapi.errors.NotFound:
            print("Unable to perform resource-level post-processing, as this resource does not exist.")
            resource = None
    if resource is not None:
        package_id = resource['package_id']
        create_data_table_view(resource, job)
        try:
            package = get_package_by_id(package_id)
        except ckanapi.errors.NotFound:
            print("Unable to perform package-level post-processing, as this package does not exist.")
        else:
            return package, resource
    return None, None

def post_process(resource_id, job, **kwparameters):
    package, resource = create_data_table_view_if_needed(resource_id, job)
    if resource is not None and package is not None:
        add_tag(package, '_etl')
        update_etl_timestamp(package, resource)
        add_time_field(package, resource, job)
        set_resource_description(job, **kwparameters)
        if job.make_datastore_queryable:
            fill_bowl(resource_id)

def lookup_parcel(parcel_id):
    """Accept parcel ID for Allegheny County parcel and return geocoordinates."""
    site = "https://data.wprdc.org"
    resource_id = '23267115-177e-4824-89d9-185c7866270d' #2018 data
    #resource_id = "4b68a6dd-b7ea-4385-b88e-e7d77ff0b294" #2016 data
    query = 'SELECT x, y FROM "{}" WHERE "PIN" = \'{}\''.format(resource_id,parcel_id)
    results = query_resource(site,query)
    assert len(results) < 2
    if len(results) == 0:
        return None, None
    elif len(results) == 1:
        return results[0]['y'], results[0]['x']

def local_file_and_dir(jobject, base_dir, file_key='source_file'):
    target_file = getattr(jobject, file_key) if file_key in jobject.__dict__ else jobject.source_file
    if target_file in [None, '']:
        local_directory = base_dir + "{}/".format(jobject.job_directory) # Is there really a situation
        # where we need to generate the local directory even though the referenced file is None?
        # YES. Currently it's when a file is obtained from (for instance) a web site
        # and is being uploaded to the filestore.
        local_file_path = jobject.source_file
    elif target_file[0] == '/': # Actually the file is specifying an absolute path, so override
        # the usual assumption that the file is located in the job_directory.
        local_file_path = target_file
        local_directory = "/".join(target_file.split("/")[:-1])
    else: # The target_file path is relative.
        # The location of the payload script (e.g., rocket-etl/engine/payload/ac_hd/script.py)
        # provides the job directory (ac_hd).
        # This is used to file the source files in a directory structure that
        # mirrors the directory structure of the jobs.
        #local_directory = "/home/sds25/wprdc-etl/source_files/{}/".format(job_directory)
        local_directory = base_dir + "{}/".format(jobject.job_directory) # Note that the
        # job_directory field is assigned by launchpad.py.
        #directory = '/'.join(date_filepath.split('/')[:-1])
        local_file_path = local_directory + (getattr(jobject,file_key) if file_key in jobject.__dict__ else jobject.source_file)

    if not os.path.isdir(local_directory): # Create local directory if necessary
        os.makedirs(local_directory)
    return local_file_path, local_directory

def ftp_target(jobject):
    target_path = jobject.source_file
    if jobject.source_dir != '':
        target_path = re.sub('/$','',jobject.source_dir) + '/' + target_path
    return target_path

def download_city_directory(jobject, local_target_directory, file_prefix=''):
    """For this function to be able to access the City's FTP server,
    it needs to be able to access the appropriate key file."""
    from icecream import ic
    from engine.parameters.local_parameters import CITY_KEYFILEPATH
    cmd = f"sftp -i {CITY_KEYFILEPATH} pitt@ftp.pittsburghpa.gov:/pitt/{jobject.source_dir}/{file_prefix}* {local_target_directory}"
    results = os.popen(cmd).readlines()
    for result in results:
        print(" > {}".format(result))
    return results

def fetch_city_file(jobject):
    """For this function to be able to get a file from the City's FTP server,
    it needs to be able to access the appropriate key file."""
    from engine.parameters.local_parameters import CITY_KEYFILEPATH
    filename = ftp_target(jobject)
    _, local_directory = local_file_and_dir(jobject, SOURCE_DIR)
    cmd = "sftp -i {} pitt@ftp.pittsburghpa.gov:/pitt/{} {}".format(CITY_KEYFILEPATH, filename, local_directory)
    results = os.popen(cmd).readlines()
    for result in results:
        print(" > {}".format(result))
    return results

#############################################

class Job:
    # It may be a good idea to make a BaseJob and then add different features
    # based on source_type.
    def __init__(self, job_dict):
        self.job_directory = job_dict['job_directory']
        self.source_type = job_dict['source_type']
        self.source_full_url = job_dict['source_full_url'] if 'source_full_url' in job_dict else None
        self.source_file = job_dict['source_file'] if 'source_file' in job_dict else job_dict['source_full_url'].split('/')[-1] if 'source_full_url' in job_dict else None
        self.source_dir = job_dict['source_dir'] if 'source_dir' in job_dict else ''
        self.source_site = job_dict['source_site'] if 'source_site' in job_dict else None
        self.verify_requests = not job_dict['ignore_certificate_errors'] if 'ignore_certificate_errors' in job_dict else True
        self.encoding = job_dict['encoding'] if 'encoding' in job_dict else 'utf-8' # wprdc-etl/pipeline/connectors.py also uses UTF-8 as the default encoding.
        self.rows_to_skip = job_dict['rows_to_skip'] if 'rows_to_skip' in job_dict else 0 # Necessary when extracting from poorly formatted Excel files.
        self.sheet_name = job_dict['sheet_name'] if 'sheet_name' in job_dict else None # To identify an Excel sheet by name.
        self.connector_config_string = job_dict['connector_config_string'] if 'connector_config_string' in job_dict else ''
        self.compressed_file_to_extract = job_dict['compressed_file_to_extract'] if 'compressed_file_to_extract' in job_dict else None
        self.custom_processing = job_dict['custom_processing'] if 'custom_processing' in job_dict else (lambda *args, **kwargs: None)
        self.custom_parameters = job_dict['custom_parameters'] if 'custom_parameters' in job_dict else {}
        self.make_datastore_queryable = job_dict['make_datastore_queryable'] if 'make_datastore_queryable' in job_dict else False
        self.custom_post_processing = job_dict['custom_post_processing'] if 'custom_post_processing' in job_dict else (lambda *args, **kwargs: None)
        self.schema = job_dict['schema'] if 'schema' in job_dict and job_dict['schema'] is not None else NullSchema
        self.filters = job_dict['filters'] if 'filters' in job_dict else []
        self.primary_key_fields = job_dict['primary_key_fields'] if 'primary_key_fields' in job_dict else None
        self.time_field = job_dict['time_field'] if 'time_field' in job_dict else None # Specify the field that provides a good temporal key.
        self.upload_method = job_dict['upload_method'] if 'upload_method' in job_dict else None
        self.always_clear_first = job_dict['always_clear_first'] if 'always_clear_first' in job_dict else False
        self.always_wipe_data = job_dict['always_wipe_data'] if 'always_wipe_data' in job_dict else False
        self.ignore_if_source_is_missing = job_dict['ignore_if_source_is_missing'] if 'ignore_if_source_is_missing' in job_dict else False # This
            # parameter allows a job to be set up to run if the source file can be found and to otherwise just end quietly
            # with a simple console message. This option was designed for the dog-licenses ETL job, which could conceivably have
            # data from the previous year in a separate file in the month of January. (Though really, we should check if this
            # file ever appears.)

        self.destination = job_dict['destination'] if 'destination' in job_dict else 'ckan'
        self.destination_file = job_dict.get('destination_file', None)
        self.production_package_id = job_dict['package'] if 'package' in job_dict else None
        self.resource_name = job_dict['resource_name'] if 'resource_name' in job_dict else None # resource_name is expecting to have a string value
        # for use in naming pipelines. For non-CKAN destinations, this field could be eliminated, but then a different field (like job_code)
        # should be used instead.
        self.resource_description = job_dict['resource_description'] if 'resource_description' in job_dict else None
        self.job_code = job_dict.get('job_code', job_dict.get('resource_name', None)) # If there's no job_code, use the resource_name.

        ic(self.job_code, self.resource_name)
        #self.clear_first = job['clear_first'] if 'clear_first' in job else False
        self.target, self.local_directory = local_file_and_dir(self, base_dir = SOURCE_DIR)
        self.local_cache_filepath = self.local_directory + self.source_file

        self.loader_config_string = 'production' # Note that loader_config_string's use
        # can be seen in the load() function of Pipeline from wprdc-etl.

    def select_extractor(self):
        extension = (self.source_file.split('.')[-1]).lower()
        if self.destination == 'ckan_filestore': # If destination == 'ckan_filestore' (meaning there's no schema)
            self.extractor = pl.FileExtractor # we just want to extract the file, not tabular data.
        elif extension == 'csv':
            self.extractor = pl.CSVExtractor
        elif extension == 'json':
            self.extractor = pl.JSONExtractor
        elif extension in ['xls']:
            self.extractor = pl.OldExcelExtractor
        elif extension in ['xlsx']:
            self.extractor = pl.ExcelExtractor
        elif extension in ['zip']:
            self.extractor = pl.CompressedFileExtractor
        else:
            self.extractor = pl.FileExtractor

    def configure_pipeline_with_options(self, **kwargs): # Rename this to reflect how it modifies parameters based on command-line-arguments.
        """This function handles the application of the command-line arguments
        to the configuration of the pipeline (things that cannot be done
        in the Job.__init__ phase without passing the command-line arguments
        in some fashion).
        """
        # launchpad could just ingest the command-line parameters, preventing them from
        # being carted around and (in principle) allowing those parameters to be even
        # used in Job.__init__().

        use_local_input_file = kwargs['use_local_input_file']
        use_local_output_file = kwargs['use_local_output_file']
        test_mode = kwargs['test_mode']

        print("==============\n" + self.job_code)
        if self.production_package_id == TEST_PACKAGE_ID:
            print(" *** Note that this job currently only writes to the test package. ***")


        # BEGIN SET CONNECTOR PROPERTIES ##
        if use_local_input_file:
            self.source_type = 'local'

        if self.source_type is not None:
            if self.source_type == 'http': # It's noteworthy that assigning connectors at this stage is a
                # completely different approach than the way destinations are handled currently
                # (the destination type is passed to run_pipeline which then configures the loaders)
                # but I'm experimenting with this as it seems like it might be a better way of separating
                # such things.
                self.source_connector = pl.RemoteFileConnector # This is the connector to use for files available via HTTP.
                if not use_local_input_file:
                    if self.source_full_url is not None:
                        self.target = self.source_full_url
                    else:
                        raise ValueError(f"No source_full_url specified for job code {self.job_code}.")
            elif self.source_type == 'sftp':
                self.target = ftp_target(self)
                self.source_connector = pl.SFTPConnector
            elif self.source_type == 'ftp':
                self.target = ftp_target(self)
                self.source_connector = pl.FTPConnector
            elif self.source_type == 'gcp':
                self.target = self.source_file
                self.source_connector = pl.GoogleCloudStorageFileConnector
            elif self.source_type == 'local':
                self.source_connector = pl.FileConnector
            else:
                raise ValueError("The source_type {} has no specified connector in default_job_setup().".format(self.source_type))
        else:
            raise ValueError("The source_type is not specified.")
            # [ ] What should we do if no source_type (or no source) is specified?
        # END SET CONNECTOR PROPERTIES ##

        ## SET EXTRACTOR PROPERTIES ##
        self.select_extractor()

        ## BEGIN SET DESTINATION PROPERTIES ##

        # It seems like self.destination_file_path and self.destination_directory should be
        # only defined if the destination is a local one. (So destination == 'file'.)
        # HOWEVER, self.destination_file_path is currently being used to specify the
        # filepath parameter in the load() part of the pipeline, below. This is
        # a workaround to avoid specifying yet another parameter.

        if use_local_output_file:
            self.destination = 'file'

        if self.destination == 'file' and self.destination_file is None:
            # Situations where it would be a good idea to just copy the source_file value over to destination_file

            # Situations where the destination_file value(s) should be determined by something else:
            if self.compressed_file_to_extract is not None:
                # When ~extracting~ files from a .zip file, the filenames can come from compressed_file_to_extract.
                self.destination_file = f'{SOURCE_DIR}{self.job_directory}/{self.compressed_file_to_extract}'
            elif self.source_file is not None:
                self.destination_file = self.source_file
            else:
                raise ValueError("No destination_file specified but self.destination == 'file'.")

        self.destination_file_path, self.destination_directory = local_file_and_dir(self, base_dir = DESTINATION_DIR, file_key = 'destination_file')
        if use_local_input_file or self.source_type == 'local':
            if self.target == self.destination_file_path and self.target not in [None, '']:
                raise ValueError("It seems like a bad idea to have the source file be the same as the destination file! Aborting pipeline execution.")

        self.package_id = get_package_id(self, test_mode) # This is the effective package ID,
        # taking into account whether test mode is active.

        ic(self.__dict__)
        ## END SET DESTINATION PROPERTIES ##

    def handle_schema_migrations_and_data_dictionary_stashing(self, **kwargs):
        # The code below was moved here to benefit from configure_pipeline_with_options
        # setting the effective destination options and package ID first (after taking
        # into account the presence of the to_file parameter).

        if (kwargs['clear_first'] or kwargs['migrate_schema']) and self.destination == 'ckan': # This deliberately excludes 'ckan_filestore'
            # because the shenanigans below (Data Tables and data dictionaries)
            # are datastore-specific.
            resource_id = find_resource_id(self.package_id, self.resource_name)

        if kwargs['migrate_schema'] and self.destination == 'ckan':
            # Delete the Data Table view to avoid new fields not getting added to an existing view.
            delete_datatable_views(resource_id)
            # Is this really necessary though? In etl_util.py, migrate_schema being True is already going to force clear_first to be True
            # which should delete all the views.
            # The scenario of concern is when the schema changes by eliminating a field, and it's not clear whether CKAN
            # supports just dropping a field from the schema and auto-dropping the field from the table while preserving
            # the other values.
            print("Note that setting migrate_schema == True is going to clear the associated datastore.")

        if (kwargs['clear_first'] or kwargs['migrate_schema']) and self.destination == 'ckan': # if the target is a CKAN datastore
            # [ ] Maybe add a check to see if an integrated data dictionary exists.
            self.saved_data_dictionary = get_data_dictionary(resource_id) # If so, obtain it.
            # Save it to a local file as a backup.
            data_dictionary_filepath = save_to_waiting_room(self.saved_data_dictionary, resource_id, self.resource_name)

            # wipe_data should preserve the data dictionary when the schema stays the same, and
            # migrate_schema should be used to change the schema but try to preserve the data dictionary.

                # If migrate_schema == True, 1) backup the data dictionary,
                # 2) delete the Data Table view, 3) clear the datastore, 4) run the job, and 5) try to restore the data dictionary.

            # Or could we overload "wipe_data" to include schema migration?

            # [ ] Also, it really seems that always_clear_first should become always_wipe_data.

    def run_pipeline(self, clear_first, wipe_data, migrate_schema, retry_without_last_line=False, ignore_empty_rows=False):
        # target is a filepath which is actually the source filepath.

        # The retry_without_last_line option is a way of dealing with CSV files
        # that abruptly end mid-line.
        locators_by_destination = {}

        if self.destination == 'ckan_link': # Handle special case of just wanting to make a resource that is just a hyperlink
            # which really doesn't need a full pipeline at this point.
            from engine.credentials import site, API_key
            ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
            resource_id = find_resource_id(self.package_id, self.resource_name)
            if resource_id is None:
                resource_as_dict = ckan.action.resource_create(package_id=self.package_id, url=self.source_full_url, format='HTML', name=self.resource_name)
                resource_id = resource_as_dict['id']
            else:
                resource_as_dict = ckan.action.resource_update(id=resource_id, url=self.source_full_url, format='HTML', name=self.resource_name)

            locators_by_destination[self.destination] = resource_id
            return locators_by_destination


        source_file_format = self.source_file.split('.')[-1].lower()
        if self.destination_file is not None:
            self.destination_file_format = self.destination_file.split('.')[-1].lower()
        else:
            self.destination_file_format = source_file_format

        # 2) While wprdc_etl uses 'CSV' as a
        # format that it sends to CKAN, I'm inclined to switch to 'csv',
        # and uniformly lowercase all file formats.

        # Though given a format of 'geojson', the CKAN API resource_create
        # response lists format as 'GeoJSON', so CKAN is doing some kind
        # of correction.

        # [ ] Maybe test_mode and any other parameters should be applied in a discrete stage between initialization and running.
        # This would allow the source and destination parameters to be prepared, leaving the pipeline running to just run the pipeline.

        # BEGIN Destination-specific configuration
            # A) First configure the loader. It might make more sense to move this to configure_pipeline_with_options().
        if self.destination == 'ckan':
            self.loader = pl.CKANDatastoreLoader
        elif self.destination == 'file':
            # The tabularity of the data (that is, whether the loader is going
            # to be handed a record (list of dicts) or a file or file-like object
            # should determine which kind of file loader will be used.
            if self.destination_file_format is None:
                raise ValueError("Destination == 'file' but self.destination_file_format is None!")
            elif self.destination_file_format.lower() in ['csv', 'json'] and self.compressed_file_to_extract is None:
                self.loader = pl.TabularFileLoader # Isn't this actually very CSV-specific, given the write_or_append_to_csv_file function it uses?
                self.upload_method = 'insert' # Note that this will always append records to an existing file
                # unless 'always_clear_first' (or 'always_wipe_data') is set to True.
            else:
                self.loader = pl.NontabularFileLoader
        elif self.destination == 'ckan_filestore':
            self.loader = pl.CKANFilestoreLoader
        elif self.destination is None:
            return {} # locators_by_destination should be empty and the pipeline should be skipped
            # for these cases where there is no destination (like in snow_plow_geojson.py when
            # no new files to upload are found.
        else:
            raise ValueError(f"run_pipeline does not know how to handle destination = {self.destination}")

            # B) Then do some boolean operations on clear_first, self.always_clear_first, migrate_schema, and wipe_first.
        clear_first = clear_first or self.always_clear_first or migrate_schema # If migrate_schema == True, 1) backup the data dictionary,
        # 2) delete the Data Table view, 3) clear the datastore, 4) run the job, and 5) try to restore the data dictionary.
        # It just seems cleaner to do most of that in launchpad.py (probably because there's so little in the main() function.

        wipe_data = wipe_data or self.always_wipe_data
        if clear_first and wipe_data:
            raise ValueError("clear_first and wipe_data should not both be True simultaneously. To clear a datastore for a job that has always_wipe_data = True, add the command-line argument 'override_wipe_data'.")
        elif clear_first:
            if self.destination in ['ckan']:
                if datastore_exists(self.package_id, self.resource_name):
                    # It should be noted that this will wipe out any integrated data_dictionary (but it's being preserved at the launchpad.py level).
                    print("Clearing the datastore for {}".format(self.resource_name)) # Actually done by the pipeline.
                else:
                    print("Since it makes no sense to try to clear a datastore that does not exist, clear_first is being toggled to False.")
                    clear_first = False
        elif wipe_data:
            if self.destination in ['ckan']:
                if datastore_exists(self.package_id, self.resource_name):
                    print("Wiping records from the datastore for {}".format(self.resource_name))
                else:
                    print("Since it makes no sense to try to wipe the records from a datastore that does not exist, wipe_data is being toggled to False.")
                    wipe_data = False

        print(f'Loading {"tabular data" if self.loader.has_tabular_output else "file"}...')
        # END Destination-specific configuration

        try:
            curr_pipeline = pl.Pipeline(self.job_code + ' pipeline', self.job_code + ' Pipeline', log_status=False, chunk_size=1000, settings_file=SETTINGS_FILE, retry_without_last_line = retry_without_last_line, ignore_empty_rows = ignore_empty_rows, filters = self.filters) \
                .connect(self.source_connector, self.target, config_string=self.connector_config_string, encoding=self.encoding, local_cache_filepath=self.local_cache_filepath, verify_requests=self.verify_requests, fallback_host=self.source_site) \
                .extract(self.extractor, firstline_headers=True, rows_to_skip=self.rows_to_skip, sheet_name=self.sheet_name, compressed_file_to_extract=self.compressed_file_to_extract) \
                .schema(self.schema) \
                .load(self.loader, self.loader_config_string,
                      filepath = self.destination_file_path,
                      file_format = self.destination_file_format,
                      fields = self.schema().serialize_to_ckan_fields(),
                      key_fields = self.primary_key_fields,
                      package_id = self.package_id,
                      resource_name = self.resource_name,
                      clear_first = clear_first,
                      wipe_data = wipe_data,
                      method = self.upload_method,
                      verify_requests = self.verify_requests).run()
        except FileNotFoundError:
            if self.ignore_if_source_is_missing:
                print("The source file for this job wasn't found, but that's not surprising.")
            else:
                raise

        if self.destination in ['ckan', 'ckan_filestore']:
            resource_id = find_resource_id(self.package_id, self.resource_name) # This IS determined in the pipeline, so it would be nice if the pipeline would return it.
            locators_by_destination[self.destination] = resource_id
        elif self.destination in ['file']:
            locators_by_destination[self.destination] = self.destination_file_path
        return locators_by_destination

    def process_job(self, **kwparameters):
        #job = kwparameters['job'] # Here job is the class instance, so maybe it shouldn't be passed this way...
        clear_first = kwparameters['clear_first']
        wipe_data = kwparameters['wipe_data']
        migrate_schema = kwparameters['migrate_schema']
        ignore_empty_rows = kwparameters['ignore_empty_rows']
        retry_without_last_line = kwparameters['retry_without_last_line']
        self.configure_pipeline_with_options(**kwparameters)
        self.handle_schema_migrations_and_data_dictionary_stashing(**kwparameters)

        self.custom_processing(self, **kwparameters)
        self.locators_by_destination = self.run_pipeline(clear_first, wipe_data, migrate_schema, retry_without_last_line=retry_without_last_line, ignore_empty_rows=ignore_empty_rows)
        self.custom_post_processing(self, **kwparameters)
        return self.locators_by_destination # Return a dict allowing look up of final destinations of data (filepaths for local files and resource IDs for data sent to a CKAN instance).

def push_to_datastore(job, file_connector, target, config_string, encoding, loader_config_string, primary_key_fields, test_mode, clear_first, upload_method='upsert'):
    # This is becoming a legacy function because all the new features are going into run_pipeline,
    # but note that this is still used at present by a parking ETL job.
    # (wipe_data support is not being added to push_to_datastore.)
    self.package_id = job['package'] if not test_mode else TEST_PACKAGE_ID
    resource_name = job['resource_name']
    schema = job['schema']
    extractor = select_extractor(job)
    # Upload data to datastore
    if clear_first:
        print("Clearing the datastore for {}".format(job['resource_name']))
    print('Uploading tabular data...')
    curr_pipeline = pl.Pipeline(job['resource_name'] + ' pipeline', job['resource_name'] + ' Pipeline', log_status=False, chunk_size=1000, settings_file=SETTINGS_FILE) \
        .connect(file_connector, target, config_string=config_string, encoding=encoding) \
        .extract(extractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, loader_config_string,
              fields=schema().serialize_to_ckan_fields(),
              key_fields=primary_key_fields,
              package_id=self.package_id,
              resource_name=resource_name,
              clear_first=clear_first,
              method=upload_method).run()

    resource_id = find_resource_id(self.package_id, resource_name) # This IS determined in the pipeline, so it would be nice if the pipeline would return it.
    return resource_id
