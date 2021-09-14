import csv, json, requests, sys, traceback, re, time, ckanapi
from pprint import pprint

from engine.parameters.local_parameters import SOURCE_DIR
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.notify import send_to_slack
from engine.credentials import site, API_key

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def get_resource_fields(site, resource_id, API_key=None):
    # Use the datastore_search API endpoint to get the field names (and schema)
    # from the given CKAN resource.
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search(id=resource_id, limit=0)
    # A typical response is a dictionary like this
    #{u'_links': {u'next': u'/api/action/datastore_search?offset=3',
    #             u'start': u'/api/action/datastore_search'},
    # u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'limit': 3,
    # u'records': [{u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}],
    # u'resource_id': u'd1e80180-5b2e-4dab-8ec3-be621628649e',
    # u'total': 88232}
    return [s['id'] for s in response['fields'] if s['id'] != '_id'], response['fields']

def get_resource_data(site, resource_id, API_key=None, count=50, offset=0, fields=None):
    # Use the datastore_search API endpoint to get <count> records from
    # a CKAN resource starting at the given offset and only returning the
    # specified fields in the given order (defaults to all fields in the
    # default datastore order).
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    if fields is None:
        response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset)
    else:
        response = ckan.action.datastore_search(id=resource_id, limit=count, offset=offset, fields=fields)
    # A typical response is a dictionary like this
    #{u'_links': {u'next': u'/api/action/datastore_search?offset=3',
    #             u'start': u'/api/action/datastore_search'},
    # u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'limit': 3,
    # u'records': [{u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}],
    # u'resource_id': u'd1e80180-5b2e-4dab-8ec3-be621628649e',
    # u'total': 88232}
    data = response['records']
    return data

def find_resource_id(package_id, resource_name):
    # Get the resource ID given the package ID and resource name.
    from engine.credentials import site, API_key
    resources = get_package_parameter(site, package_id, 'resources', API_key)
    for r in resources:
        if 'name' in r and r['name'] == resource_name:
            return r['id']
    return None

def resource_exists(package_id, resource_name):
    return find_resource_id(package_id, resource_name) is not None

def datastore_exists(package_id, resource_name):
    """Check whether a datastore exists for the given package ID and resource name.

    If there should be a datastore but it's inactive, try to restore it. If
    restoration fails, send a notification.
    """
    from engine.credentials import site, API_key
    resource_id = find_resource_id(package_id, resource_name)
    if resource_id is None:
        return False
    datastore_is_active = get_resource_parameter(site, resource_id, 'datastore_active', API_key)
    if datastore_is_active:
        return True
    else:
        url = get_resource_parameter(site, resource_id, 'url', API_key)
        if re.search('datastore/dump', url) is not None:
            # This looks like a resource that has a datastore that is inactive.
            # Try restoring it.
            ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
            response = ckan.action.resource_patch(id=resource_id, datastore_active=True)
            if response['datastore_active']:
                print("Restored inactive datastore.")
            else:
                msg = f"Unable to restore inactive datastore for resource ID {resource_id}, resource name {resource_name} and package_id {package_id}!"
                channel = "@david" #if (test_mode or not PRODUCTION) else "#etl-hell" # test_mode is not available to this function.
                if channel != "@david":
                    msg = f"@david {msg}"
                send_to_slack(msg, username='datastore_exists()', channel=channel, icon=':illuminati:')
            return response['datastore_active']

def get_package_parameter(site, package_id, parameter=None, API_key=None):
    """Gets a CKAN package parameter. If no parameter is specified, all metadata
    for that package is returned."""
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        if parameter is None:
            return metadata
        else:
            return metadata[parameter]
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter, package_id))

def get_resource_parameter(site, resource_id, parameter=None, API_key=None):
    # Some resource parameters you can fetch with this function are
    # 'cache_last_updated', 'package_id', 'webstore_last_updated',
    # 'datastore_active', 'id', 'size', 'state', 'hash',
    # 'description', 'format', 'last_modified', 'url_type',
    # 'mimetype', 'cache_url', 'name', 'created', 'url',
    # 'webstore_url', 'mimetype_inner', 'position',
    # 'revision_id', 'resource_type'
    # Note that 'size' does not seem to be defined for tabular
    # data on WPRDC.org. (It's not the number of rows in the resource.)
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = ckan.action.resource_show(id=resource_id)
    if parameter is None:
        return metadata
    else:
        return metadata[parameter]

def set_package_parameters_to_values(site, package_id, parameters, new_values, API_key):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    original_values = [] # original_values = [get_package_parameter(site, package_id, p, API_key) for p in parameters]
    for p in parameters:
        try:
            original_values.append(get_package_parameter(site, package_id, p, API_key))
        except RuntimeError:
            print("Unable to obtain package parameter {}. Maybe it's not defined yet.".format(p))
    payload = {}
    payload['id'] = package_id
    for parameter, new_value in zip(parameters, new_values):
        payload[parameter] = new_value
    results = ckan.action.package_patch(**payload)
    print("Changed the parameters {} from {} to {} on package {}".format(parameters, original_values, new_values, package_id))

def set_resource_parameters_to_values(site, resource_id, parameters, new_values, API_key):
    """Sets the given resource parameters to the given values for the specified
    resource.

    This fails if the parameter does not currently exist. (In this case, use
    create_resource_parameter().)"""
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    original_values = [get_resource_parameter(site, resource_id, p, API_key) for p in parameters]
    payload = {}
    payload['id'] = resource_id
    for parameter, new_value in zip(parameters, new_values):
        payload[parameter] = new_value
    #For example,
    #   results = ckan.action.resource_patch(id=resource_id, url='#', url_type='')
    results = ckan.action.resource_patch(**payload)
    print(results)
    print("Changed the parameters {} from {} to {} on resource {}".format(parameters, original_values, new_values, resource_id))

def set_resource_description(job, **kwparameters):
    if hasattr(job, 'resource_description') and job.resource_description is not None:
        if not kwparameters['use_local_output_file'] and job.destination in ['ckan', 'ckan_filestore']:
            if kwparameters['test_mode']:
                assert job.package_id == TEST_PACKAGE_ID # This should be taken care of in etl_util.py
            ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
            resource_id = find_resource_id(job.package_id, job.resource_name)
            if resource_id is not None:
                existing_resource_description = get_resource_parameter(site, resource_id, 'description', API_key)
                if existing_resource_description == '':
                    set_resource_parameters_to_values(site, resource_id, ['description'], [job.resource_description], API_key)
                    print("Updating the resource description")
                else:
                    print(f"Not updating the resource description because existing_resource_description = {existing_resource_description}.")

def get_number_of_rows(resource_id):
    # On other/later versions of CKAN it would make sense to use
    # the datastore_info API endpoint here, but that endpoint is
    # broken on WPRDC.org.
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        results_dict = ckan.action.datastore_search(resource_id=resource_id, limit=1) # The limit
        # must be greater than zero for this query to get the 'total' field to appear in
        # the API response.
        count = results_dict['total']
    except:
        print("get_number_of_rows threw an exception. Returning 'None'.")
        return None
    return count

def query_resource(site, query, API_key=None):
    # Use the datastore_search_sql API endpoint to query a CKAN resource.


    # Note that this doesn't work for private datasets.
    # The relevant CKAN GitHub issue has been closed.
    # https://github.com/ckan/ckan/issues/1954
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

    # Note that if a CKAN table field name is a Postgres reserverd word, you
    # get a not-very-useful error
    #      (e.g., 'query': ['(ProgrammingError) syntax error at or near
    #     "on"\nLINE 1: SELECT * FROM (SELECT load, on FROM)
    # and you need to escape the reserved field name with double quotes.

    # These seem to be reserved Postgres words:
    # ALL, ANALYSE, ANALYZE, AND, ANY, ARRAY, AS, ASC, ASYMMETRIC, AUTHORIZATION, BETWEEN, BINARY, BOTH, CASE, CAST, CHECK, COLLATE, COLUMN, CONSTRAINT, CREATE, CROSS, CURRENT_DATE, CURRENT_ROLE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_USER, DEFAULT, DEFERRABLE, DESC, DISTINCT, DO, ELSE, END, EXCEPT, FALSE, FOR, FOREIGN, FREEZE, FROM, FULL, GRANT, GROUP, HAVING, ILIKE, IN, INITIALLY, INNER, INTERSECT, INTO, IS, ISNULL, JOIN, LEADING, LEFT, LIKE, LIMIT, LOCALTIME, LOCALTIMESTAMP, NATURAL, NEW, NOT, NOTNULL, NULL, OFF, OFFSET, OLD, ON, ONLY, OR, ORDER, OUTER, OVERLAPS, PLACING, PRIMARY, REFERENCES, RIGHT, SELECT, SESSION_USER, SIMILAR, SOME, SYMMETRIC, TABLE, THEN, TO, TRAILING, TRUE, UNION, UNIQUE, USER, USING, VERBOSE, WHEN, WHERE

    return data
