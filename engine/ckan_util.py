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
