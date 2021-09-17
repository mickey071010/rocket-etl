import ckanapi, time
from datetime import datetime
from pprint import pprint

from engine.credentials import site, API_key

def get_metadata(ckan, resource_id):
    return ckan.action.resource_show(id = resource_id)

def get_resource_parameter(site, resource_id, parameter=None, API_key=None):
    """Gets a CKAN resource parameter. If no parameter is specified, all metadata
    for that resource is returned."""
    # Some resource parameters you can fetch with this function are
    # 'cache_last_updated', 'package_id', 'webstore_last_updated',
    # 'datastore_active', 'id', 'size', 'state', 'hash',
    # 'description', 'format', 'last_modified', 'url_type',
    # 'mimetype', 'cache_url', 'name', 'created', 'url',
    # 'webstore_url', 'mimetype_inner', 'position',
    # 'revision_id', 'resource_type'
    # Note that 'size' does not seem to be defined for tabular
    # data on WPRDC.org. (It's not the number of rows in the resource.)
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = get_metadata(ckan, resource_id)
        if parameter is None:
            return metadata
        else:
            return metadata[parameter]
    except:
        raise RuntimeError("Unable to obtain resource parameter '{}' for resource with ID {}".format(parameter, resource_id))

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

def initially_leashed(resource_id):
    # How can you tell if a datastore is public or not?
    # Probably just try to read it, and depending on the
    # exception that is thrown, you may be able to infer that
    # it is private.

    # The other option is to check the public/private state of the
    # package that contains the resource that the datastore belongs to, like this:
    p_id = get_resource_parameter(site, resource_id, 'package_id', API_key)
    leashed = get_package_parameter(site, p_id, 'private', API_key)    
    return leashed

def make_datastore_public(site, resource_id, API_key):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    try:
        response = ckan.action.datastore_make_public(resource_id=resource_id) # This seems to be replaced by ckanext.datastore.logic.action.set_datastore_active_flag in CKAN 2.8 (maybe).
    except ckanapi.errors.CKANAPIError:
        print("Encountered a CKAN API error. Retrying in 10 ms.")
        time.sleep(0.01)
        response = ckan.action.datastore_make_public(resource_id=resource_id)
    return response 

def make_datastore_private(site, resource_id, API_key):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_make_private(resource_id=resource_id)
    return response

def fill_bowl(resource_id):
    from engine.credentials import site, API_key
    make_datastore_public(site, resource_id, API_key)
    print("Tried to make datastore public")

def empty_bowl(resource_id):
    from engine.credentials import site, API_key
    make_datastore_private(site, resource_id, API_key)
