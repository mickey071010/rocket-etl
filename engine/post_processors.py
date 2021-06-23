import os, ckanapi
from datetime import datetime, timedelta
from dateutil import parser
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.etl_util import post_process
from engine.credentials import site, API_key
from engine.ckan_util import get_number_of_rows, get_resource_parameter

def delete_source_file(job, **kwparameters):
    assert job.source_type == 'local'
    source_filepath = job.target # Maybe self.local_cache_filepath could be used instead.
    if os.path.exists(source_filepath):
        print(f"Attempting to delete {source_filepath}...")
        os.remove(source_filepath)
    else:
        print(f"{source_filepath} does not exist.")

def express_load_then_delete_file(job, **kwparameters):
    """The basic idea is that the job processes with a 'file' destination,
    so the ETL job loads the file into destination_file_path. Then as a
    custom post-processing step, that file is Express-Loaded. This is
    faster (particularly for large files) and avoids 504 errors and unneeded
    API requests."""
    # Eventually this function should be moved either to etl_util.py or
    # more likely the pipeline framework. In either case, this approach
    # can be formalized, either as a destination or upload method and
    # possibly implemented as a loader (CKANExpressLoader).
    if kwparameters['use_local_output_file']:
        return
    if kwparameters['test_mode']:
        job.package_id = TEST_PACKAGE_ID
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    csv_file_path = job.destination_file_path
    resource_id = find_resource_id(job.package_id, job.resource_name)
    if resource_id is None:
        # If the resource does not already exist, create it.
        print(f"Unable to find a resource with name '{job.resource_name}' in package with ID {job.package_id}.")
        print(f"Creating new resource, and uploading CSV file {csv_file_path} to resource with name '{job.resource_name}' in package with ID {job.package_id}.")
        resource_as_dict = ckan.action.resource_create(package_id=job.package_id,
            name = job.resource_name,
            upload=open(csv_file_path, 'r'))
    else:
        print(f"Uploading CSV file {csv_file_path} to resource with name '{job.resource_name}' in package with ID {job.package_id}.")
        resource_as_dict = ckan.action.resource_patch(id = resource_id,
            upload=open(csv_file_path, 'r'))
        # Running resource_update once sets the file to the correct file and triggers some datastore action and
        # the Express Loader, but for some reason, it seems to be processing the old file.

        # So instead, let's run resource_patch (which just sets the file) and then run resource_update.
        #resource_as_dict = ckan.action.resource_update(id = resource_id)
        resource_as_dict = ckan.action.resource_update(id = resource_id,
            upload=open(csv_file_path, 'r'))

    print(f"Removing temp file at {csv_file_path}")
    os.remove(csv_file_path)

    # Since launchpad.py doesn't update the last_etl_update metadata value in this case
    # because this is a workaround, do it manually here:
    post_process(resource_id, job, **kwparameters)
    # [ ] But really, an ExpressLoader is probably called for, or at least a standardized express_load_then_delete_file function.

def check_for_empty_table(job, **kwparameters):
    if kwparameters['use_local_output_file'] or job.destination == 'file':
        file_path = job.destination_file_path
        if not os.path.isfile(file_path):
            print(f'No file found at the destination path: {file_path}!')
        else:
            with open(file_path, 'r') as f:
                length = len(f.readlines())
                if length == 0:
                    print(f'The destination file ({file_path}) is empty!')
                    assert False
                elif length == 1 and job.destination_file_format == 'csv':
                    print(f'The destination file ({file_path}) contains zero records!')
                    assert False
    elif job.destination in ['ckan']:
        rows = get_number_of_rows(job.locators_by_destination[job.destination])
        if rows is None:
            print(f"The row count of {job.job_code} couldn't be determined.")
            really_empty = True
        else:
            really_empty = (rows == 0)

        if really_empty:
            msg = f"{job.job_code} resulted in a CKAN table with {row} rows. It looks like the upload or ETL script broke."
            print(msg)
            raise ValueError(msg)
    else:
        raise ValueError(f'check_for_empty_table is not yet checking job.destination == {job.destination}.')

def verify_update_backup_source_file_and_then_delete_the_gcp_blob(job, **kwparameters):
    from engine.parameters.google_api_credentials import PATH_TO_SERVICE_ACCOUNT_JSON_FILE, GCP_BUCKET_NAME
    from google.cloud import storage
    from engine.parameters.local_parameters import PRODUCTION

    if not kwparameters['use_local_output_file'] and job.destination in ['ckan', 'ckan_filestore']:
        # VERIFY THAT THE UPDATE HAPPENED (This only works when data is pushed to CKAN.)
        resource_last_modified = get_resource_parameter(site, job.locators_by_destination[job.destination], 'last_modified', API_key)
        if (datetime.now() - parser.parse(resource_last_modified)).seconds < 300: # The update happened in the last 5 minutes.
            # BACKUP SOURCE FILE
            storage_client = storage.Client.from_service_account_json(PATH_TO_SERVICE_ACCOUNT_JSON_FILE)
            blobs = list(storage_client.list_blobs(GCP_BUCKET_NAME))
            possible_blobs = [b for b in blobs if b.name == job.target]
            if len(possible_blobs) != 1:
                raise RuntimeError(f'{len(possible_blobs)} blobs found with the file name {target}.')

            blob = possible_blobs[0]
            backup_path = job.local_directory + job.source_file
            blob.download_to_filename(backup_path) # This can be used to download a local copy of the file.

            if os.path.exists(backup_path) and PRODUCTION and not job.test_mode: # We're only deleting
                # the blob on a machine where PRODUCTION == True and test_mode == False to ensure that
                # the run was from a production server and pushed to the production dataset.
                # DELETE THE GCP BLOB
                print(f"Deleting the {job.target} blob.")
                blob.delete()
