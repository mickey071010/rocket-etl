import os, ckanapi
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.etl_util import post_process
from engine.credentials import site, API_key
from engine.ckan_util import get_number_of_rows

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
