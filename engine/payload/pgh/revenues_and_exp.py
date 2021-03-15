import os, csv, json, requests, sys, traceback
import ckanapi
from datetime import datetime
from dateutil import parser
from pprint import pprint

from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from unidecode import unidecode

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import fetch_city_file, find_resource_id
from engine.credentials import site, API_key
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class RevenueAndExpensesSchema(pl.BaseSchema):
    fund_number = fields.String(load_from='fund number')
    fund_description = fields.String(load_from='fund description')
    department_number = fields.String(load_from='department number')
    department_name = fields.String(load_from='departmnet name')
    cost_center_number = fields.String(load_from='cost center number')
    cost_center_description = fields.String(load_from='cost center description')
    object_account_number = fields.String(load_from='object account number')
    object_account_description = fields.String(load_from='object account description')
    general_ledger_date = fields.Date(load_from='g/l_date')
    amount = fields.Float()
    ledger_code = fields.String(load_from="ledger code")
    ledger_descrpition = fields.String(load_from="ledger description")

    class Meta:
        ordered = True

    @pre_load
    def fix_date(self, data):
        data['g/l_date'] = datetime.strptime(data['g/l_date'], "%Y-%m-%d").date().isoformat()

    @pre_load
    def fix_datetimes(self, data):
        for k, v in data.items():
            if 'date' in k:
                if v:
                    try:
                        data[k] = parser.parse(v).isoformat()
                    except:
                        data[k] = None

budget_package_id = "846f028b-bcc3-45e0-b939-255cffca5f5e" # Production version of Budget package

def conditionally_get_city_files(job, **kwparameters):
    if not kwparameters['use_local_input_file']:
        fetch_city_file(job)

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

job_dicts = [
    {
        'job_code': 'budget',
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'City_Wide_Revenue_and_Expenses.csv',
        'encoding': 'latin-1',
        'custom_processing': conditionally_get_city_files,
        'schema': RevenueAndExpensesSchema,
        'always_wipe_data': True,
        #'primary_key_fields': [],
        'upload_method': 'insert',
        'destination': 'file', # This is being done this way to set up
        'destination_file': f'budget.csv', # express_load_then_delete_file.
        'package': budget_package_id,
        'resource_name': "City Wide Revenues and Expenses",
        'custom_post_processing': express_load_then_delete_file,
    },
]
# The weird thing about running this with the express_load_then_delete_file post-processing function is
# that the script threw this error:
#!! Traceback (most recent call last):
#!!   File "../../../launchpad.py", line 263, in <module>
#    main(**kwargs)
#!!   File "../../../launchpad.py", line 123, in main
#    locators_by_destination = job.process_job(**kwparameters)
#!!   File "rocket-etl/engine/etl_util.py", line 749, in process_job
#    self.custom_post_processing(self, **kwparameters)
#!!   File "rocket-etl/engine/payload/pgh/revenues_and_exp.py", line 84, in express_load_then_delete_file
#    upload=open(csv_file_path, 'r'))
#!!   File "python3.7/site-packages/ckanapi/common.py", line 50, in action
#    files=files)
#!!   File "python3.7/site-packages/ckanapi/remoteckan.py", line 87, in call_action
#    return reverse_apicontroller_action(url, status, response)
#!!   File "python3.7/site-packages/ckanapi/common.py", line 131, in reverse_apicontroller_action
#    raise CKANAPIError(repr([url, status, response]))
#!! ckanapi.errors.CKANAPIError: ['https://data.wprdc.org/api/action/resource_patch', 504, '<html>\r\n<head><title>504 Gateway Time-out</title></head>\r\n<body bgcolor="white">\r\n<center><h1>504 Gateway Time-out</h1></center>\r\n<hr><center>nginx/1.10.3 (Ubuntu)</center>\r\n</body>\r\n</html>\r\n']

# BUT the process actually succeeded in uploading the file and even restoring the datastore. This
# was after I used the API to delete the Data Table view.
