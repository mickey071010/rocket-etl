# rocket-etl
An ETL framework customized for use with a) CKAN and b) the specific needs and uses of the [Western Pennsylvania Regional Data Center](https://www.wprdc.org) open-data portal. This uses the [wprdc-etl framework](https://github.com/WPRDC/wprdc-etl/) for core ETL functionality.

# File structure

`launchpad.py` is located in the root directory for this repository. In the `engine` subdirectory can be found the `wprdc_etl` directory, `notify.py` for sending Slack notifications, and `etl_util.py` which provides a lot of extra functionality for configuring and running pipelines, setting and using metadata, querying CKAN about existing resources, and automatically setting up CKAN resource views.

In the `engine/payload` subdirectory can be found an arbitrary number of directories, named based on the data source/publisher. This provides a namespacing for organizational purposes to help avoid collisions. (This namespacing is also replicated in the `source_files` directory, which is a subdirectory of the directory where `launchpad.py` resides.) Each payload subdirectory can contain an arbitrary number of scripts, and each script can describe multiple ETL jobs (or steps in jobs). 

# Use
To run all the jobs in a given module (say `pgh/dpw_smart_trash.py`), run this:
```bash
> python launchpad.py engine/payload/pgh/smart_trash.py
```

Since launchpad knows where the payloads can be, you can also use these variants:
```bash
> python launchpad.py payload/pgh/smart_trash.py
```
or
```bash
> python launchpad.py pgh/smart_trash.py
```

A number of options can be set using command-line parameters:

* Force the use of local files (rather than fetching them via FTP) housed in the appropriate `source_files` subdirectory:
```bash
> python launchpad.py pgh/smart_trash.py from_file
```

* Force output to be saved to local files (rather than uploading/upserting the data to CKAN) housed in the appropriate `output_files` subdirectory:
```bash
> python launchpad.py pgh/smart_trash.py to_file
```


* Run the script in test mode, which means that rather than writing the results to the production version of the CKAN package described in the `jobs` list in the script, the data will be written to a default private CKAN package:
```bash
> python launchpad.py pgh/smart_trash.py test
```

* The `PRODUCTION` boolean in `engine/parameters/local_parameters.py` decides whether output defaults to the production package or the test package. Thus running with the `test` command-line argument is only necessary when the `PRODUCTION` variable is True. When `PRODUCTION` is False, sending data to the production package requires using the `production` command-line parameter, like this:
```bash
> python launchpad.py pgh/smart_trash.py production
```

* Run the script without sending any Slack alerts:
```bash
> python launchpad.py pgh/smart_trash.py mute
```

* Log output to a default log location (also namespaced by payload directory names), for use in production:
```bash
> python launchpad.py pgh/smart_trash.py log
```

* Clear the CKAN resource before upserting data (necessary for instance when he fields or primary keys have changed):
```bash
> python launchpad.py pgh/smart_trash.py clear_first
```

* Reverse the notification-sending behavior to only send a notification if the source file is found:
```bash
> python launchpad.py pgh/smart_trash.py wake_me_when_found
```

Any other command-line parameters will be interpreted as candidate job codes (which are the filename of the source file as configured in the job WITHOUT the extension).  So if `pgh_smart_trash.py` contained three jobs and two of them had source files named `oscar.csv` and `grouch.xlsx`, running

```bash
> python launchpad.py pgh/smart_trash.py oscar grouch
```

would run only the `oscar` and `grouch` jobs.

The command-line parameters can be specified in any order, so a typical test-run of an ETL job could be 
```bash
> python launchpad.py pgh/smart_trash.py from_file mute test
```
or 
```bash
> python launchpad.py pgh/smart_trash.py test mute from_file
```


Finally, to search for all jobs in the payload directory and test them all (directing upserts to a default test package on the CKAN server), run this:
```bash
> python launchpad.py test_all
```
This is useful for making sure that nothing breaks after you modify the ETL framework.

# Writing ETL jobs

## Job description
Each atomic job unit is initially represented as a "job_dict", a dictionary specifying parameters of the job unit. This job_dict describes the job that fetches a CSV of dog-license records, processes them through the given schema and inserts them into a table (named "[CURRENT YEAR] Dog Licenses") on the default CKAN site, in the package with given the package ID.
```
    {
        'job_code': 'dog_licenses_this_year',
        'source_type': 'sftp',
        'source_dir': 'Dog_Licenses',
        'source_file': f'DL_gvData_{current_year}.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': DogLicensesSchema,
        'always_wipe_data': True,
        'upload_method': 'insert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'{current_year}_dog_licenses.csv', # purposes.
        'package': dog_licenses_package_id,
        'resource_name': f'{current_year} Dog Licenses'
    }
```
If `always_wipe_data` is True, all the records in the table will be deleted (though the table and the integrated data dictionary will remain) and replaced by whatever records are in the file.

* The `encoding` field should have a value of `binary` when fetching from a remote web site something like an Excel file.
* `primary_key_fields` can be used to specify a list of field names which together provide a unique key for upserting records to the destination.
* The value of `destinations` is `['ckan']` by default, which sends the data to the specified CKAN datastore. Other supported values are `ckan_filestore` and `file` (which saves the records to a local file). A `destinations` field is paired with a `destinations_file` field to provide the name that the file should be saved to.
* The `custom_post_processing` field gives the name of a function that should be invoked after the job is run to, for instance, delete the source file or run validation on the data at the destination.
* The `custom_processing` field gives the name of a function that does pre-processing (for example, fetching a file from an API and then saving it to the correct source_files directory, from which the main join fetches it using `source_type = local`).
* The `filters` value is a list of lists, where each list has three elements: 1) field name, 2) operator, and 3) value. The current implementation of filters is that a `filters` value of `[['breed', '==', 'Chihuahua']]` will filter the data down to only those records where the `breed` value is `Chihuahua`. Multiple filters are ANDed together, comprising an increasingly narrow filter. (This implementation was chosen since it's the kind of filtering that we tend to require.) Many other operators are supported, including `'!='`.
* `time_field` is used to set the time field for a given resource (in the package metadata) and can also be used to make the ETL job time-aware (capable of pulling only the needed records to fill in the gap between the last published record and the present).
