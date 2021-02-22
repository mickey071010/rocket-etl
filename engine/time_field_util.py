"""Functions for determining the time range of records in a CKAN datastore or CSV file."""
import csv, json
from datetime import datetime, timedelta
from dateutil import parser
from pprint import pprint

from engine.etl_util import query_resource, get_resource_parameter, get_package_by_id, find_resource_id

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def random_string(stringLength=10):
    """Generate a random string of fixed length."""
    import random, string
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

def find_extremes(resource_id, field):
    from engine.credentials import site, API_key
    from engine.leash_util import initially_leashed, fill_bowl, empty_bowl
    toggle = initially_leashed(resource_id)
    if toggle:
        fill_bowl(resource_id)

    biggest_name = 'biggest_' + random_string(5) # Append a random string to avoid query caching.
    query = 'SELECT min("{}") AS smallest, max("{}") as {} FROM "{}" LIMIT 1'.format(field,field,biggest_name,resource_id)
    record = query_resource(site=site, query=query, API_key=API_key)[0] # Note that query_resource doesn't
    # work for private datasets. Therefore it won't be possible to test the time_field aspects on a private dataset,
    # including partial filling from the last date of the dataset to the present date.

    if toggle:
        empty_bowl(resource_id)
    return record['smallest'], record[biggest_name]

def find_extreme_dates(resource_id, time_field_lookup):
    from engine.credentials import site, API_key
    datastore_active = get_resource_parameter(site, resource_id, 'datastore_active', API_key)
    if datastore_active:
        if resource_id in time_field_lookup:
            time_field = time_field_lookup[resource_id]
            if time_field is not None:
                first, last = find_extremes(resource_id, time_field)
                first = parser.parse(first)
                last = parser.parse(last)
                return first, last

    return None, None

#def synthesize_pseudo_time_field_lookup(job):

def get_extant_time_range(job, **kwparameters):
    if 'ckan' in job.destinations: # This is a strong argument for making each job_dict
        # have exactly one source and one destination and using job molecules or
        # chains to support multiple destinations (somehow).
        ## JOB CHAINS: 1) Support building up more complicated processes (often 
        ## represented by "directed acyclic graphs") by chaining job atoms.
        ## 2) More coherently support multiple destinations at this level by
        ## calling the same job atom twice, with different destinations.
        ## 3) But try to make every parameter into a potential list this way
        ## (at least by allowing one parameter at a time to be changed).

        package = get_package_by_id(job.package)
        if 'extras' in package:
            extras_list = package['extras']
            # Keep definitions and uses of extras metadata updated here:
            # https://github.com/WPRDC/data-guide/blob/master/docs/metadata_extras.md
            # The format is like this:
            #       u'extras': [{u'key': u'dcat_issued', u'value': u'2014-01-07T15:27:45.000Z'}, ...
            # not a dict, but a list of dicts.
            extras = {d['key']: d['value'] for d in extras_list}
            resource_id = find_resource_id(job.package, job.resource_name) # This adds a second call to get the
                # package when it's already been obtained a few lines above.
            if resource_id is None: # The resource does not yet exist.
                return None, None
            if 'time_field' in extras and resource_id in json.loads(extras['time_field']):
                time_field_lookup = json.loads(extras['time_field'])
                first_date, last_date = find_extreme_dates(resource_id, time_field_lookup)
                return first_date, last_date
            else:
                try:
                    time_field = getattr(job, 'time_field', None) # Try to grab time_field from job
                except AttributeError:
                    return None, None
                else:
                    time_field_lookup = {resource_id: time_field}
                    first_date, last_date = find_extreme_dates(resource_id, time_field_lookup)
                    return first_date, last_date
                return None, None
        else:
            return None, None
    else: # Find the time range of a non-datastore CSV file at one of the actual destinations.
            # OR wipe the existing file and rewrite it from scratch.
        if 'file' in job.destinations:
            try:
                f = open(job.destination_file_path, 'r')
            except FileNotFoundError:
                return None, None
            else:
                reader = csv.DictReader(f)
                first_date = datetime.max
                last_date = datetime.min
                for row in reader:
                    timestamp = row.get(job.time_field, None)
                    if timestamp is not None:
                        timestamp = parser.parse(timestamp)
                        if timestamp < first_date:
                            first_date = timestamp
                        if timestamp > last_date:
                            last_date = timestamp
                f.close()
                if first_date <= last_date:
                    return first_date, last_date
                else:
                    return None, None
        else:
            raise ValueError(f"Unable to determine the extant time range for the following destinations: {job.destinations}")
