"""Functions for getting file URLs from an ArcGIS server through it's data.json file."""
import requests, re
from pprint import pprint

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def get_arcgis_dataset(title, data_json_url):
    r = requests.get(data_json_url)
    catalog = r.json()
    candidates = [dataset for dataset in catalog['dataset'] if dataset['title'] == title]
    if len(candidates) == 1:
        return candidates[0]
    raise ValueError(f"{len(candidates)} datasets found with the title '{title}'.")

def get_arcgis_data_url(data_json_url, dataset_title, file_format):
    dataset = get_arcgis_dataset(dataset_title, data_json_url)
    for distribution in dataset['distribution']:
        if distribution['title'].lower() == file_format.lower():
            url = distribution['accessURL']
            url_without_query_string, query_string = url.split('?')
            # The query string is being stripped because a) makes the file extension no longer the last thing in the filename
            # component and b) in this case being used for testing, the outSR parameter given in the data.json file specifies
            # a non-useful projection, whereas removing that query string results in usable latitude and longitude values.
            _, filename_and_query_string = re.findall(r'(.*)\/(.*)', url, re.I)[0]
            filename, query_string = filename_and_query_string.split('?')
            return url_without_query_string, filename
    raise ValueError(f"Unable to find title file of type {file_format} and the '{dataset_title}' dataset in {data_json_url}.") 
