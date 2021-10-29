"""Functions for getting file URLs from an ArcGIS server through its data.json file."""
import requests, re, time
from pprint import pprint

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def get_arcgis_dataset(title, data_json_url, catalog=None):
    if catalog is None:
        try:
            r = requests.get(data_json_url)
        except requests.exceptions.ConnectionError: # Retry on ConnectionError
            time.sleep(10)
            r = requests.get(data_json_url)

        catalog = r.json()
    candidates = [dataset for dataset in catalog['dataset'] if dataset['title'] == title]
    if len(candidates) == 1:
        time.sleep(1)
        return candidates[0], catalog
    raise ValueError(f"{len(candidates)} datasets found with the title '{title}'.")

def get_arcgis_data_url(data_json_url, dataset_title, file_format, dataset=None, link=False):
    """Get the URL for a file of a given format from a dataset, based on its title,
    found in data.json file at the given URL.

    If a link to a web page is desired instead of a link to a file, set the link flag to True.

    Example:
        To get the CSV version of the data at
            https://hudgis-hud.opendata.arcgis.com/datasets/public-housing-buildings

        Call this function like this:
        > get_arcgis_data_url('https://hudgis-hud.opendata.arcgis.com/data.json',
            'Public Housing Buildings', 'CSV')

        It returns a two-element tuple:
        ('https://hudgis-hud.opendata.arcgis.com/datasets/52a6a3a2ef1e4489837f97dcedaf8e27_0.csv',
            '52a6a3a2ef1e4489837f97dcedaf8e27_0.csv')
    """
    if dataset is None:
        dataset, data_json_content = get_arcgis_dataset(dataset_title, data_json_url)
    for distribution in dataset['distribution']:
        if distribution['title'].lower() == file_format.lower():
            url = distribution['accessURL']
            if link:
                return url, None
            url_parts = url.split('?')
            if len(url_parts) == 2:
                url_without_query_string, query_string = url.split('?')
                # The query string is being stripped because a) it makes the file extension no
                # longer the last thing in the filename component and b) in this case being
                # used for testing, the outSR parameter given in the data.json file specifies
                # a non-useful projection, whereas removing that query string results in
                # usable latitude and longitude values.
                # example url:
                # https://pghgishub-pittsburghpa.opendata.arcgis.com/datasets/e67592c2904b497b83ccf876fced7979_0.zip?outSR=%7B%22latestWkid%22%3A2272%2C%22wkid%22%3A102729%7D
                _, filename_and_query_string = re.findall(r'(.*)\/(.*)', url, re.I)[0]
                filename, query_string = filename_and_query_string.split('?')
            elif len(url_parts) == 1:
                url_without_query_string = url
                filename = url_without_query_string.split('/')[-1]
            else:
                raise ValueError(f'Too many question marks in the url: {url}')
            return url_without_query_string, filename
    raise ValueError(f"Unable to find title file of type {file_format} and the '{dataset_title}' dataset in {data_json_url}.")

def standard_arcgis_job_dicts(data_json_url, data_json_content, arcgis_dataset_title, base_job_code, package_id, schema, new_wave_format=True):
    ag_dataset, data_json_content = get_arcgis_dataset(arcgis_dataset_title, data_json_url, data_json_content)
    if new_wave_format:
        job_dicts = [
            {
                'job_code': f'{base_job_code}_csv',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'CSV', ag_dataset)[0],
                'encoding': 'utf-8',
                'schema': schema,
                'always_wipe_data': True,
                #'primary_key_fields': ['\ufeffobjectid', 'id_no', 'oid', 'id']
                'destination': 'ckan' if schema is not None else 'ckan_filestore',
                'package': package_id,
                'resource_name': f'{arcgis_dataset_title} (CSV)',
                'upload_method': 'insert',
            },
            {
                'job_code': f'{base_job_code}_geojson',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'GeoJSON', ag_dataset)[0],
                'encoding': 'utf-8',
                'destination': 'ckan_filestore',
                'package': package_id,
                'resource_name': f'{arcgis_dataset_title} (GeoJSON)',
            },
            {
                'job_code': f'{base_job_code}_kml',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'KML', ag_dataset)[0],
                'encoding': 'utf-8',
                'destination': 'ckan_filestore',
                'package': package_id,
                'resource_name': f'{arcgis_dataset_title} (KML)',
            },
            {
                'job_code': f'{base_job_code}_shapefile',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'Shapefile', ag_dataset)[0],
                'encoding': 'binary',
                'destination': 'ckan_filestore',
                'package': package_id,
                'resource_name': f'{arcgis_dataset_title} (Shapefile)',
            },
            {
                'job_code': f'{base_job_code}_api',
                'source_type': 'http',
                #'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'Esri Rest API', ag_dataset, True)[0],
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'ArcGIS GeoService', ag_dataset, True)[0], # It looks like this got changed. Sure, why not?
                'encoding': 'utf-8',
                'destination': 'ckan_link',
                'package': package_id,
                'resource_name': f'{arcgis_dataset_title} (ESRI REST API)',
            },
            {
                'job_code': f'{base_job_code}_web',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'ArcGIS Hub Dataset', ag_dataset, True)[0],
                'encoding': 'utf-8',
                'destination': 'ckan_link',
                'package': package_id,
                'resource_name': f'{arcgis_dataset_title} (data source landing page)',
            },
        ]
    else: # Use traditional resource naming set by the harvester.
        job_dicts = [
            {
                'job_code': f'{base_job_code}_web',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'ArcGIS Hub Dataset', ag_dataset, True)[0],
                'encoding': 'utf-8',
                'destination': 'ckan_link',
                'package': package_id,
                'resource_name': f'ArcGIS Hub Dataset',
            },
            {
                'job_code': f'{base_job_code}_api',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'ArcGIS GeoService', ag_dataset, True)[0],
                'encoding': 'utf-8',
                'destination': 'ckan_link',
                'package': package_id,
                'resource_name': f'Esri Rest API',
            },
            {
                'job_code': f'{base_job_code}_geojson',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'GeoJSON', ag_dataset)[0],
                'encoding': 'utf-8',
                'destination': 'ckan_filestore',
                'package': package_id,
                'resource_name': f'GeoJSON',
            },
            {
                'job_code': f'{base_job_code}_csv',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'CSV', ag_dataset)[0],
                'encoding': 'utf-8',
                'schema': schema,
                'always_wipe_data': True,
                #'primary_key_fields': ['\ufeffobjectid', 'id_no', 'oid', 'id']
                'destination': 'ckan',
                'package': package_id,
                'resource_name': f'CSV',
                'upload_method': 'insert',
            },
            {
                'job_code': f'{base_job_code}_kml',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'KML', ag_dataset)[0],
                'encoding': 'utf-8',
                'destination': 'ckan_filestore',
                'package': package_id,
                'resource_name': f'KML',
            },
            {
                'job_code': f'{base_job_code}_shapefile',
                'source_type': 'http',
                'source_full_url': get_arcgis_data_url(data_json_url, arcgis_dataset_title, 'Shapefile', ag_dataset)[0],
                'encoding': 'binary',
                'destination': 'ckan_filestore',
                'package': package_id,
                'resource_name': f'Shapefile',
            },
        ]
    return job_dicts
