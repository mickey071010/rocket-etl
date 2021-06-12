import bigjson
import os, csv, re
from engine.etl_util import write_or_append_to_csv
# For a GeoJSON file that starts like this:
#{"type":"FeatureCollection",
# "features":
#       [{"geometry": {"type": "Point", "coordinates": [-79.81167781195305, 40.5421828403599]},
#         "type": "Feature", 
#         "id": 0,
#         "properties": {"STATUS": "ACTIVE", "ADDRESS_ID": 1, "EDIT_DATE": "2013-06-13", "PARENT_ID": 0, "MUNICIPALI": "HARMAR", "STATE": "PA", "COUNTY": "ALLEGHENY", "SOURCE": "GDR", "FULL_ADDRE": "118 ORR AVE", "ADDR_NUM": "118", "POINT_Y": 447055.300166, "STREET_ID": 17808, "FEATURE_KE": 1, "ADDRESS_TY": 0, "POINT_X": 1395563.70007, "ST_TYPE": "AVE", "ST_NAME": "ORR", "ZIP_CODE": "15024"}
#         },
#         {"geometry": {"type": "Point", "coordinates": [-79.80979744004718, 40.542211378710846]}, "type": "Feature", "id": 1, "properties": {"STATUS": "ACTIVE", "ADDRESS_ID": 2, "EDIT_DATE": "2013-06-13", "PARENT_ID": 0, "MUNICIPALI": "HARMAR", "STATE": "PA", "COUNTY": "ALLEGHENY", "SOURCE": "GDR", "FULL_ADDRE": "121 WILSON AVE", "ADDR_NUM": "121", "POINT_Y": 447053.500053, "STREET_ID": 26070, "FEATURE_KE": 2, "ADDRESS_TY": 0, "POINT_X": 1396086.40006, "ST_TYPE": "AVE", "ST_NAME": "WILSON", "ZIP_CODE": "15024"...
#

# We want to convert it to something like this:
# FEATURE_KE,ADDRESS_ID,PARENT_ID,STREET_ID,ADDRESS_TY,STATUS,ADDR_NUM_P,ADDR_NUM,ADDR_NUM_S,ST_PREMODI,ST_PREFIX,ST_PRETYPE,ST_NAME,ST_TYPE,ST_POSTMOD,UNIT_TYPE,UNIT,FLOOR,MUNICIPALI,COUNTY,STATE,ZIP_CODE,COMMENT,EDIT_DATE,EDIT_USER,SOURCE,EXP_FLAG,FULL_ADDRE,ST_SUFFIX,POINT_X,POINT_Y,LAT,LNG
#1,1,0,17808,0,ACTIVE, ,118, , , , ,ORR,AVE, , , , ,HARMAR,ALLEGHENY,PA,15024, ,2013-06-13 00:00:00, ,GDR, ,118 ORR AVE, ,1395563.70007,447055.300166,447055.3000382334,1395563.6999376416
#...

# For this to work on a humongous GeoJSON file, the file will have to be loaded and parsed in chunks. We use the marvelous bigjson library to achieve the lazy loading and parsing.

def detect_keys(list_of_dicts):
    keys = set()
    for row in list_of_dicts:
        keys = set(row.keys()) | keys
    keys = sorted(list(keys)) # Sort them alphabetically, in the absence of any better idea.
    # [One other option would be to extract the field names from the schema and send that
    # list as the third argument to write_to_csv.]
    print(f'Extracted keys: {keys}')
    return keys

def convert_geojson_row_to_dict(row):
    d = dict(row['properties'])
    geometry = row['geometry']
    assert geometry['type'] == "Point"
    d['LNG'], d['LAT'] = geometry['coordinates']
    return d

def convert_big_destination_geojson_file_to_source_csv(job, **kwparameters):
    input_filepath = job.destination_file_path
    filename = input_filepath.split('/')[-1]
    basename, extension = filename.split('.')
    output_filepath = job.local_directory + basename + '.csv'

    with open(input_filepath, 'rb') as f:
        j = bigjson.load(f)
        features = j['features']
        csv_rows = []
        count = 0
        keys = None
        for feature in features:
            csv_row = convert_geojson_row_to_dict(feature.to_python())
            csv_rows.append(csv_row)
            if len(csv_rows) == 1000:
                count += len(csv_rows)
                if keys is None:
                    keys = detect_keys(csv_rows)
                write_or_append_to_csv(output_filepath, csv_rows, keys)
                csv_rows = []
                print(f"{count} rows have been written.")
        # Delete input_filepath.


