## Run as Steve on tools.

# LOAD the data
# once the source files have been collected from the county server 
# we need to dump that data into the parcel_geocoder postgres db

# load assessment data
psql -d parcel_geocoder -c "TRUNCATE TABLE assessment_raw"
psql -d parcel_geocoder -c "COPY assessment_raw FROM '/home/sds25/wprdc-etl/assessments.csv' DELIMITER ','  CSV HEADER ENCODING 'latin-1';"

# load sales data
psql -d parcel_geocoder -c "TRUNCATE TABLE sales_raw"
psql -d parcel_geocoder -c "COPY sales_raw FROM '/home/sds25/wprdc-etl/AA301PAALL.csv' DELIMITER ','  CSV HEADER ENCODING 'latin-1';"


# GEOCODE THE DATA
# with the source data in the db, we can quickly join it with the parcel_centroid table on parcel id fields
# we copy the geocoded queries to CSVs in the wprdc directory in the rocket-etl source_files directory

# geocode assessment data and dump to file
psql -d parcel_geocoder -c "\COPY (SELECT ass.*, pc.longitude, pc.latitude from assessment_raw ass JOIN parcel_centroids pc ON ass.parid = pc.pin) TO '/home/daw165/rocket-etl/source_files/wprdc/geocoded_assessments.csv' DELIMITER ',' CSV HEADER;"

# geocode assessment data and dump to file
psql -d parcel_geocoder -c "\COPY (SELECT sales.*, pc.longitude, pc.latitude from sales_raw sales JOIN parcel_centroids pc ON sales.parid = pc.pin) TO '/home/daw165/rocket-etl/source_files/wprdc/geocoded_sales.csv' DELIMITER ',' CSV HEADER;"


# LOAD THE DATA
# now that we have geocoded CSVs, we can load those files quickly (relatively) into the WPRDC

# enter virtualenv
cd /home/daw165/rocket-etl/
. env/bin/activate

python launchpad.py engine/payload/wprdc/geo_assessments.py

python launchpad.py engine/payload/wprdc/geo_sales.py

deactivate

