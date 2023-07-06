# CONUS404 Daily Assets

CONUS404 daily meteorology Earth Engine assets.

## Assets

Collection ID: projects/openet/meteorology/conus404/daily

Image name format: YYYYMMDD

Timestep: daily

### Bands

| Band | Description | Units |
| --- | ----------- | ----- |
| T2_MAX |Daily maximum air temperature at 2 meters | K |
| T2_MIN | Daily minimum air temperature at 2 meters | K |
| TD2 | Daily mean dewpoint temperature at 2 meters | K |
| WIND10 | Daily mean windspeed at 10 meters | m s-1 |
| PSFC | Daily mean surface pressure | Pa |
| ACSWDNB | Daily total downwelling shortwave radiation flux | J m-2 |
| PREC_ACC_NC | Daily precipitation | mm |
| ETO_ASCE | Daily grass reference ET | mm |
| ETR_ASCE | Daily alfalfa reference ET | mm |

### Daily Aggregation

The daily aggregation starts (and ends) at 6 UTC to better represent the day for the CONUS.  The start date is lagged by one day because of the 6 UTC start time. 

Reference ET was computed from the daily aggregated values (i.e. NOT as the sum of the hourly values) using the Refet module (https://github.com/WSWUP/refet).

## Availability

CONUS404 is available for 1979-10-01 to 2020-09-30.

## Update Schedule

CONUS404 assets are not currently being operationally updated.

## Data Source

The CONUS404 USGS data release page (https://www.sciencebase.gov/catalog/item/6372cd09d34ed907bf6c6ab1) lists the following three sources for accessing the data:

### Zarr

The zarr dataset hosted by the USGS Hytest group is an efficient way to access the variables used for this image collection.  The "conus404-hourly-osn" source was used since it is not in a requester pays bucket.  Note, not all variables listed in the "wrfout_datadictionary.csv" file in the data release are currently available in this zarr dataset.  

https://github.com/hytest-org/hytest/blob/main/dataset_catalog/subcatalogs/conus404-catalog.yml

The "conus404_daily_zarr.py" script can be used to build assets from the zarr dataset.

### UCAR

The data is also available via a UCAR THREDDS server.  The current limitation with this approach is that the data is still being ingested from Globus and is only available for 1980-1993 (as of 2023-07-01).

https://rda.ucar.edu/datasets/ds559.0/dataaccess/
https://thredds.rda.ucar.edu/thredds/catalog/files/g/ds559.0/catalog.html

The "conus404_daily_ucar.py" script can be used to build assets from the UCAR THREDDS data.

### Globus

The raw model output files can also be downloaded via Globus, but the format and structure of the data makes it very difficult to use directly.  Each file has all of the 2d and 3d data for a single hour, with file sizes of ~2.5 GB.  There is no way to subset the data spatially or by variable and the downloads are very slow.  It is not recommended to access the data via Globus. 
