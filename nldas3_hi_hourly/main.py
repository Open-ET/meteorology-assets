import argparse
from datetime import datetime, timedelta, timezone
import importlib_metadata
# import importlib.metadata
import json
# import logging
import os
import pprint
import re
import shutil
import sys
import time
import urllib3

import ee
from flask import abort, Response
from google.cloud import storage
#from google.cloud import tasks_v2
import netCDF4
import numpy as np
#import openet.core.utils as utils
import rasterio
import rasterio.warp
import refet
import requests
import s3fs
import xarray as xr

if 'FUNCTION_REGION' in os.environ:
    from google.cloud import logging as cloudlogging
    logging_client = cloudlogging.Client()
    logging_client.setup_logging(log_level=20)  # Info
import logging

# storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024
# storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024

# requests.packages.urllib3.disable_warnings()
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
#logging.getLogger('netCDF4').setLevel(logging.INFO)
logging.getLogger('rasterio').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('s3fs').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

ASSET_COLL_ID = 'projects/openet/assets/meteorology/nldas3/hawaii/hourly'
ASSET_DT_FMT = '%Y%m%d%H'
BUCKET_NAME = 'openet_assets'
BUCKET_FOLDER = 'meteorology/nldas3/hawaii/hourly'
#FUNCTION_URL = 'https://us-central1-openet.cloudfunctions.net'
#FUNCTION_NAME = 'nldas3-meteorology-hourly-worker'
NC_URL = 's3://nasa-waterinsight/NLDAS3/forcing/hourly'
PROJECT_NAME = 'openet'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)
TASK_LOCATION = 'us-central1'
TASK_QUEUE = 'ee-asset-worker'
VARIABLES = [
    'temperature',
    'specific_humidity',
    'pressure',
    'wind_u',
    'wind_v',
    'shortwave_radiation',
    'longwave_radiation',
    'total_precipitation',
    'eto_asce',
    'etr_asce',
    # 'wind_10m',
]

START_DAY_OFFSET = 30
END_DAY_OFFSET = 0

# TODO: This environment variable only exists for python37 runtime
if 'FUNCTION_REGION' in os.environ:
    logging.debug(f'\nInitializing GEE using application default credentials')
    import google.auth
    credentials, project_id = google.auth.default(
        default_scopes=['https://www.googleapis.com/auth/earthengine']
    )
    ee.Initialize(credentials, project=project_id)
else:
    # ee.Initialize(ee.ServiceAccountCredentials(
    #     '', key_file='/Users/mortonc/Projects/keys/openet-assets.json'
    # ))
    ee.Initialize()


def hourly_asset_ingest(
        tgt_dt,
        variables,
        hours=list(range(0, 24)),
        workspace='/tmp',
        overwrite_flag=False,
):
    """Build and ingest NLDAS-3 Hawaii hourly meteorology assets into Earth Engine

    Parameters
    ----------
    tgt_dt : datetime
    variables : list
        Variables to process.
    hours : list
        Specific hours to process
    workspace : str, optional
    overwrite_flag : bool, optional

    Returns
    -------
    str : response string

    """
    tgt_date = tgt_dt.strftime('%Y-%m-%d')

    logging.info(f'\nOpenET NLDAS-3 Hawaii hourly assets - {tgt_date}')
    # response = f'Ingest NLDAS-3 Hawaii hourly assets - {tgt_date}\n'

    date_ws = os.path.join(
        workspace, tgt_dt.strftime('%Y'), tgt_dt.strftime('%m'), tgt_dt.strftime('%d')
    )
    ancillary_ws = os.path.join(workspace, 'ancillary')

    nc_file_name = f'NLDAS_FOR0010_H.A{tgt_dt.strftime("%Y%m%d")}.030.beta.nc'
    nc_file_path = os.path.join(date_ws, nc_file_name)
    nc_file_url = f'{NC_URL}/{tgt_dt.strftime("%Y%m")}/{nc_file_name}'

    crs = 'EPSG:4326'
    # Start with the full NLDAS-3 extent and transform
    extent = [-168.995, 6.995, -51.995, 71.995]
    transform = [0.01, 0, -168.995, 0, -0.01, 71.995]
    width, height = 11700, 6500
    # # extent = [-169.0, 7.0, -52.0, 72.0]
    # # transform = [0.01, 0, -169.0, 0, -0.01, 72.0]

    # Hawaii parameters
    # width = 580
    # height = 370
    # x_offset = 860
    # y1_offset = 4960                         # Offset from the top to the top
    # y0_offset = 6500 - y1_offset - height    # Offset from the bottom to the bottom
    width = 600
    height = 400
    x_offset = 850
    y1_offset = 4950                         # Offset from the top to the top
    y0_offset = 6500 - y1_offset - height    # Offset from the bottom to the bottom
    extent = [
        extent[0] + x_offset * 0.01,
        extent[3] - (y1_offset + height) * 0.01,
        extent[0] + (x_offset + width) * 0.01,
        extent[3] - y1_offset * 0.01,
    ]
    transform = [0.01, 0, extent[0], 0, -0.01, extent[3]]

    # The keys are the band names of the asset
    # The values are the band names in the download request
    # Matching the NLDAS-2 band naming in GEE for now
    nc_var_names = {
        'temperature': 'Tair',
        'specific_humidity': 'Qair',
        'pressure': 'PSurf',
        'wind_u': 'Wind_E',
        'wind_v': 'Wind_N',
        'shortwave_radiation': 'SWdown',
        'longwave_radiation': 'LWdown',
        'total_precipitation': 'Rainf',
    }
    src_nodata_value = -9999

    if ('eto_asce' in variables) or ('etr_asce' in variables):
        logging.debug('  Loading reference ET ancillary assets')

        # RasterIO can't read from the bucket directly when deployed as a function
        temp_bucket = STORAGE_CLIENT.bucket('openet_temp')
        elevation_url = 'gs://openet_temp/meteorology/nldas3/ancillary/elevation.tif'
        latitude_url = 'gs://openet_temp/meteorology/nldas3/ancillary/latitude.tif'
        longitude_url = 'gs://openet_temp/meteorology/nldas3/ancillary/longitude.tif'
        land_mask_url = 'gs://openet_temp/meteorology/nldas3/ancillary/land_mask.tif'
        # elevation_url = 'https://storage.googleapis.com/openet_temp/meteorology/nldas3/ancillary/elevation.tif'
        # latitude_url = 'https://storage.googleapis.com/openet_temp/meteorology/nldas3/ancillary/latitude.tif'
        # longitude_url = 'https://storage.googleapis.com/openet_temp/meteorology/nldas3/ancillary/longitude.tif'
        # land_mask_url = 'https://storage.googleapis.com/openet_temp/meteorology/nldas3/ancillary/land_mask.tif'
        elevation_path = os.path.join(ancillary_ws, 'elevation.tif')
        latitude_path = os.path.join(ancillary_ws, 'latitude.tif')
        longitude_path = os.path.join(ancillary_ws, 'longitude.tif')
        land_mask_path = os.path.join(ancillary_ws, 'land_mask.tif')

        if not os.path.isdir(ancillary_ws):
            os.makedirs(ancillary_ws)

        # Directly reading the URL above wasn't working when deployed
        logging.debug('  Downloading elevation')
        if not os.path.isfile(elevation_path):
            blob = temp_bucket.blob(elevation_url.replace('gs://openet_temp/', ''))
            blob.download_to_filename(elevation_path)
            # url_download(elevation_url, elevation_path)
        try:
            with rasterio.open(elevation_path) as src:
                elevation_array = src.read(1)
        except Exception as e:
            logging.exception(f'Unhandled exception: {e}')
            return f'{tgt_date} - Elevation array could not be read'
        
        logging.debug('  Downloading latitude')
        if not os.path.isfile(latitude_path):
            blob = temp_bucket.blob(latitude_url.replace('gs://openet_temp/', ''))
            blob.download_to_filename(latitude_path)
            # url_download(latitude_url, latitude_path)
        try:
            with rasterio.open(latitude_path) as src:
                latitude_array = src.read(1)
        except Exception as e:
            logging.exception(f'Unhandled exception: {e}')
            return f'{tgt_date} - Latitude array could not be read'
        
        logging.debug('  Downloading longitude')
        if not os.path.isfile(longitude_path):
            blob = temp_bucket.blob(longitude_url.replace('gs://openet_temp/', ''))
            blob.download_to_filename(longitude_path)
            # url_download(longitude_url, longitude_path)
        try:
            with rasterio.open(longitude_path) as src:
                longitude_array = src.read(1)
        except Exception as e:
            logging.exception(f'Unhandled exception: {e}')
            return f'{tgt_date} - Longitude array could not be read'

        logging.debug('  Downloading land_mask')
        if not os.path.isfile(land_mask_path):
            blob = temp_bucket.blob(land_mask_url.replace('gs://openet_temp/', ''))
            blob.download_to_filename(land_mask_path)
            # url_download(land_mask_url, land_mask_path)
        try:
            with rasterio.open(land_mask_path) as src:
                land_mask_array = src.read(1)
        except Exception as e:
            logging.exception(f'Unhandled exception: {e}')
            return f'{tgt_date} - Land mask array could not be read'

    elevation_array[elevation_array <= -9998] = 0

    if x_offset or y1_offset:
        land_mask_array = land_mask_array[y1_offset: height + y1_offset, x_offset: width + x_offset]
        elevation_array = elevation_array[y1_offset: height + y1_offset, x_offset: width + x_offset]
        latitude_array = latitude_array[y1_offset: height + y1_offset, x_offset: width + x_offset]
        longitude_array = longitude_array[y1_offset: height + y1_offset, x_offset: width + x_offset]

    # Always overwrite temporary files if the asset doesn't exist
    # if 'FUNCTION_REGION' in os.environ and os.path.isdir(date_ws):
    #     shutil.rmtree(date_ws)
    if not os.path.isdir(date_ws):
        os.makedirs(date_ws)

    # # DEADBEEF - Old code for downloading the NetCDF locally
    # # if not os.path.isfile(file_path) or overwrite_flag:
    # if not os.path.isfile(nc_file_path):
    #     logging.info('Downloading source netcdf')
    #     logging.info(f'  {nc_file_url}')
    #     s3 = s3fs.S3FileSystem({'anon': True})
    #     s3.download(nc_file_url.replace('s3://', ''), nc_file_path)
    #
    # # Check if the file was downloaded
    # if not os.path.isfile(nc_file_path):
    #     logging.warning(f'  {nc_file_name} does not exist -skipping')
    #     return f'{tgt_date} - {nc_file_name} does not exist - skipping'
    #
    # # TODO: Update min size and look for other ways to detect incomplete netcdf files
    # logging.debug('Checking netcdf')
    # logging.info(f'  {nc_file_path}')
    # if os.path.getsize(nc_file_path) < 2000000:
    #     logging.warning(f'  {nc_file_name} is incomplete - removing')
    #     os.remove(nc_file_path)
    #     return f'{tgt_date} - {nc_file_name} is incomplete - removing'

    # # # Open the NetCDF
    # # try:
    # #     logging.info('Opening netcdf')
    # #     src_ds = netCDF4.Dataset(nc_file_path)
    # # except Exception as e:
    # #     logging.warning(f'  {nc_file_name} error opening file - skipping')
    # #     logging.warning(f'  Exception: {e}')
    # #     # os.remove(nc_file_path)
    # #     return f'{tgt_date} - {nc_file_name} could not be opened - skipping'
    #
    # # Try opening using xarray instead of netcdf4
    # try:
    #     logging.info('Opening netcdf')
    #     src_ds = xr.open_dataset(nc_file_path, engine="h5netcdf")
    # except Exception as e:
    #     logging.warning(f'  {nc_file_name} error opening file - skipping')
    #     logging.warning(f'  Exception: {e}')
    #     return f'{tgt_date} - {nc_file_name} could not be opened - skipping'


    # Testing out reading directly from the bucket instead of downloading
    logging.info('\nOpening the netcdf file from the bucket')
    try:
        s3 = s3fs.S3FileSystem({'anon': True})
        logging.info(f'  {nc_file_url}')
        src_f = s3.open(nc_file_url)
        src_ds = xr.open_dataset(src_f, engine="h5netcdf")
    except Exception as e:
        logging.warning(f'  {nc_file_name} error opening file - skipping')
        logging.warning(f'  Exception: {e}')
        return f'{tgt_date} - {nc_file_name} could not be opened - skipping'


    # Iterate by hour then by variable
    for hour in hours:
        logging.info(f'  Hour: {hour}')
        hour_dt = tgt_dt.replace(hour=hour)
        upload_path = os.path.join(date_ws, f'{hour_dt.strftime(ASSET_DT_FMT)}.tif')
        bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{hour_dt.strftime(ASSET_DT_FMT)}.tif'
        # bucket_json = bucket_path.replace('.tif', '_properties.json')
        asset_id = f'{ASSET_COLL_ID}/{hour_dt.strftime(ASSET_DT_FMT)}'
        logging.debug(f'  {upload_path}')
        # logging.debug(f'  {bucket_json}')
        logging.debug(f'  {asset_id}')

        # Double check if the asset already exists
        if overwrite_flag and ee.data.getInfo(asset_id):
            logging.debug('  Removing existing asset')
            try:
                ee.data.deleteAsset(asset_id)
            except Exception as e:
                logging.warning(f'  Existing asset not deleted - skipping')
                logging.warning(f'  Exception: {e}')
                continue

        # if not os.path.isfile(local_path) or overwrite_flag:
        if not os.path.isfile(upload_path):
            logging.debug('  Reading component arrays')
            hourly_arrays = {}
            for variable in variables:
                logging.debug(f'    {variable}')

                # Skip the computed/derived variables
                if variable in ['wind_10m', 'eto_asce', 'etr_asce']:
                    hourly_arrays[variable] = None
                    continue

                # Read in the hourly data for the target variable and hour of day
                try:
                    var_ds = (
                        src_ds[nc_var_names[variable]]
                        .isel(time=hour, lat=slice(y0_offset, y0_offset+height), lon=slice(x_offset, x_offset+width))
                    )
                    nc_array = var_ds.to_numpy()
                except Exception as e:
                    logging.warning(f'  Error reading array - skipping')
                    logging.warning(f'  Exception: {e}')
                    continue

                # TODO: Add a check to see if this needs to be applied
                # Set the nodata pixels to NaN
                nc_array[nc_array == src_nodata_value] = np.nan

                if np.all(np.isnan(nc_array)):
                    logging.warning(f'  Array is all nodata - skipping')
                    continue
                    # return f'{tgt_date} - {variable} array is all nodata'

                hourly_arrays[variable] = np.flipud(nc_array[:, :])

                del nc_array

            if not hourly_arrays.keys():
                logging.info(f'  No arrays were downloaded - skipping')
                continue
                # return f'{tgt_date} - No arrays were downloaded'
            elif not all([v in hourly_arrays.keys() for v in variables]):
                logging.info(f'  Missing hourly arrays - skipping')
                continue
                # return f'{tgt_date} - Missing hourly arrays'

            # TODO: Check if this is needed
            # # Force values to be greater than or equal to one
            # # for var in ['total_precipitation', 'shortwave_radiation', 'longwave_radiation']:
            # for var in ['total_precipitation']:
            #     if var in hourly_arrays.keys():
            #         hourly_arrays[var][hourly_arrays[var] < 0] = 0
            #         # np.clip(hourly_arrays[var], a_min=0, a_max=None, out=hourly_arrays[var])

            # # Compute wind speed from component vectors
            # if 'wind_10m' in variables:
            #     hourly_arrays['wind_10m'] = np.sqrt(
            #         hourly_arrays['wind_u'] ** 2 + hourly_arrays['wind_v'] ** 2
            #     )

            # Compute Reference ET
            if ('eto_asce' in variables) or ('etr_asce' in variables):
                logging.debug('  Computing Reference ET')

                # Compute wind speed from component vectors
                wind_array = np.sqrt(hourly_arrays['wind_u'] ** 2 + hourly_arrays['wind_v'] ** 2)

                # Build computed RefET GeoTIFFs from the daily component GeoTIFFs
                for variable in ['eto_asce', 'etr_asce']:
                    logging.debug(f'    {variable}')

                    # Compute reference ET
                    # Letting function handle conversion of temperature and solar
                    # All other units should be the default values
                    # Surface pressure is in Pa but ea function is expecting kPa
                    refet_obj = refet.Hourly(
                        tmean=hourly_arrays['temperature'],
                        ea=refet.calcs._actual_vapor_pressure(
                            q=hourly_arrays['specific_humidity'],
                            pair=hourly_arrays['pressure'] / 1000
                        ),
                        rs=hourly_arrays['shortwave_radiation'],
                        uz=wind_array,
                        zw=10,
                        elev=elevation_array,
                        lat=latitude_array,
                        lon=longitude_array,
                        doy=int(hour_dt.strftime('%j')),
                        time=int(hour_dt.strftime('%H')),
                        # TODO: Maybe switch to 'refet' approach to get full rso calculation?
                        method='asce',
                        # method='refet',
                        input_units={'tmean': 'K', 'rs': 'W m-2 h-1'},
                    )
                    hourly_arrays[variable] = refet_obj.etsz(variable.split('_')[0].lower())

                    # # Set high latitude pixels to nodata
                    # hourly_arrays[variable][latitude_array >= 75] = np.nan
                    # hourly_arrays[variable][longitude_array <= -60] = np.nan
                    # hourly_arrays[variable][np.abs(lat_array) >= 75] = np.nan

                    # # Mask ocean pixels
                    # hourly_arrays[variable][land_mask_array == 0] = np.nan

                    del refet_obj

                del wind_array

            # Build composite image
            # if composite_flag and not os.path.isfile(upload_path):
            # Only build the composite if all the input images are available
            input_vars = set(hourly_arrays.keys())
            if not set(variables).issubset(input_vars):
                logging.info(
                    f'  Missing input variables for composite - skipping\n'
                    f'  {", ".join(list(set(variables) - input_vars))}'
                )
                continue
                # return f'{tgt_date} - Missing input variables for composite\n'\
                #        f'  {", ".join(list(set(variables) - input_vars))}'

            logging.debug('  Building output GeoTIFF')
            output_ds = rasterio.open(
                upload_path, 'w',
                driver='COG',
                blocksize=256,
                # driver='GTiff', tiled=True, blockxsize=256, blockysize=256,
                compress='deflate',
                # compress='lzw',
                count=len(variables),
                crs=crs,
                dtype=rasterio.float32,
                height=height,
                width=width,
                nodata=-9999,
                transform=transform,
            )

            logging.debug('  Writing arrays to composite image')
            for band_i, variable in enumerate(variables):
                output_ds.set_band_description(band_i + 1, variable)
                data_array = hourly_arrays[variable].astype(np.float32)
                data_array[np.isnan(data_array)] = -9999
                output_ds.write(data_array, band_i + 1)
                del data_array

            output_ds.build_overviews([2, 4, 8, 16], rasterio.warp.Resampling.average)
            output_ds.update_tags(ns='rio_overview', resampling='average')
            output_ds.close()
            del output_ds
            del hourly_arrays

        properties = {
            'date': hour_dt.strftime('%Y-%m-%d'),
            'date_ingested': datetime.today().strftime("%Y-%m-%d"),
            'doy': int(hour_dt.strftime('%j')),
            'hour': int(hour_dt.strftime('%H')),
            'source': nc_file_url,
            'status': 'beta',
        }
        if ('eto_asce' in variables) or ('etr_asce' in variables):
            properties['refet_version'] = importlib_metadata.version("refet")

        logging.debug('  Uploading geotiff to bucket')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(
            bucket_path.replace(f'gs://{BUCKET_NAME}/', ''),
            chunk_size=5 * 1024 * 1024,
        )
        try:
            blob.upload_from_filename(upload_path, timeout=120)
        except Exception as e:
            logging.warning(f'  Error uploading file to bucket - skipping')
            logging.warning(f'  Exception: {e}')
            continue
            # return f'{tgt_date} - exception uploading file to bucket\n'

        logging.debug('  Ingesting into Earth Engine')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        params = {
            'name': asset_id,
            'bands': [
                {'id': v, 'tilesetId': 'image', 'tilesetBandIndex': i}
                for i, v in enumerate(variables)
            ],
            'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
            'properties': properties,
            'startTime': hour_dt.isoformat() + '.000000000Z',
            # 'pyramidingPolicy': 'MEAN',
            # 'missingData': {'values': [nodata_value]},
        }
        try:
            ee.data.startIngestion(task_id, params, allow_overwrite=True)
        except Exception as e:
            logging.warning(f'  Error starting ingest task - skipping')
            logging.warning(f'  Exception: {e}')
            continue
            #return f'{tgt_date} - exception starting ingest task\n'

        # DEADBEEF - Old code for ingesting as a COG backed asset
        #   Leaving since we may need to use this approach long term
        # Add the system:time_start and system:index to the properties
        # properties['system:index'] = hour_dt.strftime('%Y%m%d%H')
        # properties['system:time_start'] = millis(hour_dt)
        # properties['uris'] = bucket_path

        # # Save the properties JSON file to the bucket
        # logging.info('  Uploading properties json to bucket')
        # bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        # blob = bucket.blob(bucket_json.replace(f'gs://{BU CKET_NAME}/', ''))
        # blob.upload_from_string(json.dumps(properties))

        # logging.info('  Registering COG')
        # request = {
        #     'type': 'IMAGE',
        #     'gcs_location': {'uris': [bucket_path]},
        #     'properties': properties,
        #     'startTime': hour_dt.isoformat() + 'Z',
        #     # 'startTime': hour_dt.isoformat() + '.000000000Z',
        # }
        # # try:
        # ee.data.createAsset(request, asset_id)
        # # except:
        # #     raise Exception('Unhandled exception registering COG')

        # TODO: Add support for a flag that leaves the intermediate files?
        # if 'FUNCTION_REGION' in os.environ and os.path.isfile(date_ws):
        # os.remove(upload_path)

    src_ds.close()
    del src_ds
    src_f.close()
    del src_f

    if ('eto_asce' in variables) or ('etr_asce' in variables):
        del elevation_array, land_mask_array, latitude_array, longitude_array

    # if ('FUNCTION_REGION' in os.environ) and os.path.isfile(nc_file_path):
    #     os.remove(nc_file_path)
    # if ('FUNCTION_REGION' in os.environ) and os.path.isdir(date_ws):
    #     shutil.rmtree(date_ws)

    # logging.info(f'  {tgt_date}')
    return f'{tgt_date}\n'


def hourly_asset_dates(start_dt, end_dt, hours=list(range(0, 24)), overwrite_flag=False):
    """Identify dates of missing NLDAS-3 hourly assets

    Parameters
    ----------
    start_dt : datetime
        Start datetime
    end_dt : datetime
        End datetime (exclusive)
    hours : list
        Specific hours to process
    overwrite_flag : bool, optional

    Returns
    -------
    list : datetimes

    """
    logging.debug('\nBuilding NLDAS-3 hourly asset ingest datetimes')

    task_id_re = re.compile(ASSET_COLL_ID.split('projects/')[-1] + '/(?P<date>\d{10})$')
    asset_id_re = re.compile(ASSET_COLL_ID.split('projects/')[-1] + '/(?P<date>\d{10})$')

    # Figure out which asset dates need to be ingested
    # Start with a list of dates to check
    # logging.debug('\nBuilding Date List')
    tgt_dt_list = list(datetime_range(start_dt, end_dt, hours))
    if not tgt_dt_list:
        logging.info('Empty date range')
        return []
    logging.debug('\nInitial test datetimes: {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y%m%d%H'), tgt_dt_list))
    ))

    # # Check if any of the needed dates are currently being ingested
    # # Check task list before checking asset list in case a task switches
    # #   from running to done before the asset list is retrieved.
    # logging.debug('\nChecking task list')
    # task_id_list = [
    #     desc.replace('\nAsset ingestion: ', '')
    #     for desc in get_ee_tasks(states=['RUNNING', 'READY']).keys()
    # ]
    # task_date_list = [
    #     datetime.strptime(m.group('date'), ASSET_DT_FMT).strftime('%Y%m%d%H')
    #     for task_id in task_id_list
    #     for m in [task_id_re.search(task_id)] if m
    # ]
    # logging.debug(f'\nTask dates: {", ".join(task_date_list)}')
    #
    # # Switch date list to be dates that are missing
    # tgt_dt_list = [
    #     dt for dt in tgt_dt_list
    #     if overwrite_flag or dt.strftime('%Y%m%d%H') not in task_date_list
    # ]
    # if not tgt_dt_list:
    #     logging.info('No dates to process after checking ready/running tasks')
    #     return []
    # logging.debug('\nDates (after filtering tasks): {}'.format(
    #     ', '.join(map(lambda x: x.strftime('%Y%m%d%H'), tgt_dt_list))
    # ))

    # Check if the assets already exist
    # For now, assume the collection exists
    logging.debug('\nChecking existing assets')
    asset_id_list = get_ee_assets(ASSET_COLL_ID, start_dt, end_dt)
    asset_date_list = [
        datetime.strptime(m.group('date'), ASSET_DT_FMT).strftime('%Y%m%d%H')
        for asset_id in asset_id_list
        for m in [asset_id_re.search(asset_id)] if m
    ]
    logging.debug(f'\nAsset dates: {", ".join(asset_date_list)}')

    # Switch date list to be dates that are missing
    tgt_dt_list = [
        dt for dt in tgt_dt_list
        if overwrite_flag or dt.strftime('%Y%m%d%H') not in asset_date_list
    ]
    if not tgt_dt_list:
        logging.info('No dates to process after filtering existing assets')
        return []
    logging.debug('\nIngest dates: {}'.format(', '.join(
        map(lambda x: x.strftime('%Y%m%d%H'), tgt_dt_list))
    ))

    return tgt_dt_list


# def queue_ingest_tasks(tgt_dt_list):
#     """Submit ingest tasks to the queue
#
#     Parameters
#     ----------
#     tgt_dt_list : list
#
#     Returns
#     -------
#     str : response string
#
#     """
#     logging.info('Queuing NLDAS-3 hourly asset ingest tasks')
#     response = 'Queue NLDAS-3 hourly asset ingest tasks\n'
#
#     TASK_CLIENT = tasks_v2.CloudTasksClient()
#     parent = TASK_CLIENT.queue_path(PROJECT_NAME, TASK_LOCATION, TASK_QUEUE)
#
#     for tgt_dt in tgt_dt_list:
#         logging.info(f'Date: {tgt_dt.strftime("%Y%m%d%H")}')
#         # response += 'Date: {}\n'.format(tgt_dt.strftime('%Y-%m-%d'))
#
#         # Using the default name in the request can create duplicate tasks
#         # Trying out adding the timestamp to avoid this for testing/debug
#         name = f'{parent}/tasks/nldas3_hawaii_meteorology_hourly_' \
#                f'{tgt_dt.strftime("%Y%m%d%H")}_' \
#                f'{datetime.today().strftime("%Y%m%d%H%M%S")}'
#         # name = f'{parent}/tasks/nldas3_hourly_asset_{tgt_dt.strftime("%Y%m%d%H")}'
#         response += name + '\n'
#         logging.info(name)
#
#         # Using the json body wasn't working, switching back to URL
#         # Couldn't get authentication with oidc_token to work
#         task = {
#             'http_request': {
#                 'http_method': tasks_v2.HttpMethod.POST,
#                 'url': '{}/{}?date={}'.format(
#                     FUNCTION_URL, FUNCTION_NAME, tgt_dt.strftime('%Y%m%d%H')),
#                 # 'url': '{}/{}?date={}&overwrite={}'.format(
#                 #     FUNCTION_URL, FUNCTION_NAME, tgt_dt.strftime('%Y%m%d%H'),
#                 #     str(overwrite_flag).lower()),
#                 # 'url': '{}/{}'.format(FUNCTION_URL, FUNCTION_NAME),
#                 # 'headers': {'Content-type': 'application/json'},
#                 # 'body': json.dumps(payload).encode(),
#                 # 'oidc_token': {
#                 #     'service_account_email': SERVICE_ACCOUNT,
#                 #     'audience': '{}/{}'.format(FUNCTION_URL, FUNCTION_NAME)},
#                 # 'relative_uri': ,
#             },
#             'name': name,
#         }
#         TASK_CLIENT.create_task(request={'parent': parent, 'task': task})
#
#         time.sleep(0.1)
#
#     return response


# def cron_scheduler(request):
#     """Parse JSON/request arguments and queue ingest tasks for a date range"""
#     args = {
#         # 'variables': VARIABLES,
#     }
#
#     request_json = request.get_json(silent=True)
#     request_args = request.args
#
#     if request_json and ('start' in request_json):
#         start_date = request_json['start']
#     elif request_args and ('start' in request_args):
#         start_date = request_args['start']
#     else:
#         start_date = None
#
#     if request_json and ('end' in request_json):
#         end_date = request_json['end']
#     elif request_args and ('end' in request_args):
#         end_date = request_args['end']
#     else:
#         end_date = None
#
#     if start_date is None and end_date is None:
#         today = datetime.today()
#         start_date = (datetime(today.year, today.month, today.day) -
#                       timedelta(days=START_DAY_OFFSET)).strftime('%Y-%m-%d')
#         end_date = (datetime(today.year, today.month, today.day) -
#                     timedelta(days=END_DAY_OFFSET)).strftime('%Y-%m-%d')
#     elif start_date is None or end_date is None:
#         abort(400, description='Both start and end date must be specified')
#
#     try:
#         args['start_dt'] = datetime.strptime(start_date, '%Y-%m-%d')
#     except:
#         abort(400, description=f'Start date {start_date} could not be parsed')
#     try:
#         args['end_dt'] = datetime.strptime(end_date, '%Y-%m-%d')
#     except:
#         abort(400, description=f'End date {end_date} could not be parsed')
#
#     if args['end_dt'] < args['start_dt']:
#         abort(400, description='End date must be after start date')
#
#     if request_json and ('hours' in request_json):
#         hours = request_json['hours']
#     elif request_args and ('hours' in request_args):
#         hours = request_args['hours']
#     else:
#         hours = '0-23'
#     args['hours'] = parse_int_set(hours)
#
#     # CGM - For now don't allow scheduler calls to overwrite existing assets
#     # if request_json and ('overwrite' in request_json):
#     #     overwrite_flag = request_json['overwrite']
#     # elif request_args and ('overwrite' in request_args):
#     #     overwrite_flag = request_args['overwrite']
#     # else:
#     #     overwrite_flag = 'false'
#     #
#     # if overwrite_flag.lower() in ['true', 't']:
#     #     args['overwrite_flag'] = True
#     # elif overwrite_flag.lower() in ['false', 'f']:
#     #     args['overwrite_flag'] = False
#     # else:
#     #     abort(400, description=f'overwrite="{overwrite_flag}" could not be parsed')
#
#     # # CGM - Should the scheduler be responsible for clearing the bucket?
#     # logging.info('Clearing all files from bucket folder')
#     # bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
#     # blobs = bucket.list_blobs()
#     # # blobs = bucket.list_blobs(prefix=BUCKET_FOLDER)
#     # for blob in blobs:
#     #     blob.delete()
#
#     response = hourly_asset_ingest(hourly_asset_dates(**args))
#     # response = queue_ingest_tasks(hourly_asset_dates(**args))
#
#     return Response(response, mimetype='text/plain')
#
#
# def cron_worker(request):
#     """Parse JSON/request arguments and start ingest for a single date export"""
#     args = {
#         'variables': VARIABLES,
#         'workspace': '/tmp',
#     }
#
#     request_json = request.get_json(silent=True)
#     request_args = request.args
#
#     if request_json and ('date' in request_json):
#         tgt_date = request_json['date']
#     elif request_args and ('date' in request_args):
#         tgt_date = request_args['date']
#     else:
#         abort(400, description='date parameter not set')
#
#     try:
#         args['tgt_dt'] = datetime.strptime(tgt_date, '%Y%m%d%H')
#         # args['tgt_dt'] = datetime.strptime(tgt_date, '%Y-%m-%d')
#     except:
#         abort(400, description=f'date "{tgt_date}" could not be parsed')
#
#     if request_json and 'overwrite' in request_json:
#         overwrite_flag = request_json['overwrite']
#     elif request_args and 'overwrite' in request_args:
#         overwrite_flag = request_args['overwrite']
#     else:
#         overwrite_flag = 'false'
#
#     if overwrite_flag.lower() in ['true', 't']:
#         args['overwrite_flag'] = True
#     elif overwrite_flag.lower() in ['false', 'f']:
#         args['overwrite_flag'] = False
#     else:
#         abort(400, description=f'overwrite "{overwrite_flag}" could not be parsed')
#
#     response = hourly_asset_ingest(**args)
#     return Response(response, mimetype='text/plain')


def datetime_range(start_dt, end_dt, hours=list(range(0, 24))):
    """Generate hourly datetimes within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date (exclusive).
    hours : list, optional
        List of specific hours to include.

    Yields
    ------
    datetime

    """
    import copy
    curr_dt = copy.copy(start_dt)
    while curr_dt < end_dt:
        if hours and curr_dt.hour not in hours:
            pass
        else:
            yield curr_dt
        curr_dt += timedelta(hours=1)


def get_ee_assets(asset_id, start_dt=None, end_dt=None, retries=4):
    """Return assets IDs in a collection

    Parameters
    ----------
    asset_id : str
        A folder or image collection ID.
    start_dt : datetime, optional
        Start date (inclusive).
    end_dt : datetime, optional
        End date (exclusive, similar to .filterDate()).
    retries : int, optional
        The number of times to retry the call (the default is 4).

    Returns
    -------
    list : Asset IDs

    """
    # # CGM - There is a bug in earthengine-api>=0.1.326 that causes listImages()
    # #   to return an empty list if the startTime and endTime parameters are set
    # # Switching to a .aggregate_array(system:index).getInfo() approach for now
    # #   since getList is flagged for deprecation
    coll = ee.ImageCollection(asset_id)
    if start_dt and end_dt:
        coll = coll.filterDate(start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d'))
    # params = {'parent': asset_id}
    # if start_dt and end_dt:
    #     # CGM - Do both start and end need to be set to apply filtering?
    #     params['startTime'] = start_dt.isoformat() + '.000000000Z'
    #     params['endTime'] = end_dt.isoformat() + '.000000000Z'

    asset_id_list = []
    for i in range(1, retries):
        try:
            asset_id_list = coll.aggregate_array('system:index').getInfo()
            asset_id_list = [f'{asset_id}/{id}' for id in asset_id_list]
            # asset_id_list = [x['id'] for x in ee.data.listImages(params)['images']]
            break
        except ValueError:
            logging.info('  Collection or folder does not exist')
            raise sys.exit()
        except Exception as e:
            logging.error(f'  Error getting asset list, retrying ({i}/{retries})\n  {e}')
            time.sleep(i ** 3)

    return asset_id_list


def get_ee_tasks(states=['RUNNING', 'READY']):
    """Return current active tasks

    Parameters
    ----------
    states : list

    Returns
    -------
    dict : Task descriptions (key) and task IDs (value).

    """
    tasks = {}
    for i in range(1, 6):
        try:
            # task_list = ee.data.listOperations()
            task_list = ee.data.getTaskList()
            task_list = sorted([
                [t['state'], t['description'], t['id']]
                for t in task_list if t['state'] in states
            ])
            tasks = {t_desc: t_id for t_state, t_desc, t_id in task_list}
            break
        except Exception as e:
            logging.info(
                '  Error getting active task list, retrying ({}/10)\n'
                '  {}'.format(i, e))
            time.sleep(i ** 2)

    return tasks


def parse_int_set(nputstr=""):
    """Return list of numbers given a string of ranges

    http://thoughtsbyclayg.blogspot.com/2008/10/parsing-list-of-numbers-in-python.html
    """
    selection = set()
    invalid = set()
    # tokens are comma separated values
    tokens = [x.strip() for x in nputstr.split(',')]
    for i in tokens:
        try:
            # typically tokens are plain old integers
            selection.add(int(i))
        except:
            # if not, then it might be a range
            try:
                token = [int(k.strip()) for k in i.split('-')]
                if len(token) > 1:
                    token.sort()
                    # we have items separated by a dash
                    # try to build a valid range
                    first = token[0]
                    last = token[len(token) - 1]
                    for x in range(first, last + 1):
                        selection.add(x)
            except:
                # not an int and not a range...
                invalid.add(i)

    return selection


def url_download(download_url, output_path, verify=True):
    """Download file from a URL using requests module

    Parameters
    ----------
    download_url : str
    output_path : str
    verify : bool, optional

    Returns
    -------
    None

    """
    for i in range(1, 6):
        try:
            response = requests.get(download_url, stream=True, verify=verify)
        except Exception as e:
            logging.info(f'  Exception: {e}')
            input('ENTER')
            return False

        logging.debug(f'  HTTP Status: {response.status_code}')
        if response.status_code == 200:
            pass
        elif response.status_code == 404:
            logging.debug('  Skipping')
            return False
        else:
            logging.info(f'  HTTPError: {response.status_code}')
            logging.info(f'  Retry attempt: {i}')
            time.sleep(i ** 2)
            continue

        logging.debug('  Beginning download')
        try:
            with (open(output_path, 'wb')) as output_f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:  # filter out keep-alive new chunks
                        output_f.write(chunk)
            logging.debug('  Download complete')
            return True
        except Exception as e:
            logging.info(f'  Exception: {e}')
            return False


def arg_valid_date(input_date):
    """Check that a date string is ISO format (YYYY-MM-DD)

    This function is used to check the format of dates entered as command
      line arguments.
    DEADBEEF - It would probably make more sense to have this function
      parse the date using dateutil parser (http://labix.org/python-dateutil)
      and return the ISO format string

    Parameters
    ----------
    input_date : string

    Returns
    -------
    datetime

    Raises
    ------
    ArgParse ArgumentTypeError

    """
    try:
        return datetime.strptime(input_date, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{}'.".format(input_date)
        raise argparse.ArgumentTypeError(msg)


def arg_valid_file(file_path):
    """Argparse specific function for testing if file exists

    Convert relative paths to absolute paths
    """
    if os.path.isfile(os.path.abspath(os.path.realpath(file_path))):
        return os.path.abspath(os.path.realpath(file_path))
        # return file_path
    else:
        raise argparse.ArgumentTypeError('{} does not exist'.format(file_path))


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest NLDAS-3 Hawaii hourly assets into Earth Engine',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--workspace', metavar='PATH',
        default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
    parser.add_argument(
        '--start', type=arg_valid_date, metavar='YYYY-MM-DD',
        help='Start date')
    parser.add_argument(
        '--end', type=arg_valid_date, metavar='YYYY-MM-DD',
        help='End date (exclusive)')
    parser.add_argument(
        '-v', '--variables', nargs='+', metavar='VAR', default=VARIABLES,
        choices=VARIABLES, help='NLDAS-3 variables')
    parser.add_argument(
        '--hours', default='0-23', type=str,
        help='Comma separated or range of hours')
    parser.add_argument(
        '--download', default=False, action='store_true',
        help='Download NetCDFs but do not start asset ingest')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--reverse', default=False, action='store_true',
        help='Process dates in reverse order')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    # Convert relative paths to absolute paths
    if args.workspace and os.path.isdir(os.path.abspath(args.workspace)):
        args.workspace = os.path.abspath(args.workspace)

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    # Build the image collection if it doesn't exist
    logging.debug('Image Collection: {}'.format(ASSET_COLL_ID))
    asset_folder = ASSET_COLL_ID.rsplit('/', 1)[0]
    if not ee.data.getInfo(asset_folder):
        logging.info('\nFolder does not exist and will be built'
                     '\n  {}'.format(asset_folder))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'FOLDER'}, asset_folder)
    if not ee.data.getInfo(ASSET_COLL_ID):
        logging.info('\nImage collection does not exist and will be built'
                     '\n  {}'.format(ASSET_COLL_ID))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, ASSET_COLL_ID)

    ingest_dt_list = hourly_asset_dates(
        start_dt=args.start, end_dt=args.end, overwrite_flag=args.overwrite
        #hours=parse_int_set(args.hours),
    )

    # Ingest function will process all hours in the day since data is stored by day
    #   so collapse hourly ingest list to a date list
    ingest_dt_list = {x.replace(hour=0) for x in ingest_dt_list}

    for ingest_dt in sorted(ingest_dt_list, reverse=args.reverse):
        response = hourly_asset_ingest(
            ingest_dt,
            hours=parse_int_set(args.hours),
            variables=args.variables,
            workspace=args.workspace,
            overwrite_flag=args.overwrite,
        )
        logging.info(f'  {response}')

    # from unittest.mock import Mock
    # for ingest_dt in sorted(ingest_dt_list, reverse=args.reverse):
    #     data = {'date': ingest_dt.strftime('%Y%m%d'), 'overwrite': args.overwrite}
    #     logging.info(data)
    #     req = Mock(get_json=Mock(return_value=data), args=data)
    #     response = cron_worker(req)

    # queue_ingest_tasks(ingest_dt_list)
