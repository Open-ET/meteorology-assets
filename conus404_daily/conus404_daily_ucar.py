import argparse
from datetime import datetime, timedelta, timezone
import importlib_metadata
# import importlib.metadata
import json
import logging
import os
import pprint
# import shutil
import time

import ee
from google.cloud import storage
import numpy as np
# import openet.core.utils as utils
from pydap.client import open_url
# from pydap.cas.urs import setup_session
import rasterio
import rasterio.warp
import refet

# import config

# logging.getLogger('ee').setLevel(logging.WARNING)
# logging.getLogger('googleapiclient').setLevel(logging.ERROR)
# logging.getLogger('pydap').setLevel(logging.WARNING)
# logging.getLogger('rasterio').setLevel(logging.INFO)
# logging.getLogger('requests').setLevel(logging.INFO)
# logging.getLogger('urllib3').setLevel(logging.INFO)

ASSET_COLL_ID = 'projects/openet/assets/meteorology/conus404/daily'
BUCKET_NAME = 'openet_assets'
BUCKET_FOLDER = 'meteorology/conus404/daily'
# ASSET_COLL_ID = 'projects/openet/assets/meteorology/conus404/daily_ucar'
# BUCKET_NAME = 'openet_temp'
# BUCKET_FOLDER = 'meteorology/conus404/daily_ucar'

ASSET_DT_FMT = '%Y%m%d'
PROJECT_NAME = 'openet'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)
NC_URL = "https://rda.ucar.edu/thredds/dodsC/files/g/ds559.0"
# VARIABLES = [
#     'temperature_2m_max', 'temperature_2m_min', 'dewpoint_temperature_2m',
#     'surface_pressure', 'wind_10m', 'surface_solar_radiation_downwards',
#     # 'surface_net_solar_radiation', 'surface_net_thermal_radiation',
#     'total_precipitation', 'eto_asce', 'etr_asce',
# ]
VARIABLES = [
    'T2_MIN', 'T2_MAX', 'TD2', 'WIND10', 'PSFC', 'ACSWDNB', 'PREC_ACC_NC',
    'ETO_ASCE', 'ETR_ASCE',
]
NC_VARS = [
    'T2', 'TD2', 'U10', 'V10', 'PSFC', 'ACSWDNB', 'PREC_ACC_NC',
    'I_ACSWDNB',
    # 'SWDNB', 'LWDNB', 'ACSWDNLSM',
    # 'SNOWC', 'Q2', 'GLW', 'SWDOWN',
]
START_HOUR_OFFSET = 6

CELLSIZE = 4000
CRS = rasterio.crs.CRS.from_proj4(
    '+proj=lcc +lat_1=30.0 +lat_2=50.0 +lat_0=39.100006 +lon_0=-97.9 '
    '+a=6370000 +b=6370000 +units=m +no_defs=True'
)
# The crs parameter needs to be manually set with the WKT in the ingest call
# To get the alignment right the datum needs to be WGS84 (instead of a sphere)
#   and the semi_major and semi_minor parameters need to be added
# I'm not sure exactly what to set the semi_major/semi_minor to
#   The FRET wkt had 6371200 but WRF sphere seems to be 6370000
EE_WKT = (
    "PROJCS[\"NWS CONUS\",GEOGCS[\"WGS84\",DATUM[\"World Geodetic System 1984\","
    "SPHEROID[\"WGS84\",6378137.0,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],"
    "AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0.0,AUTHORITY[\"EPSG\",\"8901\"]],"
    "UNIT[\"degree\",0.017453292519943295],"
    "AXIS[\"Geodetic longitude\",EAST],AXIS[\"Geodetic latitude\",NORTH],"
    "AUTHORITY[\"EPSG\",\"4326\"]],"
    "PROJECTION[\"Lambert_Conformal_Conic_2SP\"],"
    "PARAMETER[\"latitude_of_origin\",39.100006],PARAMETER[\"central_meridian\",-97.9],"
    "PARAMETER[\"standard_parallel_1\",30],PARAMETER[\"standard_parallel_2\",50],"
    "PARAMETER[\"false_easting\",0],PARAMETER[\"false_northing\",0],"
    "PARAMETER[\"semi_major\",6370000.0],PARAMETER[\"semi_minor\",6370000.0],"
    "UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],"
    "AXIS[\"Easting\",EAST],AXIS[\"Northing\",NORTH]]"
)
DTYPE = rasterio.float32
NODATA_VALUE = -9999
OVERVIEW_LEVELS = [2, 4, 8]
SHAPE = (1015, 1367)
TRANSFORM = [
    CELLSIZE, 0.0, -(SHAPE[1] / 2) * CELLSIZE,
    0.0, -CELLSIZE, (SHAPE[0] / 2) * CELLSIZE
]

TOOL_NAME = 'conus404_daily_ucar'
# TOOL_NAME = os.path.basename(__file__)
TOOL_VERSION = '0.1.0'

# TODO: Add support for Initializing from a json key file and/or ADC
# if 'FUNCTION_REGION' in os.environ:
# SCOPES = [
#     'https://www.googleapis.com/auth/cloud-platform',
#     'https://www.googleapis.com/auth/earthengine',
# ]
# credentials, project_id = google.auth.default(default_scopes=SCOPES)
# ee.Initialize(credentials)
ee.Initialize()


def main(start_dt, end_dt, variables,
         # download_flag=False, upload_flag=False,
         workspace='/tmp', overwrite_flag=False, reverse_flag=False
    ):
    """"""

    logging.info('CONUS404 Daily Asset Ingest')

    for v in variables:
        if v not in VARIABLES:
            raise ValueError(f'unsupported variable {v}')

    # TODO: Switch to reading ancillary arrays from bucket assets
    logging.debug(f'\nReading ancillary arrays')
    ancillary_ws = os.path.join(
        os.path.dirname(workspace), 'conus404_tools', 'ancillary'
    )
    with rasterio.open(os.path.join(ancillary_ws, 'elevation.tif')) as src:
        elevation = src.read(1)
    with rasterio.open(os.path.join(ancillary_ws, 'latitude.tif')) as src:
        latitude = src.read(1)
    # with rasterio.open(os.path.join(ancillary_ws, 'longitude.tif')) as src:
    #     longitude = src.read(1)

    # Set elevation ocean pixels to nodata so that reference ET is masked in the ocean
    with rasterio.open(os.path.join(ancillary_ws, 'mask.tif')) as src:
        mask = src.read(1)
    # with rasterio.open(os.path.join(ancillary_ws, 'land_mask.tif')) as src:
    #     land_mask = src.read(1)
    # with rasterio.open(os.path.join(ancillary_ws, 'lake_mask.tif')) as src:
    #     lake_mask = src.read(1)
    # elevation[mask == 0] = np.nan

    # logging.info(f'\nProcessing dates')
    logging.info('')
    for tgt_dt in sorted(datetime_range(start_dt, end_dt, hours=24), reverse=reverse_flag):

        conus404_daily_asset_ingest(
            tgt_dt, variables, workspace=workspace, overwrite_flag=overwrite_flag,
            elevation=elevation, latitude=latitude, longitude=None, mask=mask,
        )


def conus404_daily_asset_ingest(
        tgt_dt, variables, workspace, elevation=None, latitude=None,
        longitude=None, mask=None, overwrite_flag=False,
    ):
    """"""
    logging.info(f'{tgt_dt}')

    # TODO: Work on restructuring variable mappings to make it more readable
    #   and easier to support other aggregations and variables
    refet_vars = ['T2_MAX', 'T2_MIN', 'TD2', 'WIND10', 'ACSWDNB']
    refet_nc_vars = ['T2', 'TD2', 'U10', 'V10', 'ACSWDNB']
    wind_nc_vars = ['U10', 'V10']

    # Build a list of the variables that need to be read from the netcdf
    nc_vars = {v for v in variables if v in NC_VARS}
    if 'ETO_ASCE' in variables or 'ETR_ASCE' in variables:
        nc_vars.update(refet_nc_vars)
    if 'WIND10' in variables:
        nc_vars.update(wind_nc_vars)
    if 'T2_MIN' in variables or 'T2_MAX' in variables:
        nc_vars.update(['T2'])
    if 'TD2' in variables:
        nc_vars.update(['TD2'])

    # Hourly values will be aggregated as the mean unless specified below
    # The aggregation source needs to be specified if different then the
    #   variable name in the netcdf file
    agg_src = {
        'T2_MAX': 'T2',
        'T2_MIN': 'T2',
        'PREC_ACC_NC': 'PREC_ACC_NC',
    }
    agg_type = {
        'T2_MAX': 'maximum',
        'T2_MIN': 'minimum',
        'ACSWDNB': 'accumulation',
        'ACSWDNLSM': 'sum',
        'PREC_ACC_NC': 'sum',
    }

    # Every accumulation variable needs the "wrap" counter variable also
    accumulation_vars = []
    for v, a_type in agg_type.items():
        if a_type == 'accumulation':
            accumulation_vars.extend([v, 'I_' + v])

    date_ws = os.path.join(workspace, tgt_dt.strftime('%Y'))
    # date_ws = os.path.join(
    #     workspace, tgt_dt.strftime('%Y'), tgt_dt.strftime('%m'), tgt_dt.strftime('%d')
    # )
    if not os.path.isdir(date_ws):
        os.makedirs(date_ws)

    upload_path = os.path.join(date_ws, f'{tgt_dt.strftime(ASSET_DT_FMT)}.tif')
    bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/' \
                  f'{tgt_dt.strftime(ASSET_DT_FMT)}.tif'
    bucket_json = bucket_path.replace('.tif', '_properties.json')
    asset_id = f'{ASSET_COLL_ID}/{tgt_dt.strftime(ASSET_DT_FMT)}'
    logging.debug(f'  {upload_path}')
    logging.debug(f'  {bucket_path}')
    logging.debug(f'  {asset_id}')

    # Set start time to 6 UTC to match GRIDMET (or 7?)
    # This should help set the solar sum and tmax/tmin correctly
    start_dt = tgt_dt + timedelta(hours=START_HOUR_OFFSET)
    end_dt = start_dt + timedelta(hours=24)
    # start_date = ee.Date.fromYMD(tgt_dt.year, tgt_dt.month, tgt_dt.day)\
    #     .advance(START_HOUR_OFFSET, 'hour')
    # end_date = start_date.advance(1, 'day')
    logging.debug(f'  Start: {start_dt}')
    logging.debug(f'  End:   {end_dt}')


    if ee.data.getInfo(asset_id):
        if overwrite_flag:
            logging.info(f'  Asset already exists, removing')
            try:
                ee.data.deleteAsset(asset_id)
            except Exception as e:
                logging.exception(f'unhandled exception: {e}')
                return False
        else:
            logging.info(f'  Asset already exists and overwrite is False')
            return True

    # CGM - It seems to work fine without initiating the session, commenting out for now
    # # Initiate the PyDAP session
    # # TODO: Should the session be initiated for every hour or passed into the function?
    # try:
    #     session = setup_session(
    #         username=config.ucar_username,
    #         password=config.ucar_password,
    #         check_url=nc_url,
    #         # check_url=f'{NC_URL}/wy1980/197910/wrf2d_d01_1979-10-01_00:00:00.nc',
    #     )
    # except Exception as e:
    #     logging.exception(f'\nException: {e}')
    #     return False

    # Initialize the hourly arrays
    h_arrays = {
        v: np.full((24, SHAPE[0], SHAPE[1]), np.nan, DTYPE)
        # v: np.empty((24, SHAPE[0], SHAPE[1]), DTYPE)
        # v: np.full((24, SHAPE[0], SHAPE[1]), NODATE_VALUE, DTYPE)
        for v in nc_vars
        if v not in accumulation_vars
    }

    # The accumulation variables only need the first and last value
    # Each accumulation variable needs the wrap bucket variable also
    for v in accumulation_vars:
        h_arrays[v] = np.full((2, SHAPE[0], SHAPE[1]), np.nan, DTYPE)
        # h_arrays[v] = np.full((25, SHAPE[0], SHAPE[1]), np.nan, DTYPE)


    # Iterate one extra hour to get the final solar accumulation
    # This extra time step will be dropped below
    for hour_i, hour_dt in enumerate(datetime_range(start_dt, end_dt, hours=1)):
        logging.debug(f'  Hour: {hour_dt}')
        wy = (datetime(hour_dt.year, hour_dt.month, 1) + timedelta(days=100)).year
        nc_url = f'{NC_URL}/wy{wy}/{hour_dt.strftime("%Y%m")}/' \
                 f'wrf2d_d01_{hour_dt.strftime("%Y-%m-%d_%H")}:00:00.nc'

        # CGM - Still sometimes getting timeout errors even after setting timeout
        #   I don't seem to need a session parameter
        #   Passing the variable in the URL didn't seem any faster
        try:
            nc_ds = open_url(nc_url, timeout=120)
            # nc_ds = open_url(nc_url, session=session, timeout=120)
            # nc_ds = open_url(f'{nc_url}?{variable}', timeout=120)
        except Exception as e:
            logging.exception(e)
            logging.warning('unhandled exception opening url, exiting')
            return False

        for variable in nc_vars:
            if variable in accumulation_vars:
                continue

            try:
                array = nc_ds[variable][:].data
            except Exception as e:
                logging.exception(e)
                logging.warning('unhandled exception readying array, exiting')
                return False

            # Subset and flip the arrays if needed
            if array.shape == (1, SHAPE[0], SHAPE[1]):
                h_arrays[variable][hour_i, :, :] = np.flipud(array[0, :, :])
            elif array.shape == (SHAPE[0], SHAPE[1]):
                h_arrays[variable][hour_i, :, :] = np.flipud(array[:, :])
            else:
                logging.warning(f'unexpected array shape ({array.shape}), exiting')
                return False
            del array
        del nc_ds


    # Read the solar accumulation values
    # for hour_i, hour_dt in enumerate(datetime_range(start_dt, end_dt + timedelta(hours=1), hours=1)):
    for hour_i, hour_dt in enumerate([start_dt, end_dt]):
        logging.debug(f'  Hour: {hour_dt} - accumulation variables')
        wy = (datetime(hour_dt.year, hour_dt.month, 1) + timedelta(days=100)).year
        nc_url = f'{NC_URL}/wy{wy}/{hour_dt.strftime("%Y%m")}/' \
                 f'wrf2d_d01_{hour_dt.strftime("%Y-%m-%d_%H")}:00:00.nc'
        try:
            nc_ds = open_url(nc_url, timeout=120)
        except Exception as e:
            logging.exception(e)
            logging.warning('unhandled exception opening url, exiting')
            return False

        for variable in accumulation_vars:
            try:
                array = nc_ds[variable][:].data
            except Exception as e:
                logging.exception(e)
                logging.warning('unhandled exception readying array, skipping')
                continue

            # Subset and flip the arrays if needed
            if array.shape == (1, SHAPE[0], SHAPE[1]):
                h_arrays[variable][hour_i, :, :] = np.flipud(array[0, :, :])
            elif array.shape == (SHAPE[0], SHAPE[1]):
                h_arrays[variable][hour_i, :, :] = np.flipud(array[:, :])
            else:
                logging.warning(f'unexpected array shape ({array.shape}), skipping')
                continue
            del array
        del nc_ds


    # Compute the wind magnitude for each hour
    if 'WIND10' in variables and 'WIND10' not in h_arrays.keys():
        # if 'U10' not in h_arrays.keys() or 'V10' not in h_arrays.keys():
        #     logging.error('Wind speed component arrays were not present')
        #     return False
        h_arrays['WIND10'] = np.sqrt(h_arrays['U10'] ** 2 + h_arrays['V10'] ** 2)
        if 'U10' not in variables:
            del h_arrays['U10']
        if 'V10' not in variables:
            del h_arrays['V10']


    # # DEADBEEF - Computing the daily directly instead
    # # Compute the reference ET for each hour
    # # logging.debug('  Computing hourly reference ET')
    # for hour_i in range(24):
    #     refet_obj = refet.Hourly(
    #         tmean=h_arrays['T2'][hour_i] - 273.15,
    #         tdew=h_arrays['TD2'][hour_i] - 273.15,
    #         # Convert SWDNB to MJ m-2 (was converted to J m-2 above)
    #         rs=(h_arrays['ACSWDNB'][hour_i+1] - h_arrays['ACSWDNB'][hour_i]) / 1000000,
    #         uz=h_arrays['WIND10'][hour_i],
    #         zw=10,
    #         elev=elevation,
    #         lat=latitude,
    #         lon=longitude,
    #         doy=int((start_dt + timedelta(hours=hour_i)).strftime('%j')),
    #         time=int((start_dt + timedelta(hours=hour_i)).strftime('%H')),
    #         method='asce',
    #     )
    #     if 'ETO_ASCE' in variables:
    #         h_arrays['ETO_ASCE'][h_key] = refet_obj.etsz('eto')
    #     if 'ETR_ASCE' in variables:
    #         h_arrays['ETR_ASCE'][h_key] = refet_obj.etsz('etr')


    # Compute daily aggregations
    # Assume a mean aggregation if not specified
    # Assume variable and nc_var names match if not specified
    logging.debug('  Compute daily aggregations')
    daily_arrays = {}
    for v in variables:
        try:
            v_src = agg_src[v]
        except KeyError:
            v_src = v
        if v_src not in h_arrays.keys():
            continue

        if v not in agg_type.keys() or agg_type[v].lower() in ['mean', 'average']:
            daily_arrays[v] = np.mean(h_arrays[v_src], axis=0)
        elif agg_type[v].lower() == 'minimum':
            daily_arrays[v] = np.min(h_arrays[v_src], axis=0)
        elif agg_type[v].lower() == 'maximum':
            daily_arrays[v] = np.max(h_arrays[v_src], axis=0)
        elif agg_type[v].lower() == 'sum':
            daily_arrays[v] = np.sum(h_arrays[v_src], axis=0)
        elif agg_type[v].lower() == 'accumulation':
            # The accumulation values wrap after 1E9
            i_offset = (h_arrays['I_' + v][-1] - h_arrays['I_' + v][0]) * 1000000000
            daily_arrays[v] = (h_arrays[v_src][-1] + i_offset) - h_arrays[v_src][0]

    # Cleanup
    del h_arrays


    if 'ETO_ASCE' in variables or 'ETR_ASCE' in variables:
        # TODO: Check if all of the variables needed to compute reference are present
        for v in refet_vars:
            if v not in daily_arrays.keys():
                logging.error(f'The daily array for variable {v} is missing, skipping date')
                return False

        logging.debug('  Computing daily reference ET')
        refet_obj = refet.Daily(
            tmin=daily_arrays['T2_MIN'] - 273.15,
            tmax=daily_arrays['T2_MAX'] - 273.15,
            tdew=daily_arrays['TD2'] - 273.15,
            # Convert ACSWDNB to MJ m-2
            rs=daily_arrays['ACSWDNB'] / 1000000,
            # SWDOWN aggregated as the daily sum and converted to MJ m-2
            # rs=daily_arrays['SWDOWN'] * 0.0036,
            # SWDOWN aggregated as the daily mean and converted to MJ m-2
            # rs=daily_arrays['SWDOWN'] * 0.0864,
            uz=daily_arrays['WIND10'],
            zw=10,
            elev=elevation,
            lat=latitude,
            doy=int(tgt_dt.strftime('%j')),
            method='asce',
        )
        if 'ETO_ASCE' in variables:
            daily_arrays['ETO_ASCE'] = refet_obj.etsz('eto')
        if 'ETR_ASCE' in variables:
            daily_arrays['ETR_ASCE'] = refet_obj.etsz('etr')

    # TODO: Check if all of the expected daily variables are present
    for v in variables:
        if v not in daily_arrays.keys():
            logging.error(f'The daily array for variable {v} is missing, skipping date')
            return False

    # Apply the mask if necessary
    if mask is not None:
        for v in variables:
            daily_arrays[v][mask == 0] = NODATA_VALUE

    # if os.path.isfile(upload_path) and not overwrite_flag:
    #     logging.debug('  Composite raster already exists, skipping')

    # Build the images as COGs just in case we use them as COG backed assets
    # Deflate seemed to make the files about 10% smaller than LZW
    logging.debug('  Writing geotiff')
    output_ds = rasterio.open(
        upload_path, 'w', driver='COG', blocksize=256,
        dtype=DTYPE, nodata=NODATA_VALUE, compress='deflate',
        width=SHAPE[1], height=SHAPE[0], count=len(variables),
        crs=CRS, transform=TRANSFORM,
    )
    # # DEADBEEF - Save as geotiff instead of COG
    # output_ds = rasterio.open(
    #     upload_path, 'w', driver='GTiff', blockxsize=256, blockysize=256,
    #     dtype=DTYPE, nodata=NODATA_VALUE, compress='deflate',
    #     width=SHAPE[1], height=SHAPE[0], count=len(hourly_arrays.keys()),
    #     crs=CRS, transform=TRANSFORM,
    # )
    for band_i, variable in enumerate(variables):
        output_ds.set_band_description(band_i+1, variable)
        d_array = daily_arrays[variable]
        d_array[np.isnan(d_array)] = NODATA_VALUE
        output_ds.write(d_array, band_i+1)
        del d_array
    output_ds.build_overviews(OVERVIEW_LEVELS, rasterio.warp.Resampling.average)
    output_ds.update_tags(ns='rio_overview', resampling='average')
    output_ds.close()
    del output_ds
    del daily_arrays


    # CGM - Uploading the properties json should happen before uploading the image
    #   to warmup the cloud function
    logging.debug('  Uploading properties json to bucket')
    properties = {
        'system:index': tgt_dt.strftime(ASSET_DT_FMT),
        'system:time_start': millis(start_dt),
        'date': tgt_dt.strftime('%Y-%m-%d'),
        'date_ingested': datetime.today().strftime("%Y-%m-%d"),
        'doy': int(tgt_dt.strftime('%j')),
        # 'hour': int(tgt_dt.strftime('%H')),
        'source': NC_URL,
        'tool_name': TOOL_NAME,
        'tool_version': TOOL_VERSION,
        'uris': bucket_path,
    }
    if 'ETO_ASCE' in variables or 'ETR_ASCE' in variables:
        properties['refet_version'] = importlib_metadata.version("refet")
        # properties['refet_version'] = importlib.metadata.version("refet")

    # Save the properties JSON file to the bucket
    bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    blob = bucket.blob(bucket_json.replace(f'gs://{BUCKET_NAME}/', ''))
    blob.upload_from_string(json.dumps(properties))


    logging.debug('  Uploading to bucket')
    bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(bucket_path)}')
    try:
        blob.upload_from_filename(upload_path, timeout=120)
        os.path.isfile(upload_path)
    except Exception as e:
        logging.warning(f'{e}')
        # return f'  Exception uploading file to bucket\n'
        return False


    # # CGM - Upload code from ERA5 Land ingest,
    # #   Not sure if setting the chunk_size parameter is needed/helpful
    # logging.debug('  Uploading geotiff to bucket')
    # bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    # blob = bucket.blob(
    #     bucket_path.replace(f'gs://{BUCKET_NAME}/', ''),
    #     chunk_size=5 * 1024 * 1024,
    # )
    # try:
    #     blob.upload_from_filename(upload_path, timeout=120)
    # except Exception as e:
    #     logging.info(f'{e}')
    #     return f'{tgt_date} - exception uploading file to bucket\n'


    # # CGM - This is not needed if writing to the openet_assets bucket
    # # CGM - Code for registering COGs instead of ingesting as native assets
    # logging.debug('  Registering COG')
    # request = {
    #     'type': 'IMAGE',
    #     'gcs_location': {'uris': [bucket_path]},
    #     'properties': properties,
    #     'startTime': tgt_dt.isoformat() + 'Z',
    #     # 'startTime': tgt_dt.isoformat() + '.000000000Z',
    # }
    # # try:
    # ee.data.createAsset(request, asset_id)
    # # except:
    # #     raise Exception('Unhandled exception registering COG')


    # # CGM - Code for ingesting the image as a native asset
    # logging.info('  Ingesting into Earth Engine')
    # logging.debug(f'  {asset_id}')
    # task_id = ee.data.newTaskId()[0]
    # logging.debug(f'  {task_id}')
    # params = {
    #     'name': asset_id,
    #     'bands': [
    #         {'id': v, 'tilesetId': 'image', 'tilesetBandIndex': i}
    #         for i, v in enumerate(variables)
    #     ],
    #     'tilesets': [{
    #         'id': 'image',
    #         'crs': EE_WKT,
    #         'sources': [{
    #             'uris': [bucket_path],
    #             'affine_transform': {
    #                 'scale_x': TRANSFORM[0],
    #                 'shear_x': TRANSFORM[1],
    #                 'translate_x': TRANSFORM[2],
    #                 'shear_y': TRANSFORM[3],
    #                 'scale_y': TRANSFORM[4],
    #                 'translate_y': TRANSFORM[5],
    #               },
    #         }],
    #     }],
    #     'properties': {
    #         'date': tgt_dt.strftime('%Y-%m-%d'),
    #         'date_ingested': datetime.today().strftime('%Y-%m-%d'),
    #         # 'doy': int(tgt_dt.strftime('%j')),
    #         'source': NC_URL,
    #     },
    #     'startTime': tgt_dt.strftime('%Y-%m-%dT%H:00:00') + '.000000000Z',
    #     'pyramidingPolicy': 'MEAN',
    #     # 'missingData': {'values': [nodata_value]},
    # }
    # ee.data.startIngestion(task_id, params, allow_overwrite=True)

    # logging.info('  Removing from bucket')
    # if blob and blob.exists():
    #     blob.delete()

    # if 'FUNCTION_REGION' in os.environ and os.path.isdir(date_ws):
    #     shutil.rmtree(date_ws)


def datetime_range(start_dt, end_dt, hours=1, skip_leap_days=False):
    """Generate hourly datetimes within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date (exclusive).
    hours : int, optional
        Step size in hours (the default is 1).
    skip_leap_days : bool, optional
        If True, skip leap days while incrementing (the default is True).

    Yields
    ------
    datetime

    """
    import copy
    curr_dt = copy.copy(start_dt)
    while curr_dt < end_dt:
        if not skip_leap_days or curr_dt.month != 2 or curr_dt.day != 29:
            yield curr_dt
        curr_dt += timedelta(hours=hours)


def millis(input_dt):
    """Convert datetime to milliseconds since epoch"""
    return int(input_dt.replace(tzinfo=timezone.utc).timestamp()) * 1000
    # return int(calendar.timegm(end_dt.timetuple())) * 1000


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
        msg = f"Not a valid date: '{input_date}'."
        raise argparse.ArgumentTypeError(msg)


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Build CONUS404 daily assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--workspace', metavar='PATH',
        default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
    # parser.add_argument(
    #     '--download', default=False, action='store_true',
    #     help='Download component geotiffs')
    # parser.add_argument(
    #     '--upload', default=False, action='store_true',
    #     help='Build and upload  composite geotiff to bucket')
    parser.add_argument(
        '--start', type=arg_valid_date, metavar='YYYY-MM-DD',
        help='Start date')
    parser.add_argument(
        '--end', type=arg_valid_date, metavar='YYYY-MM-DD',
        help='End date (exclusive)')
    parser.add_argument(
        '-v', '--variables', nargs='+', metavar='VAR', default=VARIABLES,
        choices=VARIABLES, help='CONUS404 variables')
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

    main(
        start_dt=args.start,
        end_dt=args.end,
        variables=args.variables,
        workspace=args.workspace,
        # download_flag=args.download,
        # upload_flag=args.upload,
        overwrite_flag=args.overwrite,
        reverse_flag=args.reverse,
    )
