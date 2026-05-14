import argparse
from datetime import datetime, timedelta, timezone
import math
import os
import pprint
import shutil
import time

import ee
from google.cloud import storage
import numpy as np
import rasterio
import rasterio.warp
import refet
import requests

ASSET_COLL_ID = 'projects/openet/assets/meteorology/urma/hawaii/hourly'
ASSET_DT_FMT = '%Y%m%d%H'
BUCKET_NAME = 'openet'
BUCKET_FOLDER = 'urma/hawaii/hourly'
PROJECT_NAME = 'openet'
SOURCE_URL = 'https://noaa-urma-pds.s3.amazonaws.com'
# SOURCE_URL = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/urma/prod'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)
TODAY_DT = datetime.now(timezone.utc)
VARIABLES = [
    'TMP',
    'DPT',
    'SPFH',
    'PRES',
    'WDIR',
    'WIND',
    'TCDC',
    'SRAD_TCDC',
    'SRAD_GOES',
    'SRAD_ERA5LAND',
    'ETO_TCDC',
    'ETR_TCDC',
    'ETO_GOES',
    'ETR_GOES',
    'ETO_ERA5LAND',
    'ETR_ERA5LAND',
    # 'ETO',
    # 'ETR',
    # # CGM - Remove these bands at some point
    # 'UGRD',
    # 'VGRD',
    # 'HGT',
    # 'GUST',
    # 'VIS',
    # 'CEIL',
    # 'HTSGW',
]

if 'FUNCTION_REGION' in os.environ:
    # Logging is not working correctly in cloud functions for Python 3.8+
    # Following workflow suggested in this issue:
    # https://issuetracker.google.com/issues/124403972
    import google.cloud.logging
    log_client = google.cloud.logging.Client(project='openet')
    log_client.setup_logging(log_level=20)
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
else:
    import logging
    # logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.getLogger('earthengine-api').setLevel(logging.INFO)
    logging.getLogger('googleapiclient').setLevel(logging.ERROR)
    logging.getLogger('rasterio').setLevel(logging.INFO)
    logging.getLogger('requests').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)

if 'FUNCTION_REGION' in os.environ:
    SCOPES = [
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/earthengine',
    ]
    credentials, project_id = google.auth.default(default_scopes=SCOPES)
    ee.Initialize(credentials, project=project_id)
# else:
#     ee.Initialize()


def urma_hawaii_hourly_ingest(
        tgt_dt,
        workspace='/tmp',
        era5land_workspace=None,
        goes_workspace=None,
        overwrite_flag=False,
        cleanup_flag=False,
):
    """Ingest hourly URMA Hawaii image for a single hourly timestep

    Parameters
    ----------
    tgt_dt : datetime
    workspace : str
    overwrite_flag : bool, optional
        If True, overwrite existing assets (the default is False).

    Returns
    -------
    str : response string

    """
    logging.info(f'Ingest URMA Hawaii hourly image - {tgt_dt.strftime("%Y-%m-%d %H:00")}')

    tgt_date = tgt_dt.strftime(ASSET_DT_FMT)
    tgt_doy = int(tgt_dt.strftime('%j'))
    tgt_hour = float(tgt_dt.strftime('%H'))

    # Store image by year and by date string
    date_ws = os.path.join(workspace, tgt_dt.strftime('%Y'), tgt_dt.strftime('%Y%m%d'))

    grb_fmt = 'hiurma.t{hour:02d}z.2dvaranl_ndfd.grb2'
    grb_file = grb_fmt.format(hour=tgt_dt.hour)
    grb_url = f'{SOURCE_URL}/hiurma.{tgt_dt.strftime("%Y%m%d")}/{grb_file}'
    grb_path = os.path.join(date_ws, grb_file)
    logging.debug(f'  {grb_url}')
    logging.debug(f'  {grb_path}')

    tif_name = f'{tgt_dt.strftime(ASSET_DT_FMT)}.tif'
    local_path = os.path.join(date_ws, f'{tif_name}')
    bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{tif_name}'
    asset_id = f'{ASSET_COLL_ID}/{tgt_dt.strftime(ASSET_DT_FMT)}'
    logging.debug(f'  {local_path}')
    logging.debug(f'  {bucket_path}')
    logging.debug(f'  {asset_id}')

    if not overwrite_flag and ee.data.getInfo(asset_id):
        return f'{tgt_date} - Asset already exists'

    # # Always overwrite temporary files if the asset doesn't exist
    # if os.path.isdir(date_ws):
    #     shutil.rmtree(date_ws)
    if not os.path.isdir(date_ws):
        os.makedirs(date_ws)

    # RasterIO can't read from the bucket directly when deployed as a function
    # So for now, download the ancillary images for each timestep when deployed
    # This is really inefficient and should probably not be used for bulk ingests
    if 'FUNCTION_REGION' in os.environ:
        land_mask_url = 'https://storage.googleapis.com/openet/urma/hawaii/ancillary/land_mask.tif'
        elevation_url = 'https://storage.googleapis.com/openet/urma/hawaii/ancillary/elevation.tif'
        latitude_url = 'https://storage.googleapis.com/openet/urma/hawaii/ancillary/latitude.tif'
        longitude_url = 'https://storage.googleapis.com/openet/urma/hawaii/ancillary/longitde.tif'
        land_mask_path = os.path.join(date_ws, 'land_mask.tif')
        elevation_path = os.path.join(date_ws, 'elevation.tif')
        latitude_path = os.path.join(date_ws, 'latitude.tif')
        longitude_path = os.path.join(date_ws, 'longitude.tif')
        url_download(land_mask_url, land_mask_path)
        url_download(elevation_url, elevation_path)
        url_download(latitude_url, latitude_path)
        url_download(longitude_url, longitude_path)
    else:
        land_mask_path = os.path.join('..', 'urma_ancillary', 'hawaii', 'hi_land_mask.tif')
        elevation_path = os.path.join('..', 'urma_ancillary', 'hawaii', 'hi_elevation.tif')
        latitude_path = os.path.join('..', 'urma_ancillary', 'hawaii', 'hi_latitude.tif')
        longitude_path = os.path.join('..', 'urma_ancillary', 'hawaii', 'hi_longitude.tif')

    logging.debug('\nDownloading grib files')
    #if overwrite_flag or not os.path.isfile(grb_path):
    if not os.path.isfile(grb_path):
        url_download(grb_url, grb_path)
    if not os.path.isfile(grb_path):
        return f'{tgt_date} - GRB file does not exist'

    logging.debug('Opening grib file')
    try:
        grb_ds = rasterio.open(grb_path, 'r')
    except Exception:
        # os.path.remove(grb_path)
        return f'{tgt_date} - GRB file could not be read'

    # Hardcoding the shape and projection parameters for now
    # The transform is being manually shifted 6 cells up/north for better alignment
    # This adjustment was chosen based on visual inspection of the assets in GEE
    width, height = 321, 225
    gee_transform = [2500, 0, -16879375, 0, -2500, 2481825 - (6 * 2500)]
    # grb_transform = [2500, 0, -16879374.0603126622736454, 0, -2500, 2481825.9654569458216429]
    wkt = (
        'PROJCS["unnamed", '
        '  GEOGCS["Coordinate System imported from GRIB file", \n'
        '    DATUM["unnamed", SPHEROID["Sphere", 6371200, 0]], \n'
        '    PRIMEM["Greenwich", 0], \n'
        '    UNIT["degree", 0.0174532925199433, AUTHORITY["EPSG", "9122"]]], \n'
        '  PROJECTION["Mercator_2SP"], \n'
        '  PARAMETER["standard_parallel_1", 20], \n'
        '  PARAMETER["central_meridian", 0], \n'
        '  PARAMETER["false_easting", 0], \n'
        '  PARAMETER["false_northing", 0], \n'
        '  UNIT["Metre", 1], \n'
        '  AXIS["Easting", EAST], \n'
        '  AXIS["Northing", NORTH]]'
    )
    crs = rasterio.crs.CRS.from_wkt(wkt)
    # wkt_str = (
    #     'PROJCS[\"unnamed\",GEOGCS[\"Coordinate System imported from GRIB file\",'
    #     'DATUM[\"unnamed\",SPHEROID[\"Sphere\",6371200,0]],PRIMEM[\"Greenwich\",0],'
    #     'UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]]],'
    #     'PROJECTION[\"Mercator_2SP\"],PARAMETER[\"standard_parallel_1\",20],'
    #     'PARAMETER[\"central_meridian\",0],PARAMETER[\"false_easting\",0],'
    #     'PARAMETER[\"false_northing\",0],UNIT[\"Metre\",1],'
    #     'AXIS[\"Easting\",EAST],AXIS[\"Northing\",NORTH]]'
    # )
    # extent = [
    #     transform[2], transform[5] + height * transform[4],
    #     transform[2] + width * transform[0], transform[5]
    # ]
    # logging.debug(f'  Projection: {crs}')
    # logging.debug(f'  Width:      {width}')
    # logging.debug(f'  Height:     {height}')
    # logging.debug(f'  Transform:  {transform}')
    # logging.debug(f'  Extent:     {extent}')

    # Read in the ancillary arrays
    try:
        with rasterio.open(land_mask_path) as src:
            land_mask_array = src.read(1).astype(np.uint8)
        with rasterio.open(elevation_path) as src:
            elevation_array = src.read(1)
        with rasterio.open(latitude_path) as src:
            latitude_array = src.read(1)
        with rasterio.open(longitude_path) as src:
            longitude_array = src.read(1)
    except Exception as e:
        logging.exception(f'Ancillary arrays could not be read\n{e}')
        return f'{tgt_date} - Ancillary arrays could not be read'

    # logging.debug('Reading hourly arrays')
    hourly_arrays = {}
    var_units = {}
    for band in range(len(grb_ds.indexes)):
        band_tags = grb_ds.tags(band+1)
        band_name = band_tags['GRIB_ELEMENT']
        var_units[band_name] = band_tags['GRIB_UNIT'][1:-1]
        hourly_arrays[band_name] = grb_ds.read(band+1)
        hourly_arrays[band_name][land_mask_array == 0] = np.nan

    # Compute solar radiation from cloud cover
    # Should the midpoint time be before or after the target time?
    ra = refet.calcs._ra_hourly(
        lat=latitude_array * (math.pi / 180),
        lon=longitude_array * (math.pi / 180),
        doy=tgt_doy,
        time_mid=tgt_hour + 0.5,
        method='asce',
    )
    # Convert from MJ m-2 h-1 to W m-2
    ra = ra * 1000000 / 3600  # Convert to W/m2
    nn = -0.0083 * hourly_arrays['TCDC'] + 0.9659
    hourly_arrays['SRAD_TCDC'] = ra * (0.25 + nn * 0.5)

    # Read GOES DSR if available
    # The GOES solar images are being built as the "instantaneous" value at the image time
    #   by averaging the 10-minute
    goes_srad_path = os.path.join(
        goes_workspace, tgt_dt.strftime('%Y'), tgt_dt.strftime('%Y%m%d'),
        f'{tgt_dt.strftime(ASSET_DT_FMT)}.tif'
    )
    if goes_workspace and os.path.isfile(goes_srad_path):
        with rasterio.open(goes_srad_path) as src:
            hourly_arrays['SRAD_GOES'] = src.read(1).astype(np.float32)
            hourly_arrays['SRAD_GOES'][land_mask_array == 0] = np.nan
    else:
        hourly_arrays['SRAD_GOES'] = np.full(land_mask_array.shape, np.nan)

    # Read ERA5-Land DSR if available
    # The ERA5-Land solar images are being built as the "instantaneous" value
    era5land_srad_path = os.path.join(
        era5land_workspace, tgt_dt.strftime('%Y'), tgt_dt.strftime('%Y%m%d'),
        f'{tgt_dt.strftime(ASSET_DT_FMT)}.tif'
    )
    if era5land_workspace and os.path.isfile(era5land_srad_path):
        with rasterio.open(era5land_srad_path) as src:
            hourly_arrays['SRAD_ERA5LAND'] = src.read(1).astype(np.float32)
            hourly_arrays['SRAD_ERA5LAND'][land_mask_array == 0] = np.nan
    else:
        hourly_arrays['SRAD_ERA5LAND'] = np.full(land_mask_array.shape, np.nan)

    # # Read ERA5-Land DSR if available
    # # The ERA5-Land solar images are the accumulation over the previous hour,
    # #   so to get the value corresponding to the start of the target hour,
    # #   take the average of the target hour and the next hour
    # next_dt = tgt_dt + timedelta(hours=1)
    # era5land_srad_tgt_path = os.path.join(
    #     era5land_workspace, tgt_dt.strftime('%Y'), tgt_dt.strftime('%Y%m%d'),
    #     f'{tgt_dt.strftime(ASSET_DT_FMT)}.tif'
    # )
    # era5land_srad_next_path = os.path.join(
    #     era5land_workspace, next_dt.strftime('%Y'), next_dt.strftime('%Y%m%d'),
    #     f'{next_dt.strftime(ASSET_DT_FMT)}.tif'
    # )
    # if (era5land_workspace
    #         and os.path.isfile(era5land_srad_tgt_path)
    #         and os.path.isfile(era5land_srad_next_path)):
    #     with rasterio.open(era5land_srad_tgt_path) as src:
    #         tgt_array = src.read(1).astype(np.float32)
    #         tgt_array[land_mask_array == 0] = np.nan
    #     with rasterio.open(era5land_srad_next_path) as src:
    #         next_array = src.read(1).astype(np.float32)
    #         next_array[land_mask_array == 0] = np.nan
    #     hourly_arrays['SRAD_ERA5LAND'] = (tgt_array + next_array) * 0.5
    # else:
    #     hourly_arrays['SRAD_ERA5LAND'] = np.full(land_mask_array.shape, np.nan)


    # Compute reference ET using the cloud cover derived solar
    # TODO: Check if this should use "refet" method
    #   so that Rso calculation considers vapor pressure
    refet_obj = refet.Hourly(
        tmean=hourly_arrays['TMP'],
        tdew=hourly_arrays['DPT'],
        rs=hourly_arrays['SRAD_TCDC'],
        uz=hourly_arrays['WIND'],
        zw=10,
        elev=elevation_array,
        lat=latitude_array,
        lon=longitude_array,
        doy=tgt_doy,
        time=float(tgt_dt.strftime('%H')),
        method='asce',
        input_units={'tmean': 'C', 'tdew': 'C', 'rs': 'w m-2', 'uz': 'm s-1', 'lat': 'deg'},
    )
    hourly_arrays['ETO_TCDC'] = refet_obj.eto()
    hourly_arrays['ETR_TCDC'] = refet_obj.etr()

    # Compute reference ET using GOES solar
    # TODO: Check if this should use "refet" method
    #   so that Rso calculation considers vapor pressure
    refet_obj = refet.Hourly(
        tmean=hourly_arrays['TMP'],
        tdew=hourly_arrays['DPT'],
        rs=hourly_arrays['SRAD_GOES'],
        uz=hourly_arrays['WIND'],
        zw=10,
        elev=elevation_array,
        lat=latitude_array,
        lon=longitude_array,
        doy=tgt_doy,
        time=float(tgt_dt.strftime('%H')),
        method='asce',
        input_units={'tmean': 'C', 'tdew': 'C', 'rs': 'w m-2', 'uz': 'm s-1', 'lat': 'deg'},
    )
    hourly_arrays['ETO_GOES'] = refet_obj.eto()
    hourly_arrays['ETR_GOES'] = refet_obj.etr()

    # Compute reference ET using ERA5-Land
    # TODO: Check if this should use "refet" method
    #   so that Rso calculation considers vapor pressure
    refet_obj = refet.Hourly(
        tmean=hourly_arrays['TMP'],
        tdew=hourly_arrays['DPT'],
        rs=hourly_arrays['SRAD_ERA5LAND'],
        uz=hourly_arrays['WIND'],
        zw=10,
        elev=elevation_array,
        lat=latitude_array,
        lon=longitude_array,
        doy=tgt_doy,
        time=float(tgt_dt.strftime('%H')),
        method='asce',
        input_units={'tmean': 'C', 'tdew': 'C', 'rs': 'w m-2', 'uz': 'm s-1', 'lat': 'deg'},
    )
    hourly_arrays['ETO_ERA5LAND'] = refet_obj.eto()
    hourly_arrays['ETR_ERA5LAND'] = refet_obj.etr()

    var_units['SRAD_TCDC'] = 'W/m2'
    var_units['SRAD_GOES'] = 'W/m2'
    var_units['SRAD_ERA5LAND'] = 'W/m2'
    var_units['ETO_TCDC'] = 'mm'
    var_units['ETO_GOES'] = 'mm'
    var_units['ETO_ERA5LAND'] = 'mm'
    var_units['ETR_TCDC'] = 'mm'
    var_units['ETR_GOES'] = 'mm'
    var_units['ETR_ERA5LAND'] = 'mm'

    # Only build the composite if all the input images are available
    input_vars = set(hourly_arrays.keys())
    if not set(VARIABLES).issubset(input_vars):
        return f'{tgt_date} - Missing input variables for composite\n'\
               f'  {", ".join(list(set(VARIABLES) - input_vars))}'

    # logging.debug('\nBuilding output GeoTIFF')
    output_ds = rasterio.open(
        local_path,
        'w',
        driver='GTiff',
        nodata=-9999,
        count=len(VARIABLES),
        dtype=rasterio.float64,
        height=height,
        width=width,
        crs=crs,
        transform=gee_transform,
        compress='deflate',
        tiled=True,
        blockxsize=512,
        blockysize=512,
    )

    # logging.debug('\nWriting arrays to output GeoTIFF')
    for band_i, variable in enumerate(VARIABLES):
        # logging.debug(f'  {variable}')
        output_ds.set_band_description(band_i + 1, variable)
        data_array = hourly_arrays[variable].astype(np.float64)
        data_array[np.isnan(data_array)] = -9999
        output_ds.write(data_array, band_i + 1)
        del data_array
    # output_ds.close()
    del output_ds

    # logging.debug('\nBuilding overviews')
    dst = rasterio.open(local_path, 'r+')
    dst.build_overviews([2, 4], rasterio.warp.Resampling.average)
    dst.update_tags(ns='rio_overview', resampling='average')
    dst.close()

    # logging.debug('\nUploading to bucket')
    bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(bucket_path)}')
    blob.upload_from_filename(local_path)

    # logging.debug('\nIngesting into Earth Engine')
    task_id = ee.data.newTaskId()[0]
    logging.debug(f'  {task_id}')

    properties = {
        'date_ingested': f'{TODAY_DT.strftime("%Y-%m-%d")}',
        'source': SOURCE_URL.replace('https://', '').replace('http://', ''),
    }
    for v in VARIABLES:
        if (v in var_units.keys()) and var_units[v]:
            properties[f'units_{v}'] = var_units[v]

    params = {
        'name': asset_id,
        'bands': [
            {'id': v, 'tilesetId': 'image', 'tilesetBandIndex': i}
            for i, v in enumerate(VARIABLES)
        ],
        'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
        # 'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}], 'crs': crs_str}],
        'properties': properties,
        'startTime': tgt_dt.isoformat() + '.000000000Z',
        # 'pyramiding_policy': 'MEAN',
        # 'missingData': {'values': [nodata_value]},
    }

    # TODO: Wrap in a try/except loop
    ee.data.startIngestion(task_id, params, allow_overwrite=True)

    # Always remove local TIF
    if cleanup_flag:
        os.remove(local_path)

    # Only remove GRIB file when run from a cloud function
    if 'FUNCTION_REGION' in os.environ:
        os.remove(grb_path)

    # logging.info(f'  {export_name} - {task.id}')
    # logging.info(f'  {tgt_date} - {asset_id}')
    return f'{tgt_date} - {asset_id}\n'


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


def date_range(start_dt, end_dt, days=1, skip_leap_days=False):
    """Generate dates within a range (exclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date (exclusive).
    days : int, optional
        Step size (the default is 1).
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
        curr_dt += timedelta(days=days)


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
        return datetime.strptime(input_date, '%Y-%m-%d')
    except ValueError:
        raise argparse.ArgumentTypeError(f'Not a valid date: "{input_date}"')


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Build URMA Hawaii hourly assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--workspace', metavar='PATH',
        default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
    parser.add_argument(
        '--project', type=str, required=True, help='Earth Engine Project ID')
    parser.add_argument(
        '--start', type=arg_valid_date, metavar='YYYY-MM-DD',
        help='Start date')
    parser.add_argument(
        '--end', type=arg_valid_date, metavar='YYYY-MM-DD',
        help='End date (exclusive)')
    parser.add_argument(
        '--era5land', metavar='PATH',
        default='../era5land_hawaii_hourly',
        # default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the path to the Hawaii ERA5-Land solar data')
    parser.add_argument(
        '--goes', metavar='PATH',
        default='../goes_hawaii_hourly',
        # default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the path to the Hawaii GOES DSR data')
    parser.add_argument(
        '--delay', default=1, type=float,
        help='Delay (in seconds) between each export tasks')
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
    if args.era5land and os.path.isdir(os.path.abspath(args.era5land)):
        args.era5land = os.path.abspath(args.era5land)
    if args.goes and os.path.isdir(os.path.abspath(args.goes)):
        args.goes = os.path.abspath(args.goes)

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    logging.info('\nInitializing Earth Engine using project ID')
    ee.Initialize(project=args.project)

    # # Build the image collection if it doesn't exist
    # logging.debug(f'\nImage Collection: {ASSET_COLL_ID}')
    # if not ee.data.getInfo(ASSET_COLL_ID.rsplit('/', 1)[0]):
    #     logging.info(f'\nImage collection folder does not exist and will be built'
    #                  f'\n  {ASSET_COLL_ID.rsplit("/", 1)[0]}')
    #     input('Press ENTER to continue')
    #     ee.data.createAsset({'type': 'FOLDER'}, ASSET_COLL_ID.rsplit('/', 1)[0])
    # if not ee.data.getInfo(ASSET_COLL_ID):
    #     logging.info(f'\nImage collection does not exist and will be built'
    #                  f'\n  {ASSET_COLL_ID}')
    #     input('Press ENTER to continue')
    #     ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, ASSET_COLL_ID)

    for tgt_dt in sorted(date_range(args.start, args.end), reverse=args.reverse):
        print(tgt_dt)
        for hour in sorted(range(0, 24), reverse=args.reverse):
            response = urma_hawaii_hourly_ingest(
                tgt_dt=tgt_dt + timedelta(hours=hour),
                workspace=args.workspace,
                overwrite_flag=args.overwrite,
                era5land_workspace=args.era5land,
                goes_workspace=args.goes,
            )
            logging.info(f'  {response}')

            if args.delay:
                time.sleep(args.delay)

    # from unittest.mock import Mock
    # data = {}
    # if args.start and args.end:
    #     data['start'] = args.start.strftime('%Y-%m-%d')
    #     data['end'] = args.end.strftime('%Y-%m-%d')
    # # Convert booleans to string to mimic input when deployed
    # if args.overwrite:
    #     data['overwrite'] = str(args.overwrite)
    #
    # req = Mock(get_json=Mock(return_value=data), args=data)
    # response = cron_scheduler(req)
    # print(response.data)
