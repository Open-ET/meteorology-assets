import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import pprint
import shutil

import boto3
import ee
from google.cloud import storage
import numpy as np
import numpy.ma as ma
import openet.core.utils as utils
from pyproj import CRS
import rasterio
from rasterio.transform import from_bounds
from rasterio.warp import reproject, Resampling
import s3fs
import xarray as xr


ASSET_COLL_ID = 'projects/openet/assets/meteorology/goes_dsr/hawaii/hourly'
AWS_BUCKET_NAME = 'noaa-goes18'
AWS_BUCKET_FOLDER = 'ABI-L2-DSRF'
GCP_BUCKET_NAME = 'openet'
GCP_BUCKET_FOLDER = 'goes_dsr/hourly'
PROJECT_NAME = 'openet'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)

TODAY_DT = datetime.now(timezone.utc)

# Is it safe to assume Hawaii will stay in roughly the same location in the image?
SRC_COL_MIN, SRC_COL_MAX = 1480, 1890
SRC_ROW_MIN, SRC_ROW_MAX = 1500, 1770

# Hardcoding the shape and projection parameters for Hawaii
# The transform is being manually shifted 6 cells up/north for better alignment
# This adjustment was chosen based on visual inspection of the assets in GEE
DST_WIDTH, DST_HEIGHT = 321, 225
# Adjusted transform for positioning in GEE
GEE_TRANSFORM = [2500, 0, -16879375, 0, -2500, 2481825 - (6 * 2500)]
# GRB transform that works for QGIS but not for GEE
GRB_TRANSFORM = [2500, 0, -16879374.0603126622736454, 0, -2500, 2481825.9654569458216429]
DST_WKT = (
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

# if 'FUNCTION_REGION' in os.environ:
#     # Logging is not working correctly in cloud functions for Python 3.8+
#     # Following workflow suggested in this issue:
#     # https://issuetracker.google.com/issues/124403972
#     import google.cloud.logging
#     log_client = google.cloud.logging.Client(project='openet')
#     log_client.setup_logging(log_level=20)
#     import logging
#     logging.basicConfig(level=logging.INFO)
#     logger = logging.getLogger(__name__)
#     logger.setLevel(logging.INFO)
# else:
#     import logging
#     # logging.basicConfig(level=logging.INFO, format='%(message)s')
# logging.basicConfig(level=logging.INFO, format='%(message)s')
# logging.basicConfig(level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('h5py').setLevel(logging.INFO)
logging.getLogger('pyproj').setLevel(logging.INFO)
logging.getLogger('rasterio').setLevel(logging.INFO)
#logging.getLogger('s3fs').setLevel(logging.INFO)
logging.getLogger('s3transfer').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

# if 'FUNCTION_REGION' in os.environ:
#     SCOPES = [
#         'https://www.googleapis.com/auth/cloud-platform',
#         'https://www.googleapis.com/auth/earthengine',
#     ]
#     credentials, project_id = google.auth.default(default_scopes=SCOPES)
#     ee.Initialize(credentials, project=project_id)
# else:
#     ee.Initialize()


def asset_ingest(tgt_dt, workspace='/tmp', overwrite_flag=False, ingest_flag=True):
    """Build and ingest GOES solar radiation hourly assets into Earth Engine

    Parameters
    ----------
    tgt_dt : datetime
    workspace : str
    overwrite_flag : bool, optional
        If True, overwrite existing assets.
    ingest_flag : bool, optional
        If True, ingest hourly images into GEE.

    """
    tgt_date = tgt_dt.strftime('%Y-%m-%dT%H00')
    logging.info(f'{tgt_date} ({tgt_dt.strftime("%j")})')

    asset_id = f'{ASSET_COLL_ID}/{tgt_dt.strftime("%Y%m%d%H")}'
    if not overwrite_flag and ee.data.getInfo(asset_id):
        logging.info(f'  Asset already exists, skipping date')
        return f'{tgt_date} - asset already exists - skipping'

    # Save the hourly files in the DOY folder
    year_ws = os.path.join(workspace, tgt_dt.strftime('%Y'))
    date_ws = os.path.join(year_ws, tgt_dt.strftime('%Y%m%d'))
    tif_file_name = f'{tgt_dt.strftime("%Y%m%d%H")}.tif'
    tif_file_path = os.path.join(date_ws, tif_file_name)

    # DEADBEEF
    # # Rename the DOY folders to a date string
    # doy_ws = os.path.join(year_ws, tgt_dt.strftime('%j'))
    # if os.path.isdir(doy_ws):
    #     if not os.path.isdir(date_ws):
    #         os.makedirs(date_ws)
    #     shutil.copytree(doy_ws, date_ws, dirs_exist_ok=True)
    #     shutil.rmtree(doy_ws)

    hour_ws = os.path.join(date_ws, tgt_dt.strftime('%H'))
    if not os.path.isdir(hour_ws):
        os.makedirs(hour_ws)

    # TODO: Read from the remote netcdfs directly instead of downloading
    logging.debug(f'  Getting list of available netcdf files')
    nc_file_names = []
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(
        Bucket=AWS_BUCKET_NAME,
        Prefix=f'{AWS_BUCKET_FOLDER}/{tgt_dt.strftime("%Y/%j/%H")}'
    )
    for content in response.get('Contents', []):
        nc_file_url = content['Key']
        nc_file_name = nc_file_url.split('/')[-1]
        if not nc_file_name.endswith('.nc'):
            continue
        nc_file_names.append(nc_file_name)
    nc_file_names = sorted(nc_file_names)
    if not nc_file_names:
        logging.info(f'  No netcdf files in hour, skipping date')
        return f'{tgt_date} - no netcdf files in hour - skipping date'

    # Build the 10-minute subset geotiff for each netcdf
    for nc_file_name in nc_file_names:
        nc_file_path = os.path.join(hour_ws, nc_file_name)

        dsr_file_path = os.path.join(hour_ws, nc_file_name.replace('.nc', '_dsr.tif'))
        dqf_file_path = os.path.join(hour_ws, nc_file_name.replace('.nc', '_dqf.tif'))

        # Don't skip the scene if the netcdf files are present
        if os.path.isfile(dsr_file_path) and not os.path.isfile(nc_file_path):
            logging.debug(f'  {nc_file_name} - dsr file already exists, skipping')
            continue

        # if overwrite_flag or not os.path.isfile(nc_file_path):
        if not os.path.isfile(nc_file_path):
            logging.info(f'  {nc_file_name} - downloading')
            nc_file_url = f'{AWS_BUCKET_FOLDER}/{tgt_dt.strftime("%Y/%j/%H")}/{nc_file_name}'
            s3.download_file(AWS_BUCKET_NAME, nc_file_url, nc_file_path)

        # CGM - Tried testing out reading directly from the bucket instead of downloading
        #   but couldn't get it to work
        # try:
        #     fs = s3fs.S3FileSystem(anon=True)
        #     nc_file_url = f's3://{AWS_BUCKET_NAME}/{AWS_BUCKET_FOLDER}/{tgt_dt.strftime("%Y/%j/%H")}/{nc_file_name}'
        #     with fs.open(nc_file_url, mode='rb') as nc_f:
        #         src_ds = xr.open_dataset(nc_f, engine="h5netcdf")
        # except Exception as e:
        #     logging.warning(f'  {nc_file_name} error opening file - skipping')
        #     logging.warning(f'  Exception: {e}')
        #     return f'{tgt_date} - {nc_file_name} could not be opened - skipping'

        try:
            src_ds = xr.open_dataset(nc_file_path, engine="h5netcdf")
        except Exception as e:
            logging.warning(f'  {nc_file_name} error opening file - skipping')
            logging.warning(f'  Exception: {e}')
            continue

        src_height, src_width = src_ds['DSR'].shape

        try:
            src_crs = CRS.from_cf(src_ds.goes_imager_projection.attrs)
            proj_info = src_ds["goes_imager_projection"]
        except Exception as e:
            logging.warning(f'  missing projection information - skipping')
            logging.warning(f'  Exception: {e}')
            pprint.pprint(dir(src_ds))
            print(src_ds.goes_lat_lon_projection)
            input('ENTER')
            continue

        h = proj_info.perspective_point_height  # satellite height above ellipsoid (m)

        # --- Compute extent in scan-angle space ---
        x = src_ds["x"][:].to_numpy() * h  # radians → meters (scan angle * height)
        y = src_ds["y"][:].to_numpy() * h
        x_min, x_max = float(x.min()), float(x.max())
        y_min, y_max = float(y.min()), float(y.max())

        # Not sure if from_bounds() was using pixel corner or centers,
        #   so computing transform manually using the average cellsize
        #   between the start and end x/y
        # This gets values that are really close to what is shown in QGIS
        cs_x = (x_max - x_min) / (src_width - 1)
        cs_y = (y_max - y_min) / (src_height - 1)
        src_transform = (cs_x, 0, x_min - cs_x / 2, 0, -cs_y, y_max + cs_y / 2)
        # src_transform = from_bounds(x_min, y_min, x_max, y_max, src_width, src_height)

        clip_width = SRC_COL_MAX - SRC_COL_MIN
        clip_height = SRC_ROW_MAX - SRC_ROW_MIN
        clip_transform = (
            src_transform[0], 0, src_transform[2] + SRC_COL_MIN * src_transform[0],
            0, src_transform[4], src_transform[5] + SRC_ROW_MIN * src_transform[4]
        )

        # Slice the arrays to the study area
        try:
            dsr_array = src_ds['DSR'][SRC_ROW_MIN:SRC_ROW_MAX, SRC_COL_MIN:SRC_COL_MAX].to_numpy()
            dqf_array = src_ds['DQF'][SRC_ROW_MIN:SRC_ROW_MAX, SRC_COL_MIN:SRC_COL_MAX].to_numpy()
        except Exception as e:
            logging.warning(f'{e}')
            continue

        with rasterio.open(
            dsr_file_path,
            "w",
            driver="GTiff",
            height=clip_height,
            width=clip_width,
            count=1,
            dtype=np.float32,
            crs=src_crs,
            transform=clip_transform,
            nodata=np.nan,
            tiled=True,
            compress='DEFLATE',
            # compress='LZW',
        ) as dst_ds:
            dst_ds.set_band_description(1, 'DSR')
            dst_ds.write(dsr_array, 1)

        with rasterio.open(
            dqf_file_path,
            "w",
            driver="GTiff",
            height=clip_height,
            width=clip_width,
            count=1,
            dtype=np.float32,
            # dtype=np.uint8,
            crs=src_crs,
            transform=clip_transform,
            nodata=np.nan,
            tiled=True,
            compress='DEFLATE',
            # compress='LZW',
        ) as dst_ds:
            dst_ds.set_band_description(1, 'DQF')
            dst_ds.write(dqf_array, 1)

        os.remove(nc_file_path)

    # Compute hourly mean from the 10-minute files
    # TODO: Come up with different approach to account for missing images
    if not os.path.isfile(tif_file_path) or overwrite_flag:
        data_arrays = {}
        mask_arrays = {}

        for nc_file_name in nc_file_names:
            minute = nc_file_name.split('_')[3][10:12]

            dsr_file_path = os.path.join(hour_ws, nc_file_name.replace('.nc', '_dsr.tif'))
            # dqf_file_path = os.path.join(hour_ws, nc_file_name.replace('.nc', '_dqf.tif'))
            if not os.path.isfile(dsr_file_path):
                logging.warning(f'  Missing 10-min source file: {os.path.basename(dsr_file_path)}')
                continue

            # TODO: Switch to computing the reprojected image in memory
            temp_file_path = os.path.join(hour_ws, f'{tgt_dt.strftime("%Y%m%d%H")}{minute}.tif')

            with rasterio.open(dsr_file_path, "r") as src_ds:
                with rasterio.open(
                    temp_file_path,
                    "w",
                    driver="GTiff",
                    height=DST_HEIGHT,
                    width=DST_WIDTH,
                    count=1,
                    dtype=np.float32,
                    crs=rasterio.crs.CRS.from_wkt(DST_WKT),
                    transform=GRB_TRANSFORM,
                    nodata=np.nan,
                    tiled=True,
                    blockxsize=512,
                    blockysize=512,
                    compress='DEFLATE',
                    # compress='LZW',
                ) as dst_ds:
                    reproject(
                        source=rasterio.band(src_ds, 1),
                        destination=rasterio.band(dst_ds, 1),
                        src_transform=src_ds.transform,
                        src_crs=src_ds.crs,
                        dst_transform=GRB_TRANSFORM,
                        dst_crs=rasterio.crs.CRS.from_wkt(DST_WKT),
                        resampling=Resampling.bilinear,
                    )

            if not os.path.isfile(temp_file_path):
                logging.info(f'  Missing 10-min reprojected file: {os.path.basename(temp_file_path)}')
                continue

            with rasterio.open(temp_file_path, 'r') as src_ds:
                data_array = src_ds.read(1).astype(np.float32)
                data_array[np.isnan(data_array)] = 0
                data_arrays[minute] = data_array
                mask_arrays[minute] = np.isfinite(data_array).astype(np.uint8)

            os.remove(temp_file_path)

        # Compute the mean of the 10-minute values
        if nc_file_names and (len(nc_file_names) == len(data_arrays.keys())):
            # TODO: Come up with a better approach for computing the hourly mean
            #   when there are missing 10-minute images
            mean_array = np.nanmean([x for x in data_arrays.values()], axis=0)

            # Save the hourly array
            with rasterio.open(
                tif_file_path,
                "w",
                driver="GTiff",
                height=DST_HEIGHT,
                width=DST_WIDTH,
                count=1,
                dtype=np.float32,
                crs=rasterio.crs.CRS.from_wkt(DST_WKT),
                transform=GEE_TRANSFORM,
                nodata=-9999,
                tiled=True,
                blockxsize=512,
                blockysize=512,
                compress='DEFLATE',
                # compress='LZW',
            ) as dst_ds:
                dst_ds.set_band_description(1, 'DSR')
                dst_ds.write(mean_array, 1)

    # Ingest into GEE
    if os.path.isfile(tif_file_path) and ingest_flag:
        logging.debug('  Uploading to bucket')
        bucket_path = f'gs://{GCP_BUCKET_NAME}/{GCP_BUCKET_FOLDER}/{tif_file_name}'
        bucket = STORAGE_CLIENT.bucket(GCP_BUCKET_NAME)
        blob = bucket.blob(f'{GCP_BUCKET_FOLDER}/{os.path.basename(bucket_path)}')
        blob.upload_from_filename(tif_file_path)

        # Assume the file made it into the bucket
        logging.debug('  Ingesting into Earth Engine')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')

        properties = {
            'date_ingested': f'{TODAY_DT.strftime("%Y-%m-%d")}',
            'source_bucket': f's3://{AWS_BUCKET_NAME}/{AWS_BUCKET_FOLDER}',
            'source_files': ','.join(nc_file_names),
            'units_DSR': 'W m-2',
        }

        params = {
            'name': asset_id,
            'bands': [
                {'id': v, 'tilesetId': 'image', 'tilesetBandIndex': i}
                for i, v in enumerate(['DSR'])
            ],
            'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
            'properties': properties,
            'startTime': tgt_dt.isoformat() + '.000000000Z',
            # 'pyramiding_policy': 'MEAN',
            # 'missingData': {'values': [nodata_value]},
        }

        # TODO: Wrap in a try/except loop
        ee.data.startIngestion(task_id, params, allow_overwrite=True)


    # DEADBEEF
    # # --- Build PROJ CRS string ---
    # lon_origin = proj_info.longitude_of_projection_origin  # e.g. -75.0 for GOES-16
    # semi_major = proj_info.semi_major_axis  # meters
    # semi_minor = proj_info.semi_minor_axis  # meters
    # sweep = proj_info.sweep_angle_axis  # 'x' for GOES
    # crs = CRS.from_proj4(
    #     f"+proj=geos +lon_0={lon_origin} +h={h} "
    #     f"+a={semi_major} +b={semi_minor} "
    #     f"+sweep={sweep} +no_defs"
    # )


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


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest Hawaii GOES DSR hourly assets into Earth Engine',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--project', type=str, required=True, help='Earth Engine Project ID')
    parser.add_argument(
        '--start', required=True, type=utils.arg_valid_date, metavar='YYYY-MM-DD',
        help='Start date')
    parser.add_argument(
        '--end', required=True, type=utils.arg_valid_date, metavar='YYYY-MM-DD',
        help='End date (exclusive)')
    parser.add_argument(
        '--hours', default='0-23', type=str,
        help='Comma separated or range of hours')
    parser.add_argument(
        '--workspace', metavar='PATH',
        default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
    parser.add_argument(
        '--skip_ingest', default=False, action='store_true',
        help='Only download local images and skip ingest')
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

    logging.info('\nInitializing Earth Engine using project ID')
    ee.Initialize(project=args.project)

    # # Build the image collection if it doesn't exist
    # logging.debug('Image Collection: {}'.format(ASSET_COLL_ID))
    # asset_folder = ASSET_COLL_ID.rsplit('/', 1)[0]
    # if not ee.data.getInfo(asset_folder):
    #     logging.info('\nFolder does not exist and will be built'
    #                  '\n  {}'.format(asset_folder))
    #     input('Press ENTER to continue')
    #     ee.data.createAsset({'type': 'FOLDER'}, asset_folder)
    # if not ee.data.getInfo(ASSET_COLL_ID):
    #     logging.info('\nImage collection does not exist and will be built'
    #                  '\n  {}'.format(ASSET_COLL_ID))
    #     input('Press ENTER to continue')
    #     ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, ASSET_COLL_ID)

    for tgt_dt in sorted(date_range(args.start, args.end), reverse=args.reverse):
        for hour in sorted(utils.parse_int_set(args.hours), reverse=args.reverse):
            asset_ingest(
                tgt_dt + timedelta(hours=hour),
                workspace=args.workspace,
                overwrite_flag=args.overwrite,
                ingest_flag=not(args.skip_ingest),
            )
