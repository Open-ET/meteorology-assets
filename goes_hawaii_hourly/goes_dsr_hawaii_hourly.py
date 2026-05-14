import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import pprint

import ee
from google.cloud import storage
import numpy as np
import openet.core.utils as utils
import rasterio
from rasterio.warp import reproject, Resampling


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
logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('h5py').setLevel(logging.INFO)
logging.getLogger('pyproj').setLevel(logging.INFO)
logging.getLogger('rasterio').setLevel(logging.INFO)
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

    # Load the workspace for the target hour
    date_ws = os.path.join(workspace, tgt_dt.strftime('%Y'), tgt_dt.strftime('%Y%m%d'))
    hour_ws = os.path.join(date_ws, tgt_dt.strftime('%H'))

    # Load the workspace for the previous hour
    prev_dt = tgt_dt - timedelta(hours=1)
    date_prev_ws = os.path.join(workspace, prev_dt.strftime('%Y'), prev_dt.strftime('%Y%m%d'))
    hour_prev_ws = os.path.join(date_prev_ws, prev_dt.strftime('%H'))

    tif_file_name = f'{tgt_dt.strftime("%Y%m%d%H")}.tif'
    tif_file_path = os.path.join(date_ws, tif_file_name)

    # Build a list of files that should be included
    try:
        dsr_tgt_paths = [
            os.path.join(hour_ws, item)
            for item in os.listdir(hour_ws)
            if item.endswith('dsr.tif') and int(item.split('_')[-4][10:12]) <= 30
        ]
        dsr_prev_paths = [
            os.path.join(hour_prev_ws, item)
            for item in os.listdir(hour_prev_ws)
            if item.endswith('dsr.tif') and int(item.split('_')[-4][10:12]) >= 30
        ]
        dsr_file_paths = sorted(dsr_prev_paths + dsr_tgt_paths)
    except Exception as e:
        logging.warning(e)
        pprint.pprint(os.listdir(hour_ws))
        pprint.pprint(os.listdir(hour_prev_ws))
        input('ENTER')

    if not dsr_file_paths:
        logging.info(f'  No source files in hour, skipping hour')
        return f'{tgt_date} - no source files in hour - skipping hour'


    # Compute hourly mean from the 10-minute files
    # TODO: Come up with different approach to account for missing images
    if not os.path.isfile(tif_file_path) or overwrite_flag:
        data_arrays = {}
        mask_arrays = {}

        for dsr_file_path in dsr_file_paths:
            dsr_file_name = os.path.basename(dsr_file_path)
            minute = dsr_file_name.split('_')[3][10:12]

            # dqf_file_path = dsr_file_path.replace('.nc', '_dqf.tif')
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
        if dsr_file_paths and (len(dsr_file_paths) == len(data_arrays.keys())):
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
            'source_files': ','.join([os.path.basename(x) for x in dsr_file_paths]),
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
