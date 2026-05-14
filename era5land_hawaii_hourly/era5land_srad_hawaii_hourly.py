import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import pprint

import ee
import numpy as np
import openet.core.utils as utils
import rasterio
from rasterio.warp import reproject, Resampling
import xee
import xarray
# import numpy.ma as ma
# from pyproj import CRS
# from rasterio.transform import from_bounds
# import rasterio.shutil

SOURCE_COLL_ID = 'ECMWF/ERA5_LAND/HOURLY'

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

def asset_ingest(tgt_dt, workspace='/tmp', workers=10, overwrite=False, cleanup=True):
    """Download ERA5-Land solar radiation hourly assets into Earth Engine

    Parameters
    ----------
    tgt_dt : datetime
    workspace : str
    workers : int, optional
    overwrite : bool, optional
        If True, overwrite existing assets.
    cleanup : bool, optional

    """
    tgt_date = tgt_dt.strftime('%Y-%m-%dT%H00')
    logging.info(f'{tgt_date} ({tgt_dt.strftime("%j")})')

    # Save the hourly files in the DOY folder
    year_ws = os.path.join(workspace, tgt_dt.strftime('%Y'))
    date_ws = os.path.join(year_ws, tgt_dt.strftime('%Y%m%d'))
    tif_name = f'{tgt_dt.strftime("%Y%m%d%H")}.tif'
    tif_path = os.path.join(date_ws, tif_name)
    temp_path = tif_path.replace('.tif', '_temp.tif')
    if not os.path.isdir(date_ws):
        os.makedirs(date_ws)

    if not overwrite and os.path.isfile(tif_path):
        logging.info('  File already exists and overwrite is false, skipping')
        return True

    src_band_name = 'surface_solar_radiation_downwards_hourly'
    dst_band_name = 'SRAD_ERA5LAND'
    export_info = {
        'extent': [-161, 18, -154, 23],
        'shape': (70, 50),
        'crs': 'EPSG:4326',
        'geo': (0.1, 0, -161.05, 0, -0.1, 23.05),
    }
    nodata = -9999
    dtype = rasterio.float32

    #   To better mimic what the models are expecting the values to be,
    #   compute the instantaneous value
    srad_prev_img = (
        ee.Image(f'{SOURCE_COLL_ID}/{(tgt_dt).strftime("%Y%m%dT%H")}')
        .select([src_band_name], [dst_band_name])
        .divide(3600)
    )
    srad_next_img = (
        ee.Image(f'{SOURCE_COLL_ID}/{(tgt_dt + timedelta(hours=1)).strftime("%Y%m%dT%H")}')
        .select([src_band_name], [dst_band_name])
        .divide(3600)
    )
    srad_img = srad_prev_img.add(srad_next_img).divide(2)
    # srad_img = (
    #     ee.Image(f'{SOURCE_COLL_ID}/{tgt_dt.strftime("%Y%m%dT%H")}')
    #     .select([src_band_name], [dst_band_name])
    #     .divide(3600)
    # )

    # Fill any masked pixels along the edge of the land mask
    # This will fill most coastal pixels and any small holes
    # Filling must be applied separately per band
    def fill(image):
        img = ee.Image(image)
        fill_edge_cells = 2
        return img.unmask(
            img.reduceNeighborhood('mean', ee.Kernel.square(fill_edge_cells), 'kernel', False)
            .reproject(img.projection())
        )
    srad_img = fill(srad_img)
    # srad_img = srad_img.unmask(nodata))

    # Save the image to geotiff
    # if overwrite_flag or not os.path.isfile(tif_path):
    logging.debug('  Building output GeoTIFF')
    with rasterio.open(
        temp_path, 'w',
        driver='GTiff',
        tiled=True,
        blockxsize=512,
        blockysize=512,
        compress='deflate',
        # compress='lzw',
        count=1,
        dtype=dtype,
        nodata=nodata,
        height=export_info['shape'][1],
        width=export_info['shape'][0],
        crs=export_info['crs'],
        transform=export_info['geo'],
    ) as output_ds:
        output_ds.set_band_description(1, dst_band_name)
        output_ds.write(np.full(export_info['shape'], nodata, dtype=dtype), 1)

    logging.debug('  Writing arrays')

    try:
        output_xr = xarray.open_dataset(
            srad_img,
            engine='ee',
            crs=export_info['crs'],
            crs_transform=tuple(export_info['geo']),
            shape_2d=export_info['shape'],
            executor_kwargs={'max_workers': workers}
        )
        output_array = output_xr[dst_band_name].values[0, :, :]
    except Exception as e:
        logging.info('  Error reading array data, skipping')
        logging.info(f'  {e}')
        os.remove(temp_path)
        return False

    with rasterio.open(temp_path, 'r+') as output_ds:
        output_ds.write(output_array, 1)

    if not os.path.isfile(temp_path):
        logging.info('  Temporary file not created, skipping')
        return False

    # Reproject to the URMA grid
    with rasterio.open(temp_path, "r") as src_ds:
        with rasterio.open(
            tif_path,
            "w",
            driver="GTiff",
            height=DST_HEIGHT,
            width=DST_WIDTH,
            count=1,
            dtype=rasterio.float32,
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

    # Cleanup
    if cleanup:
        os.remove(temp_path)

    # # Ingest into GEE
    # if os.path.isfile(tif_file_path) and ingest_flag:
    #     logging.debug('  Uploading to bucket')
    #     bucket_path = f'gs://{GCP_BUCKET_NAME}/{GCP_BUCKET_FOLDER}/{tif_file_name}'
    #     bucket = STORAGE_CLIENT.bucket(GCP_BUCKET_NAME)
    #     blob = bucket.blob(f'{GCP_BUCKET_FOLDER}/{os.path.basename(bucket_path)}')
    #     blob.upload_from_filename(tif_file_path)
    #
    #     # Assume the file made it into the bucket
    #     logging.debug('  Ingesting into Earth Engine')
    #     task_id = ee.data.newTaskId()[0]
    #     logging.debug(f'  {task_id}')
    #
    #     properties = {
    #         'date_ingested': f'{TODAY_DT.strftime("%Y-%m-%d")}',
    #         'source_bucket': f's3://{AWS_BUCKET_NAME}/{AWS_BUCKET_FOLDER}',
    #         'source_files': ','.join(nc_file_names),
    #         'units_DSR': 'W m-2',
    #     }
    #
    #     params = {
    #         'name': asset_id,
    #         'bands': [
    #             {'id': v, 'tilesetId': 'image', 'tilesetBandIndex': i}
    #             for i, v in enumerate(['DSR'])
    #         ],
    #         'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
    #         'properties': properties,
    #         'startTime': tgt_dt.isoformat() + '.000000000Z',
    #         # 'pyramiding_policy': 'MEAN',
    #         # 'missingData': {'values': [nodata_value]},
    #     }
    #
    #     # TODO: Wrap in a try/except loop
    #     ee.data.startIngestion(task_id, params, allow_overwrite=True)


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
        description='Ingest Hawaii ERA5-LAND solar hourly assets into Earth Engine',
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
                overwrite=args.overwrite,
            )
