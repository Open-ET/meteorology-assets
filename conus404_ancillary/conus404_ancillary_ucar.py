import argparse
import datetime
import logging
import os
import pprint

import ee
from google.cloud import storage
import numpy as np
from pydap.client import open_url
from pydap.cas.urs import setup_session
import rasterio
import rasterio.warp
import skimage

import config

# logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('pydap').setLevel(logging.WARNING)
logging.getLogger('rasterio').setLevel(logging.WARNING)
# logging.getLogger('requests').setLevel(logging.INFO)
# logging.getLogger('urllib3').setLevel(logging.INFO)

# CGM - Switching to native assets for now until COG projection issue is worked out
ASSET_FOLDER = 'projects/earthengine-legacy/assets/projects/openet/meteorology/conus/conus404'
# ASSET_FOLDER = 'projects/openet/assets/meteorology/conus404'
PROJECT_NAME = 'openet'
BUCKET_NAME = 'openet_temp'
BUCKET_FOLDER = 'meteorology/conus404/ancillary'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)
NC_URL = "https://thredds.rda.ucar.edu/thredds/dodsC/files/g/ds559.0/INVARIANT/wrfconstants_usgs404.nc"
# NC_URL = "https://rda.ucar.edu/thredds/dodsC/files/g/ds559.0/INVARIANT/wrfconstants_usgs404.nc"


def main(overwrite_flag=False):
    """Build and ingest CONUS404 ancillary assets into Earth Engine

    Parameters
    ----------
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).

    """
    logging.info('\nCONUS404 ancillary assets')

    variables = ['LANDMASK', 'LAKEMASK', 'LU_INDEX', 'HGT', 'XLAT', 'XLONG']

    band_names = {
        'LANDMASK': 'land_mask',
        'LAKEMASK': 'lake_mask',
        'LU_INDEX': 'land_use',
        'HGT': 'elevation',
        'XLAT': 'latitude',
        'XLONG': 'longitude',
        # 'XLAND': 'land',
    }
    file_names = {
        'LANDMASK': 'land_mask',
        'LAKEMASK': 'lake_mask',
        'LU_INDEX': 'land_use',
        'HGT': 'elevation',
        'XLAT': 'latitude',
        'XLONG': 'longitude',
        # 'XLAND': 'lank',
    }

    cellsize = 4000
    crs = rasterio.crs.CRS.from_proj4(
        '+proj=lcc +lat_1=30.0 +lat_2=50.0 +lat_0=39.100006 +lon_0=-97.9 '
        '+a=6370000 +b=6370000 +units=m +no_defs=True'
    )
    # The crs parameter needs to be manually set with the WKT in the ingest call
    # To get the alignment right the datum needs to be to WGS84 (instead of a sphere)
    #   and the semi_major and semi_minor parameters need to be added
    # I'm not sure exactly what to set the semi_major/semi_minor to
    #   The FRET wkt had 6371200
    ee_wkt = (
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
    # overview_levels = [2, 4, 8]
    shape = (1015, 1367)
    transform = [
        cellsize, 0.0, -(shape[1] / 2) * cellsize,
        0.0, -cellsize, (shape[0] / 2) * cellsize
    ]

    workspace = os.getcwd()
    ancillary_ws = os.path.join(workspace, 'ancillary')
    if not os.path.isdir(ancillary_ws):
        os.makedirs(ancillary_ws)

    logging.info('\nInitializing Earth Engine')
    ee.Initialize()

    for var_name in variables:
        logging.info(f'\n{var_name}')

        tif_path = os.path.join(ancillary_ws, f'{file_names[var_name]}.tif')
        bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{file_names[var_name]}.tif'
        asset_id = f'{ASSET_FOLDER}/ancillary/{file_names[var_name]}'
        logging.debug(f' {tif_path}')
        logging.debug(f' {bucket_path}')
        logging.debug(f' {asset_id}')

        if var_name in ['XLAT', 'XLONG', 'HGT']:
            nodata_value = -9999
            # dtype = rasterio.float64
            dtype = rasterio.float32
            # resampling = rasterio.warp.Resampling.average
            # resampling_str = 'average'
            pyramid_policy = 'MEAN'
        else:
            nodata_value = 255
            dtype = rasterio.uint8
            # resampling = rasterio.warp.Resampling.mode
            # resampling_str = 'mode'
            # # resampling = rasterio.warp.Resampling.nearest
            # # resampling = 'nearest'
            pyramid_policy = 'MODE'
        logging.debug(f' {nodata_value}')
        logging.debug(f' {dtype}')
        logging.debug(f' {pyramid_policy}')

        if overwrite_flag or not os.path.isfile(tif_path):
            # Download the array
            try:
                array = open_url(NC_URL)[var_name][:].data
            except Exception as e:
                logging.exception(e)
                # logging.warning('unhandled exception downloading array, skipping')
                input('ENTER')
                continue

            # Subset and flip the arrays
            if array.shape == (1, shape[0], shape[1]):
                array = np.flipud(array[0, :, :])
            elif array.shape == (shape[0], shape[1]):
                array = np.flipud(array[:, :])
            else:
                logging.warning(f'unexpected array shape ({array.shape}), skipping')
                continue

            logging.info(f'Writing geotiff')
            output_ds = rasterio.open(
                tif_path, 'w', driver='GTiff', dtype=dtype, nodata=nodata_value,
                width=shape[1], height=shape[0], count=1, crs=crs, transform=transform,
                tiled=True, compress='lzw',
            )
            output_ds.set_band_description(1, band_names[var_name])
            output_ds.write(array, 1)
            output_ds.close()

            # logging.debug(f'Building overviews')
            # output_ds = rasterio.open(tif_path, 'r+')
            # output_ds.build_overviews(overview_levels, resampling)
            # output_ds.update_tags(ns='rio_overview', resampling=resampling_str)
            # output_ds.close()
            # del output_ds

        if ee.data.getInfo(asset_id):
            if overwrite_flag:
                logging.info(f'Asset already exists, removing')
                # TODO: Try/Except on delete
                ee.data.deleteAsset(asset_id)
            else:
                logging.info(f'Asset already exists and overwrite is False, skipping')
                continue

        logging.info('Uploading to bucket')
        logging.debug(f'  {bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(bucket_path)}')
        blob.upload_from_filename(tif_path)

        # For now, assume the file is in the bucket
        logging.info('Ingesting into Earth Engine')
        logging.debug(f'  {asset_id}')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        params = {
            'name': asset_id,
            'bands': [
                {'id': band_names[var_name], 'tilesetId': 'image', 'tilesetBandIndex': 0}
            ],
            'tilesets': [{
                'id': 'image',
                'crs': ee_wkt,
                'sources': [{
                    'uris': [bucket_path],
                    'affine_transform': {
                        'scale_x': transform[0],
                        'shear_x': transform[1],
                        'translate_x': transform[2],
                        'shear_y': transform[3],
                        'scale_y': transform[4],
                        'translate_y': transform[5],
                      },
                }],
            }],
            'properties': {
                'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
                'source': NC_URL,
            },
            # 'startTime': '2000-01-01T00:00:00' + '.000000000Z',
            'pyramidingPolicy': pyramid_policy,
            # 'missingData': {'values': [nodata_value]},
        }
        ee.data.startIngestion(task_id, params, allow_overwrite=True)
        # input('Press ENTER after assets are ingested')

        # logging.info('Removing from bucket')
        # if blob and blob.exists():
        #     blob.delete()


    # Compute a custom land mask that is buffered ~4 cells (16km) into the ocean
    logging.info(f'\nCustom Mask')
    tif_path = os.path.join(ancillary_ws, 'mask.tif')
    bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/mask.tif'
    asset_id = f'{ASSET_FOLDER}/ancillary/mask'
    logging.debug(f' {tif_path}')
    logging.debug(f' {bucket_path}')
    logging.debug(f' {asset_id}')

    if not os.path.isfile(tif_path) or overwrite_flag:
        with rasterio.open(os.path.join(ancillary_ws, 'land_mask.tif')) as src:
            land_mask = src.read(1)
        with rasterio.open(os.path.join(ancillary_ws, 'lake_mask.tif')) as src:
            lake_mask = src.read(1)

        mask = np.logical_or(land_mask, lake_mask)

        c4 = skimage.morphology.disk(radius=4)
        c1 = skimage.morphology.disk(radius=1)
        # c4 = np.array([
        #     [0, 0, 0, 0, 1, 0, 0, 0, 0],
        #     [0, 0, 1, 1, 1, 1, 1, 0, 0],
        #     [0, 1, 1, 1, 1, 1, 1, 1, 0],
        #     [0, 1, 1, 1, 1, 1, 1, 1, 0],
        #     [1, 1, 1, 1, 1, 1, 1, 1, 1],
        #     [0, 1, 1, 1, 1, 1, 1, 1, 0],
        #     [0, 1, 1, 1, 1, 1, 1, 1, 0],
        #     [0, 0, 1, 1, 1, 1, 1, 0, 0],
        #     [0, 0, 0, 0, 1, 0, 0, 0, 0]
        # ], dtype=np.uint8)
        # c1 = np.array([
        #     [0, 1, 0],
        #     [1, 1, 1],
        #     [0, 1, 0]
        # ], dtype=np.uint8)
        mask = skimage.morphology.binary_dilation(mask, c4)
        mask = skimage.morphology.binary_erosion(mask, c1)

        logging.info(f'Writing geotiff')
        output_ds = rasterio.open(
            tif_path, 'w', driver='GTiff', dtype=rasterio.uint8, nodata=255,
            width=shape[1], height=shape[0], count=1, crs=crs, transform=transform,
            tiled=True, compress='lzw',
        )
        output_ds.set_band_description(1, 'mask')
        output_ds.write(mask, 1)
        output_ds.close()

        # resampling = rasterio.warp.Resampling.mode
        # resampling_str = 'mode'
        # # resampling = rasterio.warp.Resampling.nearest
        # # resampling = 'nearest'
        pyramid_policy = 'MODE'
        # logging.debug(f'Building overviews')
        # output_ds = rasterio.open(tif_path, 'r+')
        # output_ds.build_overviews(overview_levels, resampling)
        # output_ds.update_tags(ns='rio_overview', resampling=resampling_str)
        # output_ds.close()
        # del output_ds

    if overwrite_flag and ee.data.getInfo(asset_id):
        logging.info('Asset already exists, removing')
        # TODO: Try/Except on delete
        ee.data.deleteAsset(asset_id)

    # if overwrite_flag or not ee.data.getInfo(asset_id):
    if not ee.data.getInfo(asset_id):
        logging.info('Uploading to bucket')
        logging.debug(f'  {bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(bucket_path)}')
        blob.upload_from_filename(tif_path)

        # Assuming the file made it into the bucket
        logging.info('Ingesting into Earth Engine')
        logging.debug(f'  {asset_id}')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        params = {
            'name': asset_id,
            'bands': [{'id': 'mask', 'tilesetId': 'image', 'tilesetBandIndex': 0}],
            'tilesets': [{
                'id': 'image',
                'crs': ee_wkt,
                'sources': [{
                    'uris': [bucket_path],
                    'affine_transform': {
                        'scale_x': transform[0],
                        'shear_x': transform[1],
                        'translate_x': transform[2],
                        'shear_y': transform[3],
                        'scale_y': transform[4],
                        'translate_y': transform[5],
                      },
                }],
            }],
            'properties': {
                'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
                'source': NC_URL,
            },
            # 'startTime': '2000-01-01T00:00:00' + '.000000000Z',
            'pyramidingPolicy': pyramid_policy,
            # 'missingData': {'values': [nodata_value]},
        }
        ee.data.startIngestion(task_id, params, allow_overwrite=True)
        # input('Press ENTER after assets are ingested')

        # logging.info('Removing from bucket')
        # if blob and blob.exists():
        #     blob.delete()


def wrfconstant_download():
    import requests
    import sys

    logging.info(f'\nDownloading CONUS404 WRF Constant NetCDF')

    def check_file_status(filepath, filesize):
        sys.stdout.write('\r')
        sys.stdout.flush()
        size = int(os.stat(filepath).st_size)
        percent_complete = (size / filesize) * 100
        sys.stdout.write('%.3f %s' % (percent_complete, '% Completed'))
        sys.stdout.flush()

    url = 'https://rda.ucar.edu/cgi-bin/login'
    values = {'email': config.ucar_username, 'passwd': config.ucar_password, 'action': 'login'}
    ret = requests.post(url, data=values)
    if ret.status_code != 200:
        print('Bad Authentication')
        print(ret.text)
        exit(1)

    file_name = 'wrfconstants_usgs404.nc'
    req = requests.get(
        'https://rda.ucar.edu/data/ds559.0/INVARIANT/wrfconstants_usgs404.nc',
        cookies=ret.cookies, allow_redirects=True, stream=True
    )
    filesize = int(req.headers['Content-length'])
    with open(file_name, 'wb') as outfile:
        chunk_size = 1048576
        for chunk in req.iter_content(chunk_size=chunk_size):
            outfile.write(chunk)
            if chunk_size < filesize:
                check_file_status(file_name, filesize)
    check_file_status(file_name, filesize)
    print()


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest CONUS404 ancillary assets into Earth Engine',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument(
    #     '--zero', default=False, action='store_true',
    #     help='Set elevation nodata values to 0')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    main(overwrite_flag=args.overwrite)
