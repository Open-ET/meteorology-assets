import argparse
import datetime
import logging
import math
import os
import time

import ee
from google.cloud import storage
import netCDF4
import numpy as np
import rasterio
import requests

logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('rasterio').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.INFO)

PROJECT_NAME = 'openet'
BUCKET_NAME = 'openet_temp'
BUCKET_FOLDER = 'meteorology/nldas3/ancillary'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)


def main(project_id, zero_elev_nodata_flag=False, overwrite_flag=False):
    """Ingest NLDAS-3 ancillary assets into Earth Engine

    Parameters
    ----------
    project_id : str
        Earth Engine project ID.
    zero_elev_nodata_flag : bool, optional
        If True, set elevation nodata values to 0 (the default is False).
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).

    """
    logging.info('\nNLDAS-3 Ancillary Assets')

    output_ws = os.path.join(os.getcwd(), 'ancillary')
    if not os.path.isdir(output_ws):
        os.makedirs(output_ws)

    nldas_url = 'https://portal.nccs.nasa.gov/lisdata_pub/NLDAS/NLDAS-3/lis_input.nldas3.noahmp401.1km.nc'
    nldas_nc = os.path.join(output_ws, nldas_url.split('/')[-1])

    elev_local_path = os.path.join(output_ws, 'elevation.tif')
    mask_local_path = os.path.join(output_ws, 'land_mask.tif')
    lat_local_path = os.path.join(output_ws, 'latitude.tif')
    lon_local_path = os.path.join(output_ws, 'longitude.tif')
    asp_local_path = os.path.join(output_ws, 'aspect.tif')
    slp_local_path = os.path.join(output_ws, 'slope.tif')

    elev_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/elevation.tif'
    mask_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/land_mask.tif'
    lat_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/latitude.tif'
    lon_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/longitude.tif'
    asp_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/aspect.tif'
    slp_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/slope.tif'

    elev_asset_id = 'projects/openet/assets/meteorology/nldas3/ancillary/elevation'
    mask_asset_id = 'projects/openet/assets/meteorology/nldas3/ancillary/land_mask'
    lat_asset_id = 'projects/openet/assets/meteorology/nldas3/ancillary/latitude'
    lon_asset_id = 'projects/openet/assets/meteorology/nldas3/ancillary/longitude'
    asp_asset_id = 'projects/openet/assets/meteorology/nldas3/ancillary/aspect'
    slp_asset_id = 'projects/openet/assets/meteorology/nldas3/ancillary/slope'

    elev_nc_name = 'ELEVATION'
    mask_nc_name = 'LANDMASK'
    lat_nc_name = 'lat'
    lon_nc_name = 'lon'
    asp_nc_name = 'ASPECT'
    slp_nc_name = 'SLOPE'

    elev_band_name = 'elevation'
    mask_band_name = 'land_mask'
    lat_band_name = 'latitude'
    lon_band_name = 'longitude'
    asp_band_name = 'aspect'
    slp_band_name = 'slope'

    nodata_value = -9999

    crs = 'EPSG:4326'
    # transform = [0.01, 0, -169.0, 0, -0.01, 72.0]
    transform = [0.01, 0, -168.995, 0, -0.01, 71.995]
    # transform = [0.01, 0, -168.995, 0, -0.01, 72.005]
    # shape = (11700, 6500)
    # extent = [
    #     transform[2], transform[5] + output_shape[0] * transform[4],
    #     transform[2] + output_shape[1] * transform[0], transform[5]
    # ]

    logging.info('\nInitializing Earth Engine')
    ee.Initialize(project=project_id)

    for var, asset_id in [
        ['Elevation', elev_asset_id],
        ['Land mask', mask_asset_id],
        ['Latitude', lat_asset_id],
        ['Longitude', lon_asset_id],
        ['Aspect', asp_asset_id],
        ['Slope', slp_asset_id],
    ]:
        if overwrite_flag and ee.data.getInfo(asset_id):
            logging.info(f'\n{var} asset already exists, removing')
            ee.data.deleteAsset(asset_id)

    if not os.path.isfile(nldas_nc):
        logging.info('\nDownloading NLDAS-3 elevation data NetCDF')
        logging.debug(f'  {nldas_url}')
        url_download(nldas_url, nldas_nc)

    if not ee.data.getInfo(elev_asset_id):
        logging.info('\nElevation Asset')
        logging.debug('Reading NetCDF')
        logging.debug(f'  {nldas_nc}')
        nldas_nc_f = netCDF4.Dataset(nldas_nc, 'r')
        elev_ma = nldas_nc_f.variables[elev_nc_name][:, :]
        elev_array = np.flip(elev_ma.data.astype(np.float32), 0)
        elev_nodata = float(elev_ma.fill_value)
        nldas_nc_f.close()

        # # CGM - This should not be applied since lakes are also masked in the elevation array
        # # Set masked/nodata/ocean pixels to 0
        # if zero_elev_nodata_flag:
        #     # elev_array[(elev_array == elev_ma.fill_value) | (elev_array <= -1000)] = np.nan
        #     elev_array[np.isnan(elev_array)] = 0

        logging.debug('Saving local tif')
        logging.debug(f'  {elev_local_path}')
        output_ds = rasterio.open(
            elev_local_path, 'w', count=1, dtype=rasterio.float32, nodata=nodata_value,
            driver='GTiff', tiled=True, compress='deflate', transform=transform, crs=crs,
            height=elev_array.shape[0], width=elev_array.shape[1],
        )
        output_ds.set_band_description(1, elev_band_name)
        output_ds.write(elev_array, 1)
        output_ds.close()

        logging.debug('Uploading to bucket')
        logging.debug(f'  {elev_bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(elev_bucket_path)}')
        blob.upload_from_filename(elev_local_path)

        # For now, assume the file is in the bucket at this point
        logging.info('Ingesting into Earth Engine')
        logging.debug(f'  {elev_asset_id}')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        params = {
            'name': elev_asset_id,
            'bands': [{'id': elev_band_name, 'tilesetId': 'image', 'tilesetBandIndex': 0}],
            'tilesets': [{'id': 'image', 'sources': [{'uris': [elev_bucket_path]}]}],
            'properties': {
                'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
            },
        }
        ee.data.startIngestion(task_id, params, allow_overwrite=True)

    if not ee.data.getInfo(mask_asset_id):
        logging.info('\nLand Mask Asset')
        logging.debug('Reading NetCDF')
        logging.debug(f'  {nldas_nc}')
        nldas_nc_f = netCDF4.Dataset(nldas_nc, 'r')
        mask_ma = nldas_nc_f.variables[mask_nc_name][:, :]
        mask_array = np.flip(mask_ma.data.astype(np.uint8), 0)
        # mask_nodata = float(mask_ma.fill_value)
        nldas_nc_f.close()

        logging.debug('\Saving local tif')
        logging.debug(f'  {mask_local_path}')
        output_ds = rasterio.open(
            mask_local_path, 'w', count=1, dtype=rasterio.uint8, nodata=0,
            driver='GTiff', tiled=True, compress='deflate', transform=transform, crs=crs,
            height=mask_array.shape[0], width=mask_array.shape[1],
        )
        output_ds.set_band_description(1, mask_band_name)
        output_ds.write(mask_array, 1)
        output_ds.close()

        logging.debug('Uploading to bucket')
        logging.debug(f'  {mask_bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(mask_bucket_path)}')
        blob.upload_from_filename(mask_local_path)

        # For now, assume the file is in the bucket at this point
        logging.info('Ingesting into Earth Engine')
        logging.debug(f'  {mask_asset_id}')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        params = {
            'name': mask_asset_id,
            'bands': [{'id': mask_band_name, 'tilesetId': 'image', 'tilesetBandIndex': 0}],
            'tilesets': [{'id': 'image', 'sources': [{'uris': [mask_bucket_path]}]}],
            'properties': {
                'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
            },
        }
        ee.data.startIngestion(task_id, params, allow_overwrite=True)


    # Process the other variables in a loop
    # This could be modified to support elevation and land mask
    #   but would need options for setting the data type and potential unmasking
    for var, asset_id, nc_name, band_name, local_path, bucket_path in [
        ['Latitude', lat_asset_id, lat_nc_name, lat_band_name, lat_local_path, lat_bucket_path],
        ['Longitude', lon_asset_id, lon_nc_name, lon_band_name, lon_local_path, lon_bucket_path],
        ['Aspect', asp_asset_id, asp_nc_name, asp_band_name, asp_local_path, asp_bucket_path],
        ['Slope', slp_asset_id, slp_nc_name, slp_band_name, slp_local_path, slp_bucket_path],
    ]:
        if not ee.data.getInfo(asset_id):
            logging.info(f'\n{var} Asset')
            logging.debug('Reading NetCDF')
            logging.debug(f'  {nldas_nc}')
            nc_f = netCDF4.Dataset(nldas_nc, 'r')
            ma = nc_f.variables[nc_name][:, :]
            array = np.flip(ma.data.astype(np.float32), 0)
            # mc_nodata = float(ma.fill_value)
            nc_f.close()
    
            logging.debug('Saving local tif')
            logging.debug(f'  {local_path}')
            output_ds = rasterio.open(
                local_path, 'w', count=1, dtype=rasterio.float32, nodata=nodata_value,
                driver='GTiff', tiled=True, compress='deflate', transform=transform, crs=crs,
                height=array.shape[0], width=array.shape[1],
            )
            output_ds.set_band_description(1, band_name)
            output_ds.write(array, 1)
            output_ds.close()
    
            logging.debug('Uploading to bucket')
            logging.debug(f'  {bucket_path}')
            bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
            blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(bucket_path)}')
            blob.upload_from_filename(local_path)
    
            # For now, assume the file is in the bucket at this point
            logging.info('Ingesting into Earth Engine')
            logging.debug(f'  {asset_id}')
            task_id = ee.data.newTaskId()[0]
            logging.debug(f'  {task_id}')
            params = {
                'name': asset_id,
                'bands': [{'id': lat_band_name, 'tilesetId': 'image', 'tilesetBandIndex': 0}],
                'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
                'properties': {
                    'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
                    'source': nldas_url,
                },
                # 'startTime': '2000-01-01T00:00:00' + '.000000000Z',
                # 'pyramidingPolicy': 'MEAN',
                # 'missingData': {'values': [nodata_value]},
            }
            ee.data.startIngestion(task_id, params, allow_overwrite=True)

            # # Make the asset public
            # policy = {
            #     "all_users_can_read": True,
            #     "owners": ["group:openet@googlegroups.com"],
            #     "readers": [],
            #     "writers": [],
            # }
            # ee.data.setIamPolicy(elev_asset_id, policy)


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
    for i in range(1, 4):
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
            time.sleep(i ** 3)
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


# def url_download(download_url, output_path, verify=True):
#     """Download file from a URL using requests module"""
#     response = requests.get(download_url, stream=True, verify=verify)
#     if response.status_code != 200:
#         logging.error(f'  HTTPError: {response.status_code}')
#         return False
#
#     logging.debug('  Beginning download')
#     with (open(output_path, "wb")) as output_f:
#         for chunk in response.iter_content(chunk_size=1024 * 1024):
#             if chunk:  # filter out keep-alive new chunks
#                 output_f.write(chunk)
#     logging.debug('  Download complete')
#     return True


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest NLDAS-3 ancillary assets into Earth Engine',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--project', type=str, required=True, help='Earth Engine Project ID')
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
    logging.getLogger('googleapiclient').setLevel(logging.ERROR)

    main(
        project_id=args.project,
        # zero_elev_nodata_flag=args.zero,
        overwrite_flag=args.overwrite,
    )
