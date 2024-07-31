import argparse
import datetime
import logging
import os
import pprint
# import urllib3

import ee
from google.cloud import storage
import netCDF4
import numpy as np
import rasterio
import requests

# logging.getLogger('googleapiclient').setLevel(logging.ERROR)
# logging.getLogger('rasterio').setLevel(logging.ERROR)
# logging.getLogger('urllib3').setLevel(logging.INFO)

PROJECT_NAME = 'openet'
BUCKET_NAME = 'openet_temp'
BUCKET_FOLDER = 'meteorology/era5land/ancillary'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)


def main(project_id, overwrite_flag=False):
    """Ingest ERA5-Land ancillary assets into Earth Engine

    Parameters
    ----------
    project_id : str
        Earth Engine project ID.
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).

    """
    logging.info('\nERA5-Land Ancillary Asset')

    elev_url = 'https://confluence.ecmwf.int/download/attachments/140385202/' \
               'geo_1279l4_0.1x0.1.grib2_v4_unpack.nc'
    lsm_url = 'https://confluence.ecmwf.int/download/attachments/140385202/' \
               'lsm_1279l4_0.1x0.1.grb_v4_unpack.nc'

    output_ws = os.path.join(os.getcwd(), 'ancillary')
    if not os.path.isdir(output_ws):
        os.makedirs(output_ws)

    elev_nc = os.path.join(output_ws, elev_url.split('/')[-1])
    lsm_nc = os.path.join(output_ws, lsm_url.split('/')[-1])
    # elev_grb = os.path.join(output_ws, elev_url.split('/')[-1])
    # lsm_grb = os.path.join(output_ws, lsm_url.split('/')[-1])

    elev_tif = os.path.join(output_ws, 'elevation.tif')
    lsm_tif = os.path.join(output_ws, 'land_mask.tif')
    # lat_tif = os.path.join(output_ws, 'era5land_latitude.tif')
    # lon_tif = os.path.join(output_ws, 'era5land_longitude.tif')

    elev_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/elevation.tif'
    lsm_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/land_mask.tif'

    ASSET_FOLDER = 'projects/openet/assets/meteorology/era5land'
    elev_asset_id = f'{ASSET_FOLDER}/ancillary/elevation'
    lsm_asset_id = f'{ASSET_FOLDER}/ancillary/land_mask'
    lat_asset_id = f'{ASSET_FOLDER}/ancillary/latitude'
    lon_asset_id = f'{ASSET_FOLDER}/ancillary/longitude'

    elev_band_name = 'elevation'
    lsm_band_name = 'land_mask'
    lat_band_name = 'latitude'
    lon_band_name = 'longitude'
    # nodata_value = -9999

    output_proj = 'EPSG:4326'
    output_shape = (3600, 1801)
    # output_shape = (3601, 1801)  # The hourly assets in EE have an extra column
    output_geo = [0.1, 0, -180.05, 0, -0.1, 90.05]    # RasterIO transform
    output_extent = [
        output_geo[2], output_geo[5] + output_shape[1] * output_geo[4],
        output_geo[2] + output_shape[0] * output_geo[0], output_geo[5]
    ]
    logging.debug(f'  Projection: {output_proj}')
    logging.debug(f'  Shape:      {output_shape}')
    logging.debug(f'  Transform:  {output_geo}')
    logging.debug(f'  Extent:     {output_extent}')

    logging.info('\nInitializing Earth Engine')
    ee.Initialize(project=project_id)

    if overwrite_flag and ee.data.getInfo(lsm_asset_id):
        logging.info('\nLand surface mask asset already exists, removing')
        ee.data.deleteAsset(lsm_asset_id)
    if overwrite_flag and ee.data.getInfo(elev_asset_id):
        logging.info('\nElevation asset already exists, removing')
        ee.data.deleteAsset(elev_asset_id)
    if overwrite_flag and ee.data.getInfo(lat_asset_id):
        logging.info('\nLatitude asset already exists, removing')
        ee.data.deleteAsset(lat_asset_id)
    if overwrite_flag and ee.data.getInfo(lon_asset_id):
        logging.info('\nLongitude asset already exists, removing')
        ee.data.deleteAsset(lon_asset_id)


    # if not ee.data.getInfo(lsm_asset_id):
    if True:
        logging.info('\nLand Surface Mask (LSM)')

        if overwrite_flag or not os.path.isfile(lsm_nc):
            logging.info('Downloading netcdf file')
            logging.debug(f'  {lsm_url}')
            url_download(lsm_url, lsm_nc)

        logging.info('Reading netcdf file')
        logging.debug(f'  {lsm_nc}')
        mask_nc_f = netCDF4.Dataset(lsm_nc, 'r')
        mask_ma = np.roll(mask_nc_f.variables['lsm'][0, :, :], 1800, axis=1)
        mask_array = mask_ma.data.astype(np.float32)
        mask_nc_f.close()

        logging.info('Saving geotff')
        logging.debug(f'  {lsm_tif}')
        output_ds = rasterio.open(
            lsm_tif, 'w',
            driver='GTiff', tiled=True, blockxsize=256, blockysize=256,
            compress='deflate',
            # compress='lzw',
            # compress='deflate', predictor=2,
            # compress='lzw', predictor=1,
            count=1,
            crs=output_proj,
            dtype=rasterio.float32,
            height=mask_array.shape[0], width=mask_array.shape[1],
            nodata=-9999,
            transform=output_geo,
        )
        output_ds.write(mask_array, 1)
        output_ds.close()

        logging.info('Uploading to bucket')
        logging.debug(f'  {lsm_bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(lsm_bucket_path)}')
        blob.upload_from_filename(lsm_tif)

        # # For now, assume the file is in the bucket
        # logging.info('Ingesting into Earth Engine')
        # logging.debug(f'  {lsm_asset_id}')
        # task_id = ee.data.newTaskId()[0]
        # logging.debug(f'  {task_id}')
        # params = {
        #     'name': lsm_asset_id,
        #     'bands': [{'id': lsm_band_name, 'tilesetId': 'image', 'tilesetBandIndex': 0}],
        #     'tilesets': [{'id': 'image', 'sources': [{'uris': [lsm_bucket_path]}]}],
        #     'properties': {
        #         'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
        #     },
        #     # 'startTime': '2000-01-01T00:00:00' + '.000000000Z',
        #     # 'pyramidingPolicy': 'MEAN',
        #     # 'missingData': {'values': [nodata_value]},
        # }
        # ee.data.startIngestion(task_id, params, allow_overwrite=True)
        # input('Press ENTER after lsm asset is ingested')

        # logging.info('Removing from bucket')
        # if blob and blob.exists():
        #     blob.delete()

        # logging.debug('Removing local file')
        # if os.path.isfile(lsm_tif):
        #     os.remove(lsm_tif)
        # if os.path.isfile(lsm_nc):
        #     os.remove(lsm_nc)

        # # Make the asset public
        # policy = {'bindings': [{'role': 'roles/viewer', 'members': ['allUsers']}]}
        # ee.data.setIamPolicy(lst_asset_id, policy)


    # if not ee.data.getInfo(elev_asset_id):
    if True:
        logging.info('\nElevation')

        if overwrite_flag or not os.path.isfile(elev_nc):
            logging.info('Downloading netcdf file')
            logging.debug(f'  {elev_url}')
            url_download(elev_url, elev_nc)

        logging.info('Reading netcdf file')
        logging.debug(f'  {elev_nc}')
        elev_nc_f = netCDF4.Dataset(elev_nc, 'r')
        elev_ma = np.roll(elev_nc_f.variables['z'][0, :, :], 1800)
        elev_array = elev_ma.data.astype(np.float32)
        elev_nc_f.close()

        # Convert geopotential to geopotential height
        # CGM - Is this the same as "elevation"?
        elev_array /= 9.80665

        # # Set masked/nodata/ocean pixels to 0
        # if zero_elev_nodata_flag:
        #     elev_nodata = float(elev_ma.fill_value)
        #     # elev_array[
        #     #     (elev_array == elev_ma.fill_value) |
        #     #     (elev_array <= -1000)] = np.nan
        #     elev_array[np.isnan(elev_array)] = 0

        logging.info('Saving tif')
        logging.debug(f'  {elev_tif}')
        output_ds = rasterio.open(
            elev_tif, 'w',
            driver='GTiff', tiled=True, blockxsize=256, blockysize=256,
            compress='deflate',
            # compress='lzw',
            # compress='deflate', predictor=2,
            # compress='lzw', predictor=1,
            count=1,
            crs=output_proj,
            dtype=rasterio.float32,
            height=elev_array.shape[0], width=elev_array.shape[1],
            nodata=-9999,
            transform=output_geo,
        )
        output_ds.write(elev_array, 1)
        output_ds.close()

        logging.info('Uploading to bucket')
        logging.debug(f'  {elev_bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(elev_bucket_path)}')
        blob.upload_from_filename(elev_tif)

        # # For now, assume the file is in the bucket
        # logging.info('Ingesting into Earth Engine')
        # logging.debug(f'  {elev_asset_id}')
        # task_id = ee.data.newTaskId()[0]
        # logging.debug(f'  {task_id}')
        # params = {
        #     'name': elev_asset_id,
        #     'bands': [{'id': elev_band_name, 'tilesetId': 'image', 'tilesetBandIndex': 0}],
        #     'tilesets': [{'id': 'image', 'sources': [{'uris': [elev_bucket_path]}]}],
        #     'properties': {
        #         'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
        #     },
        #     # 'startTime': '2000-01-01T00:00:00' + '.000000000Z',
        #     # 'pyramidingPolicy': 'MEAN',
        #     # 'missingData': {'values': [nodata_value]},
        # }
        # ee.data.startIngestion(task_id, params, allow_overwrite=True)
        # input('Press ENTER after elevation asset is ingested')

        # logging.info('Removing from bucket')
        # if blob and blob.exists():
        #     blob.delete()

        # logging.debug('Removing local file')
        # if os.path.isfile(elev_tif):
        #     os.remove(elev_tif)
        # if os.path.isfile(elev_nc):
        #     os.remove(elev_nc)

        # # Make the asset public
        # policy = {'bindings': [{'role': 'roles/viewer', 'members': ['allUsers']}]}
        # ee.data.setIamPolicy(elev_asset_id, policy)


    # TODO: Pause in loop to wait for assets to finish ingesting
    #   For now just hold with a prompt
    if not ee.data.getInfo(elev_asset_id):
        input('Press ENTER after the elevation asset has ingested')


    # Build the latitude/longitude assets in EE from the elevation asset
    if not ee.data.getInfo(lat_asset_id):
        logging.info('\nExporting latitude asset')
        logging.debug(f'  {lat_asset_id}')
        lat_img = (
            ee.Image(elev_asset_id)
            .multiply(0)
            .add(ee.Image.pixelLonLat().select('latitude'))
            .rename([lat_band_name])
        )
        task = ee.batch.Export.image.toAsset(
            image=lat_img,
            description='openet_era5land_latitude_asset',
            assetId=lat_asset_id,
            dimensions='{}x{}'.format(*output_shape),
            crs=output_proj,
            crsTransform=output_geo,
        )
        task.start()

    if not ee.data.getInfo(lon_asset_id):
        logging.info('\nExporting longitude asset')
        logging.debug(f'  {lon_asset_id}')
        lon_img = (
            ee.Image(elev_asset_id)
            .multiply(0)
            .add(ee.Image.pixelLonLat().select('longitude'))
            .rename([lon_band_name])
        )
        task = ee.batch.Export.image.toAsset(
            image=lon_img,
            description='openet_era5land_longitude_asset',
            assetId=lon_asset_id,
            dimensions='{}x{}'.format(*output_shape),
            crs=output_proj,
            crsTransform=output_geo,
        )
        task.start()


    # # CGM - Pull the mask from one of the hourly images
    # #   (since they are already masked)
    # logging.info('\nBuilding mask from existing hourly asset')
    # if ee.data.getInfo(mask_asset_id):
    #     if overwrite_flag:
    #         logging.info('\nMask asset already exists, removing')
    #         ee.data.deleteAsset(mask_asset_id)
    #     else:
    #         logging.info('\nMask asset already exists, exiting')
    #         return True
    #
    # mask_img = ee.Image('ECMWF/ERA5_LAND/HOURLY/20170701T00')\
    #     .select(['temperature_2m'], ['land_mask'])\
    #     .gt(0)\
    #     .set({'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d')})
    #
    # export_task = ee.batch.Export.image.toAsset(
    #     image=mask_img,
    #     description='era5_land_mask_asset',
    #     assetId=mask_asset_id,
    #     dimensions=output_shape,
    #     crs=output_proj,
    #     crsTransform=output_geo,
    # )
    # logging.info('  Starting task')
    # export_task.start()


def url_download(download_url, output_path, verify=True):
    """Download file from a URL using requests module"""
    response = requests.get(download_url, stream=True, verify=verify)
    if response.status_code != 200:
        logging.error(f'  HTTPError: {response.status_code}')
        return False

    logging.debug('  Beginning download')
    with (open(output_path, "wb")) as output_f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:  # filter out keep-alive new chunks
                output_f.write(chunk)
    logging.debug('  Download complete')
    return True


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest ERA5-Land ancillary assets into Earth Engine',
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

    main(project_id=args.project, overwrite_flag=args.overwrite)
