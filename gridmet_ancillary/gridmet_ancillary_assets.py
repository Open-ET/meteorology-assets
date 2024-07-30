import argparse
import datetime
import logging
import os
import pprint

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
BUCKET_FOLDER = 'meteorology/gridmet/ancillary'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)


def main(project_id, overwrite_flag=False):
    """Ingest GridMET ancillary assets into Earth Engine

    Parameters
    ----------
    project_id : str
        Earth Engine project ID.
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).

    """
    logging.info('\nGridMET Ancillary Asset')

    elev_url = 'https://climate.northwestknowledge.net/METDATA/data/metdata_elevationdata.nc'

    # Get the land surface mask from one of the data netcdfs
    mask_url = 'https://www.northwestknowledge.net/metdata/data/tmmx_2000.nc'
    mask_nc_var = 'air_temperature'

    output_ws = os.path.join(os.getcwd(), 'ancillary')
    if not os.path.isdir(output_ws):
        os.makedirs(output_ws)

    elev_nc = os.path.join(output_ws, elev_url.split('/')[-1])
    mask_nc = os.path.join(output_ws, mask_url.split('/')[-1])

    elev_tif = os.path.join(output_ws, 'elevation.tif')
    mask_tif = os.path.join(output_ws, 'land_mask.tif')

    elev_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/elevation.tif'
    mask_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/land_mask.tif'

    ASSET_FOLDER = 'projects/openet/assets/meteorology/gridmet'
    elev_asset_id = f'{ASSET_FOLDER}/ancillary/elevation'
    mask_asset_id = f'{ASSET_FOLDER}/ancillary/land_mask'
    lat_asset_id = f'{ASSET_FOLDER}/ancillary/latitude'
    lon_asset_id = f'{ASSET_FOLDER}/ancillary/longitude'

    elev_band_name = 'elevation'
    mask_band_name = 'land_mask'
    lat_band_name = 'latitude'
    lon_band_name = 'longitude'
    # nodata_value = -9999

    output_proj = 'EPSG:4326'
    output_shape = (1386, 585)
    output_geo = [
        0.041666666666666664, 0, -124.78749996666667,
        0, -0.041666666666666664, 49.42083333333334
    ]
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

    # if overwrite_flag and ee.data.getInfo(mask_asset_id):
    #     logging.info('\nLand surface mask asset already exists, removing')
    #     ee.data.deleteAsset(mask_asset_id)
    if overwrite_flag and ee.data.getInfo(elev_asset_id):
        logging.info('\nElevation asset already exists, removing')
        ee.data.deleteAsset(elev_asset_id)
    if overwrite_flag and ee.data.getInfo(lat_asset_id):
        logging.info('\nLatitude asset already exists, removing')
        ee.data.deleteAsset(lat_asset_id)
    if overwrite_flag and ee.data.getInfo(lon_asset_id):
        logging.info('\nLongitude asset already exists, removing')
        ee.data.deleteAsset(lon_asset_id)


    # if not ee.data.getInfo(mask_asset_id):
    if True:
        logging.info('\nLand Surface Mask')

        if overwrite_flag or not os.path.isfile(mask_nc):
            logging.info('Downloading netcdf file')
            logging.debug(f'  {mask_url}')
            url_download(mask_url, mask_nc)

        logging.info('Reading netcdf file')
        logging.debug(f'  {mask_nc}')
        mask_nc_f = netCDF4.Dataset(mask_nc, 'r')
        mask_ma = mask_nc_f.variables[mask_nc_var][0, :, :]
        mask_array = (mask_ma.data != mask_ma.fill_value).astype(np.uint8)
        mask_nc_f.close()

        logging.info('Saving geotff')
        logging.debug(f'  {mask_tif}')
        output_ds = rasterio.open(
            mask_tif, 'w',
            driver='GTiff', tiled=True, blockxsize=256, blockysize=256,
            compress='deflate',
            # compress='lzw',
            # compress='deflate', predictor=2,
            # compress='lzw', predictor=1,
            count=1,
            crs=output_proj,
            dtype=rasterio.uint8,
            height=mask_array.shape[0], width=mask_array.shape[1],
            # nodata=0,
            transform=output_geo,
        )
        output_ds.write(mask_array, 1)
        output_ds.close()

        logging.info('Uploading to bucket')
        logging.debug(f'  {mask_bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(mask_bucket_path)}')
        blob.upload_from_filename(mask_tif)

        # For now, assume the file is in the bucket
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
            # 'startTime': '2000-01-01T00:00:00' + '.000000000Z',
            # 'pyramidingPolicy': 'MEAN',
            # 'missingData': {'values': [nodata_value]},
        }
        ee.data.startIngestion(task_id, params, allow_overwrite=True)
        input('Press ENTER after mask asset is ingested')

        # logging.info('Removing from bucket')
        # if blob and blob.exists():
        #     blob.delete()

        # logging.debug('Removing local file')
        # if os.path.isfile(mask_tif):
        #     os.remove(mask_tif)
        # if os.path.isfile(mask_nc):
        #     os.remove(mask_nc)

        # Make the asset public
        policy = {'bindings': [{'role': 'roles/viewer', 'members': ['allUsers']}]}
        ee.data.setIamPolicy(mask_asset_id, policy)


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
        elev_ma = elev_nc_f.variables['elevation'][:, :]
        elev_array = elev_ma.data.astype(np.float32)
        elev_nc_f.close()

        # # Set masked/nodata/ocean pixels to 0
        # if zero_elev_nodata_flag:
        #     elev_nodata = float(elev_ma.fill_value)
        #     elev_array[elev_array == elev_nodata] = 0

        # Apply the land surface mask to the elevation data?
        elev_nodata = -9999
        elev_array[mask_array == 0] = elev_nodata

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
            nodata=elev_nodata,
            transform=output_geo,
        )
        output_ds.write(elev_array, 1)
        output_ds.close()

        logging.info('Uploading to bucket')
        logging.debug(f'  {elev_bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(elev_bucket_path)}')
        blob.upload_from_filename(elev_tif)

        # For now, assume the file is in the bucket
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
            # 'startTime': '2000-01-01T00:00:00' + '.000000000Z',
            # 'pyramidingPolicy': 'MEAN',
            # 'missingData': {'values': [nodata_value]},
        }
        ee.data.startIngestion(task_id, params, allow_overwrite=True)
        input('Press ENTER after elevation asset is ingested')

        # logging.info('Removing from bucket')
        # if blob and blob.exists():
        #     blob.delete()

        # logging.debug('Removing local file')
        # if os.path.isfile(elev_tif):
        #     os.remove(elev_tif)
        # if os.path.isfile(elev_nc):
        #     os.remove(elev_nc)

        # Make the asset public
        policy = {'bindings': [{'role': 'roles/viewer', 'members': ['allUsers']}]}
        ee.data.setIamPolicy(elev_asset_id, policy)


    # # TODO: Pause in loop to wait for assets to finish ingesting
    # #   For now just hold with a prompt
    # if not ee.data.getInfo(elev_asset_id):
    #     input('Press ENTER after the elevation asset has ingested')


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
            description='openet_gridmet_latitude_asset',
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
            description='openet_gridmet_longitude_asset',
            assetId=lon_asset_id,
            dimensions='{}x{}'.format(*output_shape),
            crs=output_proj,
            crsTransform=output_geo,
        )
        task.start()


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
        description='Ingest GridMET ancillary assets into Earth Engine',
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
