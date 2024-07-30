import argparse
import datetime
import logging
import math
import os

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
BUCKET_FOLDER = 'meteorology/nldas/ancillary'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)


def main(project_id, zero_elev_nodata_flag=False, overwrite_flag=False):
    """Ingest NLDAS ancillary assets into Earth Engine

    Parameters
    ----------
    project_id : str
        Earth Engine project ID.
    zero_elev_nodata_flag : bool, optional
        If True, set elevation nodata values to 0 (the default is False).
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).

    """
    logging.info('\nNLDAS Ancillary Assets')

    output_ws = os.path.join(os.getcwd(), 'ancillary')
    if not os.path.isdir(output_ws):
        os.makedirs(output_ws)

    nldas_url = 'https://ldas.gsfc.nasa.gov/sites/default/files/ldas/nldas/NLDAS_elevation.nc4'
    nldas_nc = os.path.join(output_ws, nldas_url.split('/')[-1])

    elev_local_tif = os.path.join(output_ws, 'nldas_elev.tif')
    aspect_local_tif = os.path.join(output_ws, 'nldas_aspect.tif')
    slope_local_tif = os.path.join(output_ws, 'nldas_slope.tif')

    elev_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/elevation.tif'
    aspect_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/aspect.tif'
    slope_bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/slope.tif'

    elev_asset_id = 'projects/openet/assets/meteorology/nldas/ancillary/elevation'
    aspect_asset_id = 'projects/openet/assets/meteorology/nldas/ancillary/aspect'
    slope_asset_id = 'projects/openet/assets/meteorology/nldas/ancillary/slope'
    lat_asset_id = 'projects/openet/assets/meteorology/nldas/ancillary/latitude'
    lon_asset_id = 'projects/openet/assets/meteorology/nldas/ancillary/longitude'

    elev_band_name = 'elevation'
    aspect_band_name = 'aspect'
    slope_band_name = 'slope'
    lat_band_name = 'latitude'
    lon_band_name = 'longitude'
    nodata_value = -9999

    output_proj = 'EPSG:4326'
    output_shape = (464, 224)
    output_geo = [0.125, 0, -125, 0, -0.125, 53]    # RasterIO transform
    # output_extent = [
    #     output_geo[2], output_geo[5] + output_shape[0] * output_geo[4],
    #     output_geo[2] + output_shape[1] * output_geo[0], output_geo[5]
    # ]

    logging.info('\nInitializing Earth Engine')
    ee.Initialize(project=project_id)

    if overwrite_flag and ee.data.getInfo(elev_asset_id):
        logging.info('\nElevation asset already exists, removing')
        ee.data.deleteAsset(elev_asset_id)
    if overwrite_flag and ee.data.getInfo(aspect_asset_id):
        logging.info('\nAspect asset already exists, removing')
        ee.data.deleteAsset(aspect_asset_id)
    if overwrite_flag and ee.data.getInfo(slope_asset_id):
        logging.info('\nSlope asset already exists, removing')
        ee.data.deleteAsset(slope_asset_id)
    if overwrite_flag and ee.data.getInfo(lat_asset_id):
        logging.info('\nLatitude asset already exists, removing')
        ee.data.deleteAsset(lat_asset_id)
    if overwrite_flag and ee.data.getInfo(lon_asset_id):
        logging.info('\nLongitude asset already exists, removing')
        ee.data.deleteAsset(lon_asset_id)


    if (not ee.data.getInfo(elev_asset_id)
            or not ee.data.getInfo(aspect_asset_id)
            or not ee.data.getInfo(slope_asset_id)):
        logging.info('\nDownloading NLDAS elevation data NetCDF')
        logging.debug(f'  {nldas_url}')
        url_download(nldas_url, nldas_nc)


    if not ee.data.getInfo(elev_asset_id):
        logging.info('\nReading NetCDF')
        logging.debug(f'  {nldas_nc}')
        nldas_nc_f = netCDF4.Dataset(nldas_nc, 'r')
        elev_ma = nldas_nc_f.variables['NLDAS_elev'][0, :, :]
        elev_array = np.flip(elev_ma.data.astype(np.float32), 0)
        elev_nodata = float(elev_ma.fill_value)
        nldas_nc_f.close()

        # Set masked/nodata/ocean pixels to 0
        if zero_elev_nodata_flag:
            # elev_array[
            #     (elev_array == elev_ma.fill_value) |
            #     (elev_array <= -1000)] = np.nan
            elev_array[np.isnan(elev_array)] = 0

        logging.info('\nSaving elevation tif')
        logging.debug(f'  {elev_local_tif}')
        output_ds = rasterio.open(
            elev_local_tif, 'w', count=1, driver='GTiff', tiled=True,
            crs=output_proj, dtype=rasterio.float32,
            height=elev_array.shape[0], width=elev_array.shape[1],
            nodata=nodata_value, transform=output_geo,
            compress='deflate',
            # compress='lzw',
            # compress='deflate', predictor=2,
            # compress='lzw', predictor=1,
        )
        output_ds.write(elev_array, 1)
        output_ds.close()

        logging.info('\nUploading to bucket')
        logging.debug(f'  {elev_bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(elev_bucket_path)}')
        # blob = bucket.blob(os.path.basename(elev_bucket_path))
        blob.upload_from_filename(elev_local_tif)

        # For now, assume the file is in the bucket at this point
        logging.info('\nIngesting into Earth Engine')
        logging.debug(f'  {elev_asset_id}')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        params = {
            'name': elev_asset_id,
            'bands': [
                {'id': elev_band_name, 'tilesetId': 'image', 'tilesetBandIndex': 0}
            ],
            'tilesets': [
                {
                    'id': 'image',
                    'sources': [{'uris': [elev_bucket_path]}]
                }
            ],
            'properties': {
                'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
            },
            # 'startTime': '2000-01-01T00:00:00' + '.000000000Z',
            # 'pyramidingPolicy': 'MEAN',
            # 'missingData': {'values': [nodata_value]},
        }
        ee.data.startIngestion(task_id, params, allow_overwrite=True)

        # Hold until the elevation asset is ready
        input('Press ENTER after the elevation asset has ingested')

        # # Remove the file from the bucket
        # logging.info('\nRemoving from bucket')
        # if blob and blob.exists():
        #     blob.delete()

        # # Make the asset public
        # policy = {
        #     "all_users_can_read": true,
        #     "owners": ["group:openet@googlegroups.com"],
        #     "readers": [],
        #     "writers": [],
        # }
        # ee.data.setIamPolicy(elev_asset_id, policy)

        # Cleanup
        try:
            os.remove(elev_local_tif)
        except:
            pass


    if not ee.data.getInfo(aspect_asset_id):
        logging.info('\nReading NetCDF')
        logging.debug(f'  {nldas_nc}')
        nldas_nc_f = netCDF4.Dataset(nldas_nc, 'r')
        aspect_ma = nldas_nc_f.variables['NLDAS_aspect'][0, :, :]
        aspect_array = np.flip(aspect_ma.data.astype(np.float32), 0)
        aspect_nodata = float(aspect_ma.fill_value)
        nldas_nc_f.close()

        # # Set masked/nodata/ocean pixels to 0
        # if zero_elev_nodata_flag:
        #     # aspect_array[
        #     #     (aspect_array == aspect_ma.fill_value) |
        #     #     (aspect_array <= -1000)] = np.nan
        #     aspect_array[np.isnan(slope_array)] = 0

        logging.info('\nSaving aspect tif')
        logging.debug(f'  {aspect_local_tif}')
        output_ds = rasterio.open(
            aspect_local_tif, 'w', count=1, driver='GTiff', tiled=True,
            crs=output_proj, dtype=rasterio.float32,
            height=aspect_array.shape[0], width=aspect_array.shape[1],
            nodata=nodata_value, transform=output_geo,
            compress='deflate',
            # compress='lzw',
            # compress='deflate', predictor=2,
            # compress='lzw', predictor=1,
        )
        output_ds.write(aspect_array, 1)
        output_ds.close()

        logging.info('\nUploading to bucket')
        logging.debug(f'  {aspect_bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(aspect_bucket_path)}')
        # blob = bucket.blob(os.path.basename(aspect_bucket_path))
        blob.upload_from_filename(aspect_local_tif)

        # For now, assume the file is in the bucket at this point
        logging.info('\nIngesting into Earth Engine')
        logging.debug(f'  {aspect_asset_id}')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        params = {
            'name': aspect_asset_id,
            'bands': [
                {'id': aspect_band_name, 'tilesetId': 'image', 'tilesetBandIndex': 0}
            ],
            'tilesets': [
                {
                    'id': 'image',
                    'sources': [{'uris': [aspect_bucket_path]}]
                }
            ],
            'properties': {
                'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
            },
            # 'startTime': '2000-01-01T00:00:00' + '.000000000Z',
            # 'pyramidingPolicy': 'MEAN',
            # 'missingData': {'values': [nodata_value]},
        }
        ee.data.startIngestion(task_id, params, allow_overwrite=True)

        # # Remove the file from the bucket
        # logging.info('\nRemoving from bucket')
        # if blob and blob.exists():
        #     blob.delete()

        # # Make the asset public
        # policy = {
        #     "all_users_can_read": true,
        #     "owners": ["group:openet@googlegroups.com"],
        #     "readers": [],
        #     "writers": [],
        # }
        # ee.data.setIamPolicy(aspect_asset_id, policy)

        # Cleanup
        try:
            os.remove(aspect_local_tif)
        except:
            pass


    if not ee.data.getInfo(slope_asset_id):
        logging.info('\nReading NetCDF')
        logging.debug(f'  {nldas_nc}')
        nldas_nc_f = netCDF4.Dataset(nldas_nc, 'r')
        slope_ma = nldas_nc_f.variables['NLDAS_slope'][0, :, :]
        slope_array = np.flip(slope_ma.data.astype(np.float32), 0)
        slope_nodata = float(slope_ma.fill_value)
        nldas_nc_f.close()

        # # Set masked/nodata/ocean pixels to 0
        # if zero_elev_nodata_flag:
        #     # slope_array[
        #     #     (slope_array == slope_ma.fill_value) |
        #     #     (slope_array <= -1000)] = np.nan
        #     slope_array[np.isnan(slope_array)] = 0

        logging.info('\nSaving slope tif')
        logging.debug(f'  {slope_local_tif}')
        output_ds = rasterio.open(
            slope_local_tif, 'w', count=1, driver='GTiff', tiled=True,
            crs=output_proj, dtype=rasterio.float32,
            height=slope_array.shape[0], width=slope_array.shape[1],
            nodata=nodata_value, transform=output_geo,
            compress='deflate',
            # compress='lzw',
            # compress='deflate', predictor=2,
            # compress='lzw', predictor=1,
        )
        output_ds.write(slope_array, 1)
        output_ds.close()

        logging.info('\nUploading to bucket')
        logging.debug(f'  {slope_bucket_path}')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(slope_bucket_path)}')
        # blob = bucket.blob(os.path.basename(slope_bucket_path))
        blob.upload_from_filename(slope_local_tif)

        # For now, assume the file is in the bucket at this point
        logging.info('\nIngesting into Earth Engine')
        logging.debug(f'  {slope_asset_id}')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        params = {
            'name': slope_asset_id,
            'bands': [
                {'id': slope_band_name, 'tilesetId': 'image', 'tilesetBandIndex': 0}
            ],
            'tilesets': [
                {
                    'id': 'image',
                    'sources': [{'uris': [slope_bucket_path]}]
                }
            ],
            'properties': {
                'date_ingested': datetime.datetime.today().strftime('%Y-%m-%d'),
            },
            # 'startTime': '2000-01-01T00:00:00' + '.000000000Z',
            # 'pyramidingPolicy': 'MEAN',
            # 'missingData': {'values': [nodata_value]},
        }
        ee.data.startIngestion(task_id, params, allow_overwrite=True)

        # # Remove the file from the bucket
        # logging.info('\nRemoving from bucket')
        # if blob and blob.exists():
        #     blob.delete()

        # # Make the asset public
        # policy = {
        #     "all_users_can_read": true,
        #     "owners": ["group:openet@googlegroups.com"],
        #     "readers": [],
        #     "writers": [],
        # }
        # ee.data.setIamPolicy(slope_asset_id, policy)

        # Cleanup
        try:
            os.remove(slope_local_tif)
        except:
            pass

    # # Cleanup
    # try:
    #     os.remove(nldas_nc)
    # except:
    #     pass


    # Build the latitude and longitude assets in EE from the elevation asset
    if not ee.data.getInfo(lat_asset_id):
        logging.info('\nExporting latitude asset')
        logging.debug(f'  {lat_asset_id}')
        lat_img = (
            ee.Image(elev_asset_id)
            .multiply(0).add(ee.Image.pixelLonLat().select('latitude'))
            .rename([lat_band_name])
        )
        task = ee.batch.Export.image.toAsset(
            image=lat_img,
            description='openet_nldas_latitude_asset',
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
            .multiply(0).add(ee.Image.pixelLonLat().select('longitude'))
            .rename([lon_band_name])
        )
        task = ee.batch.Export.image.toAsset(
            image=lon_img,
            description='openet_nldas_longitude_asset',
            assetId=lon_asset_id,
            dimensions='{}x{}'.format(*output_shape),
            crs=output_proj,
            crsTransform=output_geo,
        )
        task.start()


    # # Build the slope and aspect assets in EE from the elevation asset
    # if not ee.data.getInfo(slope_asset_id):
    #     logging.info('\nExporting slope asset')
    #     logging.debug(f'  {slope_asset_id}')
    #     task = ee.batch.Export.image.toAsset(
    #         image=ee.Terrain.slope(ee.Image(elev_asset_id)).rename('slope'),
    #         description='openet_nldas_slope_asset',
    #         assetId=slope_asset_id,
    #         dimensions='{}x{}'.format(*output_shape),
    #         crs=output_proj,
    #         crsTransform=output_geo,
    #     )
    #     task.start()
    #
    # if not ee.data.getInfo(aspect_asset_id):
    #     logging.info('\nExporting aspect asset')
    #     logging.debug(f'  {aspect_asset_id}')
    #     task = ee.batch.Export.image.toAsset(
    #         image=ee.Terrain.aspect(ee.Image(elev_asset_id)).rename('aspect'),
    #         description='openet_nldas_aspect_asset',
    #         assetId=aspect_asset_id,
    #         dimensions='{}x{}'.format(*output_shape),
    #         crs=output_proj,
    #         crsTransform=output_geo,
    #     )
    #     task.start()



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
        description='Ingest NLDAS ancillary assets into Earth Engine',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--project', type=str, required=True, help='Earth Engine Project ID')
    parser.add_argument(
        '--zero', default=False, action='store_true',
        help='Set elevation nodata values to 0')
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
        zero_elev_nodata_flag=args.zero,
        overwrite_flag=args.overwrite,
    )
