import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import pprint
import requests
from time import sleep

import ee
from google.cloud import storage
import numpy as np
import rasterio
import rasterio.crs
import rasterio.warp

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('rasterio').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

ASSET_FOLDER = 'projects/openet/assets/meteorology/urma/hawaii/ancillary'
BUCKET_NAME = 'openet'
BUCKET_FOLDER = 'urma/hawaii/ancillary'
PROJECT_NAME = 'openet'
SOURCE_URL = 'https://noaa-urma-pds.s3.amazonaws.com'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)
TODAY_DT = datetime.now(timezone.utc)



def main(project_id, workspace='/tmp', overwrite_flag=False):
    """Build URMA Hawaii ancillary assets

    Parameters
    ----------
    project_id : str
        Earth Engine project ID.
    workspace : str
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).

    Returns
    -------
    None

    """
    logging.info('\nBuild URMA Hawaii ancillary assets')

    region = 'hawaii'
    region_ws = os.path.join(workspace, region)
    if not os.path.isdir(region_ws):
        os.makedirs(region_ws)
    land_mask_path = os.path.join(region_ws, 'hi_land_mask.tif')
    elevation_path = os.path.join(region_ws, 'hi_elevation.tif')
    latitude_path = os.path.join(region_ws, 'hi_latitude.tif')
    longitude_path = os.path.join(region_ws, 'hi_longitude.tif')

    land_mask_asset_id = f'{ASSET_FOLDER}/land_mask'
    elevation_asset_id = f'{ASSET_FOLDER}/elevation'
    latitude_asset_id = f'{ASSET_FOLDER}/latitude'
    longitude_asset_id = f'{ASSET_FOLDER}/longitude'
    #slope_asset_id = f'{ASSET_FOLDER}/slope'
    #aspect_asset_id = f'{ASSET_FOLDER}/aspect'

    # Hardcoding the shape parameters for now
    width, height = 321, 225
    gee_transform = [2500, 0, -16879375, 0, -2500, 2481825 - (6 * 2500)]
    grb_transform = [2500, 0, -16879374.0603126622736454, 0, -2500, 2481825.9654569458216429]
    wkt = (
        'PROJCS["unnamed", '
        '  GEOGCS["Coordinate System imported from GRIB file", \n'
        #'    DATUM["unnamed", SPHEROID["WGS 84",6378137,298.257223563]], \n'
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
    #     #'DATUM[\"unnamed\",SPHEROID[\"WGS 84\",6378137,0]],PRIMEM[\"Greenwich\",0],'
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

    ee.Initialize(project=project_id)

    # Process the Land Mask first
    #if overwrite_flag or not os.path.isfile(land_mask_path):
    logging.info('\nBuilding land mask asset')
    land_mask_grb = os.path.join('masks', 'hiurma_slmask.grb2')
    # land_mask_grb = os.path.join('masks', 'nam_smartmaskhi.grb2')

    # logging.debug('Reading grib file')
    land_mask_grb_ds = rasterio.open(land_mask_grb, 'r')

    # logging.debug('Reading hourly arrays')
    land_mask_array = land_mask_grb_ds.read(1)
    # Set the ocean to the nodata value and convert to uint8
    land_mask_array[land_mask_array == 2] = 0
    land_mask_array = land_mask_array.astype(np.uint8)
    land_mask_grb_ds = None

    # logging.info('Building output GeoTIFF')
    land_mask_ds = rasterio.open(
        land_mask_path, 'w', driver='GTiff', nodata=0, count=1, dtype=rasterio.uint8,
        height=height, width=width, crs=crs, transform=gee_transform,
        compress='lzw', tiled=True, blockxsize=512, blockysize=512,
    )
    land_mask_ds.set_band_description(1, 'land_mask')
    land_mask_ds.write(land_mask_array, 1)
    land_mask_ds = None

    # # logging.debug('Building overviews')
    # dst = rasterio.open(land_mask_path, 'r+')
    # dst.build_overviews([2, 4], rasterio.warp.Resampling.average)
    # dst.update_tags(ns='rio_overview', resampling='average')
    # dst.close()

    if overwrite_flag or not ee.data.getInfo(land_mask_asset_id):
        if ee.data.getInfo(land_mask_asset_id):
            ee.data.deleteAsset(land_mask_asset_id)

        # logging.info('Uploading to bucket')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{os.path.basename(land_mask_path)}'
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(land_mask_path)}')
        blob.upload_from_filename(land_mask_path)

        # logging.info('Ingesting into Earth Engine')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        properties = {
            'date_ingested': f'{TODAY_DT.strftime("%Y-%m-%d")}',
            'source': os.path.basename(land_mask_grb),
        }
        params = {
            'name': land_mask_asset_id,
            'bands': [{'id': 'land_mask', 'tilesetId': 'image', 'tilesetBandIndex': 0}],
            'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
            # 'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}], 'crs': tif_crs}],
            'properties': properties,
            'pyramiding_policy': 'MODE',
            # 'startTime': datetime(2025, 1, 1).isoformat() + '.000000000Z',
            # 'missingData': {'values': [nodata_value]},

        }
        # # TODO: Wrap in a try/except loop
        ee.data.startIngestion(task_id, params, allow_overwrite=True)


    # Get the elevation from one of the data grib files
    #if overwrite_flag or not os.path.isfile(elevation_path):
    logging.info('\nBuilding elevation asset')
    grb_fmt = 'hiurma.t{hour:02d}z.2dvaranl_ndfd.grb2'
    grb_file = grb_fmt.format(hour=18)
    grb_url = f'{SOURCE_URL}/hiurma.20240701/{grb_file}'
    grb_path = os.path.join(region_ws, grb_file)
    logging.debug(f'  {grb_url}')
    logging.debug(f'  {grb_path}')

    # logging.debug('Downloading grib file')
    if overwrite_flag or not os.path.isfile(grb_path):
        url_download(grb_url, grb_path)

    # logging.debug('Opening grib file')
    grb_ds = rasterio.open(grb_path, 'r')
    for band in range(len(grb_ds.indexes)):
        band_tags = grb_ds.tags(band+1)
        band_name = band_tags['GRIB_ELEMENT']
        if band_name != 'HGT':
            continue
        elev_units = band_tags['GRIB_UNIT'][1:-1]
        elev_array = grb_ds.read(band+1)
        # elev_array = elev_array.astype(np.float64)

    # logging.debug('\nBuilding output GeoTIFF')
    elev_ds = rasterio.open(
        elevation_path, 'w', driver='GTiff',
        nodata=-9999, count=1, dtype=rasterio.float64,
        height=height, width=width, crs=crs, transform=gee_transform,
        compress='lzw', tiled=True, blockxsize=512, blockysize=512,
    )
    elev_ds.set_band_description(1, 'elevation')
    elev_array[land_mask_array==0] = 0
    elev_ds.write(elev_array, 1)
    del elev_ds

    if overwrite_flag or not ee.data.getInfo(elevation_asset_id):
        if ee.data.getInfo(elevation_asset_id):
            ee.data.deleteAsset(elevation_asset_id)

        # logging.debug('Uploading to bucket')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{os.path.basename(elevation_path)}'
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(elevation_path)}')
        blob.upload_from_filename(elevation_path)

        # logging.debug('Ingesting into Earth Engine')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        properties = {
            'date_ingested': f'{TODAY_DT.strftime("%Y-%m-%d")}',
            'source': grb_file,
        }
        params = {
            'name': elevation_asset_id,
            'bands': [{'id': 'elevation', 'tilesetId': 'image', 'tilesetBandIndex': 0}],
            'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
            # 'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}], 'crs': tif_crs}],
            'properties': properties,
            'pyramiding_policy': 'MODE',
            # 'startTime': datetime(2025, 1, 1).isoformat() + '.000000000Z',
            # 'missingData': {'values': [nodata_value]},

        }
        # TODO: Wrap in a try/except loop
        ee.data.startIngestion(task_id, params, allow_overwrite=True)


    logging.info('\nBuilding longitude/latitude asset')
    # if overwrite_flag or not os.path.isfile(latitude_path) or not os.path.isfile(longitude_path):

    # Snap the projected GCS transform and recompute shape
    gcs_cs = 0.01
    xmin = -163
    ymin = 17
    xmax = -153
    ymax = 24
    gcs_transform = [gcs_cs, 0.00, xmin, 0.00, -gcs_cs, ymax]
    gcs_cols = int(round(abs((xmin - xmax) / gcs_cs), 0))
    gcs_rows = int(round(abs((ymax - ymin) / -gcs_cs), 0))

    # Build the GCS lat/lon arrays
    # Cell lat/lon values are measured half a cell in from extent edge
    lon_src_array, lat_src_array = np.meshgrid(
        np.linspace(xmin + 0.5 * gcs_cs, xmax - 0.5 * gcs_cs, gcs_cols),
        np.linspace(ymax - 0.5 * gcs_cs, ymin + 0.5 * gcs_cs, gcs_rows)
    )

    # Generate generic lat/lon grids that cover the GRB extent
    lat_src_path = latitude_path.replace('.tif', '_src.tif')
    lon_src_path = longitude_path.replace('.tif', '_src.tif')
    array_to_geotiff(
        lat_src_array.astype(np.float64), lat_src_path,
        output_geo=gcs_transform, output_crs='EPSG:4326',
        output_nodata=-9999, output_type=rasterio.float64,
    )
    array_to_geotiff(
        lon_src_array.astype(np.float64), lon_src_path,
        output_geo=gcs_transform, output_crs='EPSG:4326',
        output_nodata=-9999, output_type=rasterio.float64,
    )

    # Reproject/interpolate the lat/lon to the GRB grid
    lat_grb_path = latitude_path.replace('.tif', '_grb.tif')
    lon_grb_path = longitude_path.replace('.tif', '_grb.tif')
    reproject(
        src_path=lat_src_path, dst_path=lat_grb_path,
        dst_crs=crs, dst_geo=grb_transform,
        dst_rows=height, dst_cols=width,
        dst_nodata=-9999, dst_type=rasterio.float64,
        dst_resample=rasterio.warp.Resampling.bilinear,
    )
    reproject(
        src_path=lon_src_path, dst_path=lon_grb_path,
        dst_crs=crs, dst_geo=grb_transform,
        dst_rows=height, dst_cols=width,
        dst_nodata=-9999, dst_type=rasterio.float64,
        dst_resample=rasterio.warp.Resampling.bilinear,
    )
    os.remove(lat_src_path)
    os.remove(lon_src_path)

    # Copy the lat/lon grids to new files with the adjusted transform for GEE
    with rasterio.open(lat_grb_path) as lat_ds:
        lat_array = lat_ds.read(1)
    lat_ds = rasterio.open(
        latitude_path, 'w', driver='GTiff',
        nodata=-9999, count=1, dtype=rasterio.float64,
        height=height, width=width, crs=crs, transform=gee_transform,
        compress='lzw', tiled=True, blockxsize=512, blockysize=512,
    )
    lat_ds.set_band_description(1, 'latitude')
    # lat_array[land_mask_array == 0] = 0
    lat_ds.write(lat_array, 1)
    del lat_ds

    with rasterio.open(lon_grb_path) as lon_ds:
        lon_array = lon_ds.read(1)
    lon_ds = rasterio.open(
        longitude_path, 'w', driver='GTiff',
        nodata=-9999, count=1, dtype=rasterio.float64,
        height=height, width=width, crs=crs, transform=gee_transform,
        compress='lzw', tiled=True, blockxsize=512, blockysize=512,
    )
    lon_ds.set_band_description(1, 'longitude')
    # lon_array[land_mask_array == 0] = 0
    lon_ds.write(lon_array, 1)
    del lon_ds

    os.remove(lat_grb_path)
    os.remove(lon_grb_path)

    if overwrite_flag or not ee.data.getInfo(latitude_asset_id):
        if ee.data.getInfo(latitude_asset_id):
            ee.data.deleteAsset(latitude_asset_id)

        # logging.debug('Uploading to bucket')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{os.path.basename(latitude_path)}'
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(latitude_path)}')
        blob.upload_from_filename(latitude_path)

        # logging.debug('Ingesting into Earth Engine')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        properties = {
            'date_ingested': f'{TODAY_DT.strftime("%Y-%m-%d")}',
            # 'source': grb_file,
        }
        params = {
            'name': latitude_asset_id,
            'bands': [{'id': 'latitude', 'tilesetId': 'image', 'tilesetBandIndex': 0}],
            'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
            # 'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}], 'crs': wkt_str}],
            'properties': properties,
            'pyramiding_policy': 'MODE',
            # 'startTime': datetime(2025, 1, 1).isoformat() + '.000000000Z',
            # 'missingData': {'values': [nodata_value]},
        }
        # TODO: Wrap in a try/except loop
        ee.data.startIngestion(task_id, params, allow_overwrite=True)


    if overwrite_flag or not ee.data.getInfo(longitude_asset_id):
        if ee.data.getInfo(longitude_asset_id):
            ee.data.deleteAsset(longitude_asset_id)

        # logging.debug('Uploading to bucket')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{os.path.basename(longitude_path)}'
        blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(longitude_path)}')
        blob.upload_from_filename(longitude_path)

        # logging.debug('Ingesting into Earth Engine')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        properties = {
            'date_ingested': f'{TODAY_DT.strftime("%Y-%m-%d")}',
            # 'source': grb_file,
        }
        params = {
            'name': longitude_asset_id,
            'bands': [{'id': 'longitude', 'tilesetId': 'image', 'tilesetBandIndex': 0}],
            'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
            # 'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}], 'crs': wkt_str}],
            'properties': properties,
            'pyramiding_policy': 'MODE',
            # 'startTime': datetime(2025, 1, 1).isoformat() + '.000000000Z',
            # 'missingData': {'values': [nodata_value]},
        }
        # TODO: Wrap in a try/except loop
        ee.data.startIngestion(task_id, params, allow_overwrite=True)


    # # TODO - Rewrite to generate lat/lon arrays directly from gribs
    # #   Using this approach for now since
    # logging.info('\nWaiting for land_mask asset to ingest')
    # while True:
    #     sleep(20)
    #     if ee.data.getInfo(land_mask_asset_id):
    #         break
    # mask_img = ee.Image(land_mask_asset_id).mask()
    #
    # # Latitude
    # if not ee.data.getInfo(latitude_asset_id):
    #     logging.info('\nLatitude asset')
    #     latitude_img = (
    #         mask_img.multiply(0).add(ee.Image.pixelLonLat().select(['latitude']))
    #         .rename('latitude')
    #         .set({
    #             'build_date': datetime.today().strftime('%Y-%m-%d'),
    #             'source': land_mask_asset_id,
    #         })
    #     )
    #     export_task = ee.batch.Export.image.toAsset(
    #         image=latitude_img,
    #         description='openet_urma_hawaii_latitude_asset',
    #         assetId=latitude_asset_id,
    #         dimensions=f'{width}x{height}',
    #         crs=crs,
    #         crsTransform='[' + ', '.join(map(str, transform)) + ']',
    #     )
    #     logging.info('  Starting task')
    #     export_task.start()
    #
    # # Longitude
    # if not ee.data.getInfo(longitude_asset_id):
    #     logging.info('\nLongitude asset')
    #     longitude_img = (
    #         mask_img.multiply(0).add(ee.Image.pixelLonLat().select(['longitude']))
    #         .rename('longitude')
    #         .set({
    #             'build_date': datetime.today().strftime('%Y-%m-%d'),
    #             'source': land_mask_asset_id,
    #         })
    #     )
    #     export_task = ee.batch.Export.image.toAsset(
    #         image=longitude_img,
    #         description='openet_urma_hawaii_longitude_asset',
    #         assetId=longitude_asset_id,
    #         dimensions=f'{width}x{height}',
    #         crs=crs,
    #         crsTransform='[' + ', '.join(map(str, transform)) + ']',
    #     )
    #     logging.info('  Starting task')
    #     export_task.start()


def array_to_geotiff(
        output_array,
        output_path,
        output_geo,
        output_crs,
        output_nodata,
        output_type=rasterio.float32
):
    """Save NumPy array as a geotiff

    Parameters
    ----------
    output_array : np.array
    output_path : str
        GeoTIFF file path.
    output_shape : tuple or list of ints
        Image shape (rows, cols).
    output_geo : tuple or list of floats
        Geo-transform (xmin, cs, 0, ymax, 0, -cs).
    output_crs : str
        Projection Well Known Text (WKT) string.
    output_nodata : float
        GeoTIFF nodata value.
    output_type : str
        RasterIO data type (the default is float32).

    Returns
    -------
    None

    Notes
    -----
    There is no checking of the output_path file extension or that the
    output_array is 2d (1 band).

    """
    output_ds = rasterio.open(
        output_path, 'w', driver='GTiff', nodata=output_nodata,
        width=output_array.shape[1], height=output_array.shape[0], count=1,
        dtype=output_type, crs=output_crs, transform=output_geo,
        compress='deflate', tiled=True,
        # compress='deflate', tiled=True, predictor=2,
        # compress='lzw', tiled=True, predictor=1,
    )
    output_ds.write(output_array, 1)
    output_ds.close()


def reproject(
        src_path,
        dst_path,
        dst_crs,
        dst_geo,
        dst_rows,
        dst_cols,
        dst_nodata,
        dst_type,
        dst_resample
):
    """https://rasterio.readthedocs.io/en/latest/topics/reproject.html"""
    with rasterio.open(src_path) as src:
        # transform, width, height = rasterio.warp.calculate_default_transform(
        #     src.crs, dst_crs, src.width, src.height, *src.bounds)
        kwargs = src.meta.copy()
        kwargs.update({
            'driver': 'GTiff',
            'crs': dst_crs,
            'transform': dst_geo,
            'width': dst_cols,
            'height': dst_rows,
            'compress': 'deflate',
            'tiled': True,
            'nodata': dst_nodata,
            'dtype': dst_type,
        })

        with rasterio.open(dst_path, 'w', **kwargs) as dst:
            # for i in range(1, src.count + 1):
            rasterio.warp.reproject(
                source=rasterio.band(src, 1),
                destination=rasterio.band(dst, 1),
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=dst_geo,
                dst_crs=dst_crs,
                resampling=dst_resample,
            )


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


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Build URMA Hawaii ancillary assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--project', type=str, required=True, help='Earth Engine Project ID')
    parser.add_argument(
        '--workspace', metavar='PATH',
        default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
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

    main(
        project_id=args.project,
        workspace=args.workspace,
        overwrite_flag=args.overwrite,
    )
