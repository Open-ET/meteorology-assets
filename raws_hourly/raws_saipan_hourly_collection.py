import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import pprint
import time

import ee
from google.cloud import storage
import openet.core.utils as utils
import numpy as np
import pandas as pd
import rasterio
import refet

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('raster').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

ASSET_DT_FMT = '%Y%m%d%H'
BUCKET_NAME = 'openet'
PROJECT_NAME = 'openet'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)
TODAY_DT = datetime.now(timezone.utc)


def main(start_dt, end_dt, overwrite_flag=False, workspace='/tmp', delay=0):
    """"""

    logging.info('Ingest Saipan RAWS time series file as an hourly image collection\n')

    ######

    station_name = 'saipan'

    # TODO: Make these input parameters at some point
    data_path = 'WRCC_AMMENPSSaipan_RAWS_Data_C.csv'

    coll_id = 'projects/openet/assets/meteorology/raws/saipan/hourly'
    bucket_folder = 'raws/saipan/hourly'

    # TODO: Double check wind speed height for Saipan RAWS
    #   RAWS documentation says 6 meters (20 ft),
    #   but it doesn't look like it in the station photos
    wind_height = 2
    # wind_height = 6

    # TODO: Switch to computing reference ET as an image
    latitude = 15.217778
    longitude = 145.717778
    elevation = 8
    timezone = 'Pacific/Guam'

    asset_extent = [145.45, 14.75, 145.90, 15.35]
    asset_geo = [0.01, 0, asset_extent[0], 0, -0.01, asset_extent[3]]
    asset_width = 45
    asset_height = 60
    asset_shape = [asset_width, asset_height]
    # asset_shape = [
    #     round(abs(asset_extent[2] - asset_extent[0]) / asset_geo[0]),
    #     round(abs(asset_extent[3] - asset_extent[1]) / asset_geo[0])
    # ]
    # print(asset_shape)
    asset_crs = 'EPSG:4326'

    variables = [
        'TEMPERATURE',
        'RH',
        'SPFH',
        'EA',
        'PRESSURE',
        'WIND',
        'RS',
        'PRECIP',
        'ETO',
        'ETR',
    ]

    units = {
        'TEMPERATURE': 'C',
        'RH': '%',
        'SPFH': 'kg kg-1',
        'EA': 'Pa',
        'PRESSURE': 'Pa',
        'WIND': 'm s-1',
        'RS': 'W m-2',
        'PRECIP': 'kg m-2',
        'ETO': 'mm',
        'ETR': 'mm',
    }

    ######

    # Build the image collection if it doesn't exist
    logging.debug(f'Image Collection: {coll_id}'.format())
    if not ee.data.getInfo(coll_id.rsplit('/', 1)[0]):
        utils.build_parent_folders(coll_id, set_public=True)
    if not ee.data.getInfo(coll_id):
        logging.info('\nImage collection does not exist and will be built'
                     '\n  {}'.format(coll_id))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, coll_id)
        ee.data.setIamPolicy(
            coll_id,
            {'bindings': [{'role': 'roles/viewer', 'members': ['allUsers']}]}
        )
        asset_id_list = []
    else:
        asset_id_list = utils.get_ee_assets(coll_id, start_dt, end_dt)

    ######

    # Read the CSV file
    logging.debug(f'{data_path}')
    data_df = pd.read_csv(data_path, skiprows=[1], na_values='M')

    # Compute the UTC datetime
    data_df['DATETIME'] = pd.to_datetime(data_df['DATE'] + ' ' + data_df['TIME'])
    data_df['DATETIME'] = data_df['DATETIME'].dt.tz_localize(timezone)
    data_df['DATETIME'] = data_df['DATETIME'].dt.tz_convert('UTC')

    for row_i, row in data_df.iterrows():
        asset_date = row["DATETIME"].strftime(ASSET_DT_FMT)
        asset_id = f'{coll_id}/{asset_date}'

        if start_dt and (row['DATETIME'].strftime('%Y-%m-%d') < start_dt.strftime('%Y-%m-%d')):
            logging.debug(f'{asset_date} - skipping')
            continue
        elif end_dt and (row['DATETIME'].strftime('%Y-%m-%d') >= end_dt.strftime('%Y-%m-%d')):
            logging.debug(f'{asset_date} - skipping')
            continue

        # if not overwrite_flag and ee.data.getInfo(asset_id):
        if not overwrite_flag and (asset_id in asset_id_list):
            logging.debug(f'{asset_date} - Image already exists, skipping')
            continue
        logging.info(f'{asset_date}')

        date_ws = os.path.join(
            workspace, row["DATETIME"].strftime('%Y'), row["DATETIME"].strftime('%Y-%m-%d')
        )
        tif_name = f'{asset_date}.tif'
        upload_path = os.path.join(date_ws, tif_name)
        bucket_path = f'gs://{BUCKET_NAME}/{bucket_folder}/{tif_name}'
        asset_id = f'{coll_id}/{asset_date}'
        logging.debug(f'  {upload_path}')
        logging.debug(f'  {bucket_path}')
        logging.debug(f'  {asset_id}')

        # Always overwrite temporary files if the asset doesn't exist
        # if os.path.isdir(date_ws):
        #     shutil.rmtree(date_ws)
        if not os.path.isdir(date_ws):
            os.makedirs(date_ws)

        # Compute vapor pressure from temperature and relative humidity
        ea_kpa = (row['RH'] / 100) * refet.calcs.sat_vapor_pressure(row['TEMPERATURE'])[0]

        # Then compute specific humidity from vapor pressure and air pressure
        #   with Pair converted from mbar to kPa
        pair_kpa = row['PRESSURE'] * 100 / 1000
        q = 0.622 * ea_kpa / (pair_kpa - 0.378 * ea_kpa)

        # logging.debug('  Computing daily reference ET')
        refet_obj = refet.Hourly(
            tmean=row['TEMPERATURE'],
            ea=ea_kpa,
            rs=row['RS'],
            uz=row['WIND'],
            zw=wind_height,
            elev=elevation,
            lat=latitude,
            lon=longitude,
            doy=int(row['DATETIME'].strftime('%j')),
            time=float(row['DATETIME'].strftime('%H')),
            method='asce',
            input_units={'rs': 'W m-2'}
        )

        # TODO: Compute these at the dataframe level?
        #   Or just make hourly_arrays as a copy of row
        hourly_arrays = {}
        hourly_arrays['TEMPERATURE'] = np.full(asset_shape, row['TEMPERATURE'], np.float32)
        hourly_arrays['RH'] = np.full(asset_shape, row['RH'], np.float32)
        hourly_arrays['SPFH'] = np.full(asset_shape, q, np.float32)
        # Convert to Pascals
        hourly_arrays['EA'] = np.full(asset_shape, ea_kpa * 1000, np.float32)
        # Convert to Pascals
        hourly_arrays['PRESSURE'] = np.full(asset_shape, pair_kpa * 1000, np.float32)
        hourly_arrays['WIND'] = np.full(asset_shape, row['WIND'], np.float32)
        hourly_arrays['RS'] = np.full(asset_shape, row['RS'], np.float32)
        hourly_arrays['PRECIP'] = np.full(asset_shape, row['PRECIP'], np.float32)
        hourly_arrays['ETO'] = np.full(asset_shape, refet_obj.etsz('eto')[0], np.float32)
        hourly_arrays['ETR'] = np.full(asset_shape, refet_obj.etsz('etr')[0], np.float32)

        logging.debug('  Building output GeoTIFF')
        output_ds = rasterio.open(
            upload_path, 'w',
            driver='GTiff',
            nodata=-9999,
            count=len(variables),
            dtype=rasterio.float32,
            height=asset_height,
            width=asset_width,
            crs=asset_crs,
            transform=asset_geo,
            compress='deflate',
            tiled=True,
            blockxsize=512,
            blockysize=512,
        )

        logging.debug('  Writing arrays to output GeoTIFF')
        for band_i, variable in enumerate(variables):
            # logging.debug(f'  {variable}')
            output_ds.set_band_description(band_i + 1, variable)
            data_array = hourly_arrays[variable].astype(np.float32)
            data_array[np.isnan(data_array)] = -9999
            output_ds.write(data_array, band_i + 1)
            del data_array

        # output_ds.close()
        del output_ds

        logging.debug('  Uploading to bucket')
        bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
        blob = bucket.blob(f'{bucket_folder}/{os.path.basename(bucket_path)}')
        blob.upload_from_filename(upload_path)

        # DEADBEEF - For now, assume the file is in the bucket
        logging.info('  Ingesting into Earth Engine')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')

        properties = {
            'date_ingested': f'{TODAY_DT.strftime("%Y-%m-%d")}',
            'date': row['DATETIME'].strftime('%Y-%m-%d'),
            'doy': row['DATETIME'].strftime('%j'),
            'hour': row['DATETIME'].strftime('%H'),
            'elevation': elevation,
            'latitude': latitude,
            'longitude': longitude,
            'source': data_path,
            'timezone': timezone,
        }
        for v in variables:
            if (v in units.keys()) and units[v]:
                properties[f'units_{v}'] = units[v]

        params = {
            'name': asset_id,
            'bands': [
                {'id': v, 'tilesetId': 'image', 'tilesetBandIndex': i}
                for i, v in enumerate(variables)
            ],
            'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
            'properties': properties,
            'startTime': row['DATETIME'].to_pydatetime().replace(tzinfo=None).isoformat() + '.000000000Z',
            # 'pyramiding_policy': 'MEAN',
            # 'missingData': {'values': [nodata_value]},
        }

        # TODO: Wrap in a try/except loop
        try:
            ee.data.startIngestion(task_id, params, allow_overwrite=True)

        except Exception as e:
            logging.info(f'  Ingest task not started')
            continue

        if delay:
            time.sleep(delay)

        # DEADBEEF - Building image locally and uploading instead of as an export call
        # image = (
        #     ee.Image.constant([
        #         row['TEMPERATURE'],
        #         row['RH'],
        #         q,
        #         ea_kpa * 1000,  # Convert to Pascals
        #         row['PRESSURE'] * 100,  # Convert to Pascals
        #         row['WIND'],
        #         row['RS'],
        #         row['PRECIP'],
        #         refet_obj.etsz('eto')[0],
        #         refet_obj.etsz('etr')[0],
        #     ])
        #     .rename([
        #         'TEMPERATURE',
        #         'RH',
        #         'SPFH',
        #         'EA',
        #         'PRESSURE',
        #         'WIND',
        #         'RS',
        #         'PRECIP',
        #         'ETO',
        #         'ETR',
        #     ])
        #     .set({
        #         'date': row['DATETIME'].strftime('%Y-%m-%d'),
        #         'doy': row['DATETIME'].strftime('%j'),
        #         'hour': row['DATETIME'].strftime('%H'),
        #         'elevation': elevation,
        #         'latitude': latitude,
        #         'longitude': longitude,
        #         'source': data_path,
        #         'timezone': timezone,
        #         'system:time_start': ee.Date(row['DATETIME']).millis(),
        #         'units_TEMPERATURE': 'C',
        #         'units_RH': '%',
        #         'units_SPFH': 'kg kg-1',
        #         'units_EA': 'Pa',
        #         'units_PRESSURE': 'Pa',
        #         'units_WIND': 'm s-1',
        #         'units_RS': 'W m-2',
        #         'units_PRECIP': 'mm',
        #         'units_ETO': 'mm',
        #         'units_ETR': 'mm',
        #         'wind_height': wind_height,
        #     })
        # )
        #
        # task = ee.batch.Export.image.toAsset(
        #     image=image,
        #     description=f'raws_saipan_meteo_hourly_{asset_date}',
        #     assetId=asset_id,
        #     dimensions=shape,
        #     crs=crs,
        #     crsTransform=transform,
        #     # crsTransform='[' + ', '.join(map(str, transform)) + ']',
        #     overwrite=overwrite_flag,
        # )
        # try:
        #     task.start()
        #     logging.info(f'{asset_date} - export task started')
        #     logging.debug(f'  {task.id}')
        # except Exception as e:
        #     logging.info(f'{asset_date} - task not started')
        #     print(e)


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest Saipan RAWS time series file as an hourly image collection',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--workspace', metavar='PATH',
        default=os.path.dirname(os.path.abspath(__file__)),
        help='Set the current working directory')
    parser.add_argument(
        '--start', type=utils.arg_valid_date, metavar='DATE',
        help='Start date (format YYYY-MM-DD)')
    parser.add_argument(
        '--end', type=utils.arg_valid_date, metavar='DATE',
        help='End date (format YYYY-MM-DD)')
    # parser.add_argument(
    #     '--data', required=True, metavar='PATH',
    #     help='Meteorology time series file path')
    parser.add_argument(
        '--project', type=str, required=True, help='Earth Engine Project ID')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--reverse', default=False, action='store_true',
        help='Process dates in reverse order')
    parser.add_argument(
        '--delay', type=int, default=0,
        help='Number of seconds to pause between each ingest task')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    # # Convert relative paths to absolute paths
    if args.workspace and os.path.isdir(os.path.abspath(args.workspace)):
        args.workspace = os.path.abspath(args.workspace)
    # if args.data and os.path.isdir(os.path.abspath(args.data)):
    #     args.data = os.path.abspath(args.data)

    return args

if __name__ == "__main__":
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    logging.info('\nInitializing Earth Engine using project ID')
    ee.Initialize(project=args.project)

    main(
        start_dt=args.start,
        end_dt=args.end,
        overwrite_flag=args.overwrite,
        workspace=args.workspace,
    )
