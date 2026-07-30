import argparse
import logging
import pprint

import ee
import pandas as pd
import openet.core.utils as utils
import refet

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

ASSET_COLL_ID = 'projects/openet/assets/meteorology/raws/saipan/daily'
ASSET_DT_FMT = '%Y%m%d'
PROJECT_NAME = 'openet'


def main(start_dt, end_dt, overwrite_flag=False):
    """"""

    logging.info('Ingest Saipan RAWS time series file as a daily image collection\n')

    ######

    # TODO: Make these input parameters at some point
    data_path = 'WRCC_AMMENPSSaipan_RAWS_Data_C.csv'

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

    extent = [145.45, 14.75, 145.90, 15.35]
    transform = [0.01, 0, extent[0], 0, -0.01, extent[3]]
    shape = [45, 60]
    # shape = [
    #     round(abs(extent[2] - extent[0]) / transform[0]),
    #     round(abs(extent[3] - extent[1]) / transform[0])
    # ]
    # print(shape)
    crs = 'EPSG:4326'

    coll_id = 'projects/openet/assets/meteorology/raws/saipan/daily'

    ######

    logging.debug(f'{data_path}')
    logging.debug(f'{coll_id}')

    # Build the image collection if it doesn't exist
    logging.debug(f'Image Collection: {coll_id}'.format())
    if not ee.data.getInfo(coll_id.rsplit('/', 1)[0]):
        utils.build_parent_folders(coll_id, set_public=True)
    if not ee.data.getInfo(coll_id):
        logging.info('\nImage collection does not exist and will be built'
                     '\n  {}'.format(coll_id))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, coll_id)
        ee.data.setIamPolicy(coll_id, {'bindings': [{'role': 'roles/viewer', 'members': ['allUsers']}]})

    # Read the CSV file
    data_df = pd.read_csv(data_path, skiprows=[1], na_values='M')

    # Compute the UTC datetime
    data_df['DATETIME'] = pd.to_datetime(data_df['DATE'] + ' ' + data_df['TIME'])
    data_df['DATETIME'] = data_df['DATETIME'].dt.tz_localize(timezone)
    data_df['DATETIME'] = data_df['DATETIME'].dt.tz_convert('UTC')

    #
    for row_i, row in data_df.iterrows():
        asset_date = row["DATETIME"].strftime("%Y%m%d%H")

        if row['DATETIME'].strftime('%Y-%m-%d') < start_dt.strftime('%Y-%m-%d'):
            continue
        elif row['DATETIME'].strftime('%Y-%m-%d') >= end_dt.strftime('%Y-%m-%d'):
            continue

        asset_id = f'{coll_id}/{asset_date}'
        if not overwrite_flag and ee.data.getInfo(asset_id):
            logging.info(f'{asset_date} - Image already exists, skipping')
            continue

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

        image = (
            ee.Image.constant([
                row['TEMPERATURE'],
                row['RH'],
                q,
                ea_kpa * 1000,  # Convert to Pascals
                row['WIND'],
                row['PRESSURE'] * 100,  # Convert to Pascals
                row['RS'],
                row['PRECIP'],
                refet_obj.etsz('eto')[0],
                refet_obj.etsz('etr')[0],
            ])
            .rename([
                'TEMPERATURE',
                'RH',
                'SPFH',
                'EA',
                'WIND',
                'PRESSURE',
                'RS',
                'PRECIP',
                'ETO',
                'ETR',
            ])
            .set({
                'date': row['DATETIME'].strftime('%Y-%m-%d'),
                'doy': row['DATETIME'].strftime('%j'),
                'hour': row['DATETIME'].strftime('%H'),
                'elevation': elevation,
                'latitude': latitude,
                'longitude': longitude,
                'source': data_path,
                'timezone': timezone,
                'system:time_start': ee.Date(row['DATETIME']).millis(),
                'units_TEMPERATURE': 'C',
                'units_RH': '%',
                'units_SPFH': 'kg kg-1',
                'units_EA': 'Pa',
                'units_WIND': 'm s-1',
                'units_PRESSURE': 'Pa',
                'units_RS': 'W m-2',
                'units_PRECIP': 'mm',
                'units_ETO': 'mm',
                'units_ETR': 'mm',
                'wind_height': wind_height,
            })
        )

        task = ee.batch.Export.image.toAsset(
            image=image,
            description=f'raws_saipan_meteo_daily_{asset_date}',
            assetId=asset_id,
            dimensions=shape,
            crs=crs,
            crsTransform=transform,
            # crsTransform='[' + ', '.join(map(str, transform)) + ']',
            overwrite=overwrite_flag,
        )
        try:
            task.start()
            logging.info(f'{asset_date} - export task started')
            logging.debug(f'  {task.id}')
        except Exception as e:
            logging.info(f'{asset_date} - task not started')
            print(e)


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest Saipan RAWS time series file as an daily image collection',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
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
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    # # Convert relative paths to absolute paths
    # if args.data and os.path.isdir(os.path.abspath(args.data)):
    #     args.data = os.path.abspath(args.data)

    return args

if __name__ == "__main__":
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    logging.info('\nInitializing Earth Engine using project ID')
    ee.Initialize(project=args.project)

    # Build the image collection if it doesn't exist
    logging.debug(f'Image Collection: {ASSET_COLL_ID}'.format())
    if not ee.data.getInfo(ASSET_COLL_ID.rsplit('/', 1)[0]):
        utils.build_parent_folders(ASSET_COLL_ID, set_public=True)
    if not ee.data.getInfo(ASSET_COLL_ID):
        logging.info('\nImage collection does not exist and will be built'
                     '\n  {}'.format(ASSET_COLL_ID))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, ASSET_COLL_ID)
        ee.data.setIamPolicy(ASSET_COLL_ID, {'bindings': [{'role': 'roles/viewer', 'members': ['allUsers']}]})

    main(start_dt=args.start, end_dt=args.end, overwrite_flag=args.overwrite)
