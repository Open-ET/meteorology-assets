import argparse
import logging
import math
import pprint

import ee
import pandas as pd
import openet.core.utils as utils

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)


def main(overwrite_flag=False):
    """"""
    logging.info('Build meteorology time series image collection\n')

    # TODO: Make these input parameters at some point
    data_path = 'WRCC_AMMENPSSaipan_RAWS_Data.csv'

    coll_id = 'projects/openet/assets/meteorology/raws/saipan/hourly'
    # coll_id = 'projects/openet/assets/meteorology/raws/saipan/daily'

    logging.debug(f'{data_path}')
    logging.debug(f'{coll_id}')

    start_date = '2024-07-01'
    end_date = '2024-07-02'
    extent = [145.45, 14.75, 145.90, 15.35]
    transform = [0.01, 0, extent[0], 0, -0.01, extent[3]]
    shape = [45, 60]
    # shape = [
    #     round(abs(extent[2] - extent[0]) / transform[0]),
    #     round(abs(extent[3] - extent[1]) / transform[0])
    # ]
    print(shape)
    crs = 'EPSG:4326'

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

    data_df = pd.read_csv(data_path, skiprows=[1], na_values='M')
    # data_df['DATETIME'] = pd.to_datetime(data_df['DATE'] + ' ' + data_df['TIME'])
    print(data_df.head())
    input('ENTER')

    for row_i, row in data_df.iterrows():
        if row['DATETIME'].strftime('%Y-%m-%d') < start_date:
            continue
        elif row['DATETIME'].strftime('%Y-%m-%d') >= start_date:
            continue

        # Compute vapor pressure from temperature and relative humidity
        ea_kpa = row['RH'] * 0.6108 * math.exp(17.27 * row['TEMP'] / (row['TEMP'] + 237.3))

        # Then compute specific humidity from vapor pressure and air pressure
        #   with Pair converted from mbar to kPa
        pair_kpa = row['PRES'] * 100 / 1000
        q = 0.622 * ea_kpa / (pair_kpa - 0.378 * ea_kpa)

        image = (
            ee.Image.constant([
                row['TEMP'],
                row['RH'],
                # tdew
                q,
                # ea_kpa * 1000,  # Convert to Pasacals
                row['WIND'],
                row['PRES'] * 100,  # Convert to Pascals
                row['RS'],
                row['PPT'],
            ])
            .rename([
                'TEMP',
                'RH',
                # 'DPT',
                'SPFH',
                # 'EA',
                'WIND',
                'PRES',
                'RS',
                'PPT',
            ])
            .set({
                'date': row['DATETIME'].strftime('%Y-%m-%d'),
                'hour': row['DATETIME'].strftime('%h'),
                'source': data_path,
                'system:time_start': ee.Date(row['DATETIME']).millis(),
                'units_TEMP': 'Deg C',
                'units_RH': '%',
                # 'units_DPT': 'Deg C',
                'units_SPFH': 'kg kg-1',
                # 'units_EA': 'Pa',
                'units_WIND': 'm s-1',
                'units_PRES': 'Pa',
                'units_RS': 'W m-2',
                'units_PPT': 'mm',
            })
        )
        pprint.pprint(image.getInfo())

        asset_date = row["DATETIME"].strftime("%Y%m%d%h")
        asset_id = f'{coll_id}/{asset_date}'
        if not overwrite_flag and ee.data.getInfo(asset_id):
            logging.info(f'{asset_date} - Image already exists, skipping')
            continue

        # export_task = ee.batch.Export.image.toAsset(
        #     image=image,
        #     description='openet_rtma_elevation_asset',
        #     assetId=asset_id,
        #     dimensions=shape,
        #     crs=crs,
        #     crsTransform=transform,
        #     # crsTransform='[' + ', '.join(map(str, transform)) + ']',
        #     overwrite=overwrite_flag,
        # )
        # logging.info('  Starting task')
        # export_task.start()
        break


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Ingest meteorology time series file as an image collection',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument(
    #     '--data', required=True, metavar='PATH',
    #     help='Meteorology time series file path')
    # parser.add_argument(
    #     '--timestep', default='hourly', choices=['hourly'],
    #     help='Meteorology timestep')
    # parser.add_argument(
    #     '--variables', default='hourly',
    #     help='Meteorology variables')
    # parser.add_argument(
    #     '--crs', required=True, type=str,
    #     help='Coordinate reference system')
    # parser.add_argument(
    #     '--shape', required=True,
    #     help='Image shape (width, height)')
    # parser.add_argument(
    #     '--transform', required=True,
    #     help='CRS transform')
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

    ee.Initialize(project=args.project)

    main()
    # main(data_path=args.data)
