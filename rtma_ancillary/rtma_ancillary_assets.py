import argparse
import datetime
import logging

import ee

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.ERROR)

PROJECT_NAME = 'openet'
ASSET_FOLDER = 'projects/openet/assets/meteorology/rtma/ancillary'


def main(project_id, overwrite_flag=False):
    """Build RTMA ancillary assets

    Parameters
    ----------
    project_id : str
        Earth Engine project ID.
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).

    Returns
    -------
    None

    """
    logging.info('\nBuild RTMA ancillary assets\n')

    ee.Initialize(project=project_id)

    land_mask_asset_id = f'{ASSET_FOLDER}/land_mask'
    elevation_asset_id = f'{ASSET_FOLDER}/elevation'
    latitude_asset_id = f'{ASSET_FOLDER}/latitude'
    longitude_asset_id = f'{ASSET_FOLDER}/longitude'
    slope_asset_id = f'{ASSET_FOLDER}/slope'
    aspect_asset_id = f'{ASSET_FOLDER}/aspect'

    src_date = '2019-07-01'
    src_hour = '18'
    src_img_id = f'NOAA/NWS/RTMA/{src_date.replace("-", "")}{src_hour}'
    src_img = ee.Image(src_img_id)

    # The spatial grid for assets starting on 2018-12-05 is slightly larger
    # Intentionally using the larger grid for the ancillary assets
    # if src_date <= '2018-12-04':
    # src_size_str = '2145x1377'
    # src_geo = [2539.703, 0, -2764486.9281005403, 0, -2539.703, 3232110.5100932177]
    # else:
    src_size_str = '2345x1597'
    src_geo = [2539.703, 0, -3272417.1397942575, 0, -2539.703, 3790838.3367873137]

    # This CRS WKT is modified from the GRIB2 CRS to align the images in EE
    src_crs = (
        'PROJCS["NWS CONUS", \n'
        '  GEOGCS["WGS 84", \n'
        '    DATUM["World Geodetic System 1984", \n'
        '      SPHEROID["WGS 84", 6378137.0, 298.257223563, '
        'AUTHORITY["EPSG","7030"]], \n'
        '      AUTHORITY["EPSG","6326"]], \n'
        '    PRIMEM["Greenwich", 0.0, AUTHORITY["EPSG","8901"]], \n'
        '    UNIT["degree", 0.017453292519943295], \n'
        '    AXIS["Geodetic longitude", EAST], \n'
        '    AXIS["Geodetic latitude", NORTH], \n'
        '    AUTHORITY["EPSG","4326"]], \n'
        '  PROJECTION["Lambert_Conformal_Conic_1SP"], \n'
        '  PARAMETER["semi_major", 6371200.0], \n'
        '  PARAMETER["semi_minor", 6371200.0], \n'
        '  PARAMETER["central_meridian", -95.0], \n'
        '  PARAMETER["latitude_of_origin", 25.0], \n'
        '  PARAMETER["scale_factor", 1.0], \n'
        '  PARAMETER["false_easting", 0.0], \n'
        '  PARAMETER["false_northing", 0.0], \n'
        '  UNIT["m", 1.0], \n'
        '  AXIS["x", EAST], \n'
        '  AXIS["y", NORTH]]'
    )
    # src_crs = src_img.projection().wkt()

    # Use the elevation image as the source for all the ancillary assets
    elevation_img = (
        src_img.select(['HGT'], ['elevation'])
        .set({
            'build_date': datetime.datetime.today().strftime('%Y-%m-%d'),
            'source': src_img_id,
        })
    )
    mask_img = elevation_img.multiply(0)

    # Remove existing assets if necessary
    if overwrite_flag and ee.data.getInfo(land_mask_asset_id):
        ee.data.deleteAsset(land_mask_asset_id)
    if overwrite_flag and ee.data.getInfo(elevation_asset_id):
        ee.data.deleteAsset(elevation_asset_id)
    if overwrite_flag and ee.data.getInfo(latitude_asset_id):
        ee.data.deleteAsset(latitude_asset_id)
    if overwrite_flag and ee.data.getInfo(longitude_asset_id):
        ee.data.deleteAsset(longitude_asset_id)

    # Elevation
    if not ee.data.getInfo(elevation_asset_id):
        logging.info('\nElevation asset')
        export_task = ee.batch.Export.image.toAsset(
            image=elevation_img,
            description='openet_rtma_elevation_asset',
            assetId=elevation_asset_id,
            dimensions=src_size_str,
            crs=src_crs,
            crsTransform='[' + ', '.join(map(str, src_geo)) + ']',
        )
        logging.info('  Starting task')
        export_task.start()

    # Land Mask
    # Read the RTMA water mask that Thomas ingested and invert to identify land
    if not ee.data.getInfo(land_mask_asset_id):
        logging.info('\nLand mask asset')
        land_mask_img = (
            ee.Image('projects/bor-evap/assets/rtma_water_mask')
            .eq(0).selfMask()
            .rename('land_mask')
            .set({
                'build_date': datetime.datetime.today().strftime('%Y-%m-%d'),
                'source': src_img_id,
            })
        )
        export_task = ee.batch.Export.image.toAsset(
            image=land_mask_img,
            description='openet_rtma_land_mask_asset',
            assetId=land_mask_asset_id,
            dimensions=src_size_str,
            crs=src_crs,
            crsTransform='[' + ', '.join(map(str, src_geo)) + ']',
        )
        logging.info('  Starting task')
        export_task.start()

    # Latitude
    if not ee.data.getInfo(latitude_asset_id):
        logging.info('\nLatitude asset')
        latitude_img = (
            mask_img.add(ee.Image.pixelLonLat().select(['latitude']))
            .rename('latitude')
            .set({
                'build_date': datetime.datetime.today().strftime('%Y-%m-%d'),
                'source': src_img_id,
            })
        )
        export_task = ee.batch.Export.image.toAsset(
            image=latitude_img,
            description='openet_rtma_latitude_asset',
            assetId=latitude_asset_id,
            dimensions=src_size_str,
            crs=src_crs,
            crsTransform='[' + ', '.join(map(str, src_geo)) + ']',
        )
        logging.info('  Starting task')
        export_task.start()

    # Longitude
    if not ee.data.getInfo(longitude_asset_id):
        logging.info('\nLongitude asset')
        longitude_img = (
            mask_img.add(ee.Image.pixelLonLat().select(['longitude']))
            .rename('longitude')
            .set({
                'build_date': datetime.datetime.today().strftime('%Y-%m-%d'),
                'source': src_img_id,
            })
        )
        export_task = ee.batch.Export.image.toAsset(
            image=longitude_img,
            description='openet_rtma_longitude_asset',
            assetId=longitude_asset_id,
            dimensions=src_size_str,
            crs=src_crs,
            crsTransform='[' + ', '.join(map(str, src_geo)) + ']',
        )
        logging.info('  Starting task')
        export_task.start()


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Build RTMA ancillary assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--project', type=str, required=True, help='Earth Engine Project ID')
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
        overwrite_flag=args.overwrite,
    )
