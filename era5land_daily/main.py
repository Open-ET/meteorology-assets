import argparse
from datetime import datetime, timedelta, timezone
import os
import re
import sys
import time

import ee
from flask import abort, Response
from google.cloud import storage
from importlib import metadata
import openet.refetgee

# The full path/ID will be "projects/openet/meteorology/<dataset>/<region>/<timestep>"
ASSET_FOLDER = 'projects/openet/assets/meteorology/era5land'
ASSET_DT_FMT = '%Y%m%d'
# BUCKET_NAME = 'openet_assets'
# BUCKET_FOLDER = 'meteorology/era5land'
PROJECT_NAME = 'openet'
REGIONS = ['global', 'eu', 'na', 'sa', 'hawaii']
SOURCE_COLL_ID = 'ECMWF/ERA5_LAND/HOURLY'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)
START_DAY_OFFSET = 90
END_DAY_OFFSET = 0
NODATA = -9999
TIMESTEP = 'daily'
TODAY_DT = datetime.now(timezone.utc)
# VARIABLES = [
#     'temperature_2m_max',
#     'temperature_2m_min',
#     'dewpoint_temperature_2m',
#     'surface_pressure',
#     'wind_10m',
#     'surface_solar_radiation_downwards',
#     # 'surface_net_solar_radiation',
#     # 'surface_net_thermal_radiation',
#     'total_precipitation',
#     'eto_asce',
#     'etr_asce',
# ]

if 'FUNCTION_REGION' in os.environ:
    # Logging is not working correctly in cloud functions for Python 3.8+
    # Following workflow suggested in this issue:
    # https://issuetracker.google.com/issues/124403972
    import google.cloud.logging
    log_client = google.cloud.logging.Client(project='openet')
    log_client.setup_logging(log_level=20)
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
else:
    import logging
    # logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.getLogger('earthengine-api').setLevel(logging.INFO)
    logging.getLogger('googleapiclient').setLevel(logging.INFO)
    logging.getLogger('requests').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)

if 'FUNCTION_REGION' in os.environ:
    SCOPES = [
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/earthengine',
    ]
    credentials, project_id = google.auth.default(default_scopes=SCOPES)
    ee.Initialize(credentials, project=project_id)
else:
    ee.Initialize(project='ee-cmorton')


def era5land_daily_export(
        tgt_dt,
        region=None,
        refet_timestep='hourly',
        fill_edge_cells=True,
        overwrite_flag=False,
):
    """Export daily ERA5-Land image for a single date

    Parameters
    ----------
    tgt_dt : datetime
    region : {'global', 'eu', 'na', 'sa'}, optional
        Daily image assets can be generated for predefined study areas.
        If not set, the asset will include the full global extent of ERA5-Land.
    refet_timestep : {'daily', 'hourly' (default)}, optional
        Daily Reference ET can be computed at the hourly timestep and summed to the day,
        or at the daily timestep from the aggregated meteorology variables.
    fill_edge_cells : bool, optional
        If True, fill any masked pixels within a one cell buffer of the input image.
        This will fill in small holes and cells along the coasts.
    overwrite_flag : bool, optional
        If True, overwrite existing assets

    Returns
    -------
    str : response string

    """

    var_units = {
        'temperature_2m_max': 'K',
        'temperature_2m_min': 'K',
        'temperature_2m_mean': 'K',
        'dewpoint_temperature_2m': 'K',
        'wind_10m': 'm s-1',
        'surface_solar_radiation_downwards': 'J m-2',
        'total_precipitation': 'm',
        'eto_asce': 'mm',
        'etr_asce': 'mm',
    }

    if not region or (region.lower() == 'global'):
        logging.info(f'Export ERA5-Land {TIMESTEP} image - {tgt_dt.strftime("%Y-%m-%d")}')
        export_name = f'openet_meteo_era5land_{TIMESTEP}_{tgt_dt.strftime("%Y%m%d")}'
        # DEADBEEF - No longer saving ERA5-Land assets as COGs
        # bucket_img = f'{BUCKET_FOLDER}/{TIMESTEP}/{tgt_dt.strftime(ASSET_DT_FMT)}.tif'
        # bucket_json = bucket_img.replace('.tif', '_properties.json')
        asset_id = f'{ASSET_FOLDER}/{TIMESTEP}/{tgt_dt.strftime(ASSET_DT_FMT)}'
    else:
        logging.info(f'Export ERA5-Land {TIMESTEP} image - {region} - '
                     f'{tgt_dt.strftime("%Y-%m-%d")}')
        logging.debug(f'  Region: {region}')
        export_name = f'openet_meteo_era5land_{region}_{TIMESTEP}_{tgt_dt.strftime("%Y%m%d")}'
        # DEADBEEF - No longer saving ERA5-Land assets as COGs
        # bucket_img = f'{BUCKET_FOLDER}/{region}/{TIMESTEP}/{tgt_dt.strftime(ASSET_DT_FMT)}.tif'
        # bucket_json = bucket_img.replace('.tif', '_properties.json')
        asset_id = f'{ASSET_FOLDER}/{region}/{TIMESTEP}/{tgt_dt.strftime(ASSET_DT_FMT)}'
    # logging.info(f'export_id:   {export_name}')
    # logging.info(f'bucket_img:  {bucket_img}')
    # logging.info(f'bucket_json: {bucket_json}')
    # logging.info(f'asset_id:    {asset_id}')

    try:
        asset_info = ee.data.getInfo(asset_id)
    except ee.ee_exception.EEException:
        # DEADBEEF - No longer saving ERA5-Land assets as COGs
        # logging.info('  EEException reading COG backed asset, removing')
        # delete_cog_asset(asset_id, BUCKET_NAME, bucket_img, bucket_json)
        # asset_info = None
        return f'{export_name} - EEException on getInfo call, skipping'
    except:
        return f'{export_name} - Unhandled exception on getInfo call, skipping'

    if asset_info:
        if overwrite_flag:
            logging.info('  Removing existing asset')
            try:
                ee.data.deleteAsset(asset_id)
                # DEADBEEF - No longer saving ERA5-Land assets as COGs
                # delete_cog_asset(asset_id, BUCKET_NAME, bucket_img, bucket_json)
            except Exception as e:
                logging.info('  Error deleting asset, skipping')
                logging.info(f'{e}')
                return f'{export_name} - Error deleting asset, skipping\n{e}'
        else:
            logging.debug('  Asset already exists, skipping')
            return f'{export_name} - Asset already exists, skipping'

    if not region or (region.lower() == 'global'):
        start_hour_offset = 0
        crs = 'EPSG:4326'
        # Setting width to 3601 to match source assets was causing issues
        # Limiting extent since Reference ET equations don't work well above ~67 deg
        width, height = 3600, 1271
        transform = [0.1, 0, -180.05, 0, -0.1, 67.05]
        # width, height = 3600, 1201
        # transform = [0.1, 0, -180.05, 0, -0.1, 60.05]
        # width, height = 3600, 1251
        # transform = [0.1, 0, -180.05, 0, -0.1, 65.05]
        # width, height = 3600, 1301
        # transform = [0.1, 0, -180.05, 0, -0.1, 70.05]
        # width, height = 3600, 1321
        # transform = [0.1, 0, -180.05, 0, -0.1, 72.05]
        # # Clipped Artic water and all Antartica
        # width, height = 3600, 1451
        # transform = [0.1, 0, -180.05, 0, -0.1, 85.05]
        # # Full global
        # width, height = 3600, 1801
        # transform = [0.1, 0, -180.05, 0, -0.1, 90.05]
    elif region.lower() in ['eu']:
        # European Union (not Europe) region
        # Start hour offset should probably be -1,
        #   but having an earlier start time might throw off the filterDate calls
        #   and the difference between 0 and 1 is negligible
        start_hour_offset = 0
        crs = 'EPSG:4326'
        width, height = 461, 326
        transform = [0.1, 0, -11.05, 0, -0.1, 67.05]
    elif region in ['hawaii']:
        start_hour_offset = 10
        crs = 'EPSG:4326'
        width, height = 61, 41
        transform = [0.1, 0, -160.55, 0, -0.1, 22.55]
    elif region.lower() in ['na']:
        # To include southern Greenland, include -33 east (1350 width?)
        start_hour_offset = 6
        crs = 'EPSG:4326'
        width, height = 1160, 601
        transform = [0.1, 0, -168.05, 0, -0.1, 67.05]
    elif region.lower() in ['sa']:
        start_hour_offset = 3
        crs = 'EPSG:4326'
        width, height = 480, 691
        transform = [0.1, 0, -82.05, 0, -0.1, 13.05]
        # Include the Galapagos?
        # width, height = 580, 691
        # transform = [0.1, 0, -92.05, 0, -0.1, 13.05]
    else:
        return f'{export_name} - Unsupported region {region}, skipping'

    extent = [
        transform[2], transform[5] + height * transform[4],
        transform[2] + width * transform[0], transform[5]
    ]
    logging.debug(f'  Projection: {crs}')
    logging.debug(f'  Width:      {width}')
    logging.debug(f'  Height:     {height}')
    logging.debug(f'  Transform:  {transform}')
    logging.debug(f'  Extent:     {extent}')

    logging.debug(f'  Start hour offset: {start_hour_offset}')
    start_dt = tgt_dt + timedelta(hours=start_hour_offset)
    start_date = ee.Date.fromYMD(tgt_dt.year, tgt_dt.month, tgt_dt.day)\
        .advance(start_hour_offset, 'hour')
    end_date = start_date.advance(1, 'day')

    src_coll = ee.ImageCollection(SOURCE_COLL_ID).filterDate(start_date, end_date)

    # Check if there are 24 available images
    try:
        src_count = src_coll.size().getInfo()
    except Exception as e:
        return f'{export_name} - Could not get source image count, skipping\n{e}\n'
    if not src_count:
        src_count = 0
    logging.debug(f'  Source image count: {src_count}')

    if src_count == 0:
        logging.info('  No source data for date')
        return f'{export_name} - No source data for date, skipping\n'
    elif src_count < 24:
        logging.info('  Less than 24 hours data for date')
        return f'{export_name} - Less than 24 hours data for date, skipping\n'

    properties = {
        'build_date': datetime.today().strftime('%Y-%m-%d'),
        'date': tgt_dt.strftime('%Y-%m-%d'),
        'geerefet_version': metadata.version('openet-refet-gee'),
        'reference_et_timestep': refet_timestep.lower(),
        'start_hour_offset': start_hour_offset,
        'system:index': tgt_dt.strftime('%Y%m%d'),
        'system:time_start': millis(start_dt),
        # 'system:time_start': start_date.millis(),
    }
    for band_name, units in var_units.items():
        if units:
            properties[f'units_{band_name}'] = units

    def wind_magnitude(input_img):
        """Compute hourly wind magnitude from vectors"""
        return (
            ee.Image(input_img.select(['u_component_of_wind_10m'])).pow(2)
            .add(ee.Image(input_img.select(['v_component_of_wind_10m'])).pow(2))
            .sqrt()
            .rename(['wind_10m'])
        )

    # The reference ET is computed after masked cell filling is applied in the function
    # To keep everything consistent, the same filling approach is applied to the meteorology
    #   variables, but not the eto/etr
    if refet_timestep.lower() in ['daily', 'day']:
        # Compute reference ET at the daily timestep
        eto = openet.refetgee.Daily.era5_land(src_coll, fill_edge_cells=fill_edge_cells).eto
        etr = openet.refetgee.Daily.era5_land(src_coll, fill_edge_cells=fill_edge_cells).etr
    elif refet_timestep.lower() in ['hour', 'hourly']:
        # Compute reference ET at the hourly timestep and then sum to daily
        def hourly_eto(img):
            return openet.refetgee.Hourly.era5_land(img, fill_edge_cells=fill_edge_cells).eto
        def hourly_etr(img):
            return openet.refetgee.Hourly.era5_land(img, fill_edge_cells=fill_edge_cells).etr
        eto = ee.Image(ee.ImageCollection(src_coll.map(hourly_eto)).sum())
        etr = ee.Image(ee.ImageCollection(src_coll.map(hourly_etr)).sum())
    else:
        return f'{export_name} - Unsupported timestep {refet_timestep}, skipping'

    # Compute the daily meteorology variables
    tmax = src_coll.select(['temperature_2m']).max()
    tmin = src_coll.select(['temperature_2m']).min()
    tmean = src_coll.select(['temperature_2m']).mean()
    tdew = src_coll.select(['dewpoint_temperature_2m']).mean()
    wind = ee.Image(ee.ImageCollection(src_coll.map(wind_magnitude)).mean())
    srad = src_coll.select(['surface_solar_radiation_downwards_hourly']).sum()
    prcp = src_coll.select(['total_precipitation_hourly']).sum()
    #pres = src_coll.select(['surface_pressure']).mean()

    # Fill any masked pixels along the edge of the land mask
    # This will fill most coastal pixels and any small holes
    # TODO: It might be more efficient to apply this to the output image below,
    #   but it would need to exclude ETo and ETr
    if fill_edge_cells:
        def fill_edge_cells(image):
            img = ee.Image(image)
            return img.unmask(
                img.reduceNeighborhood('mean', ee.Kernel.square(1), 'kernel', False)
                .reproject(img.projection())
            )
        tmax = fill_edge_cells(tmax)
        tmin = fill_edge_cells(tmin)
        tmean = fill_edge_cells(tmean)
        tdew = fill_edge_cells(tdew)
        wind = fill_edge_cells(wind)
        srad = fill_edge_cells(srad)
        prcp = fill_edge_cells(prcp)
        #pres = fill_edge_cells(pres)

    output_img = (
        ee.Image([tmax, tmin, tmean, tdew, wind, srad, prcp, eto, etr])
        .rename([
            'temperature_2m_max',
            'temperature_2m_min',
            'temperature_2m_mean',
            'dewpoint_temperature_2m',
            'wind_10m',
            'surface_solar_radiation_downwards',
            'total_precipitation',
            'eto_asce',
            'etr_asce',
            # 'surface_pressure',
        ])
        .set(properties)
    )

    try:
        task = ee.batch.Export.image.toAsset(
            image=output_img,
            description=export_name,
            assetId=asset_id,
            dimensions='{}x{}'.format(width, height),
            crs=crs,
            crsTransform='[' + ', '.join(map(str, transform)) + ']',
            # maxPixels=int(1E10),
            # pyramidingPolicy='mean',
        )
        # task = ee.batch.Export.image.toCloudStorage(
        #     image=output_img.unmask(NODATA),
        #     description=export_name,
        #     bucket=BUCKET_NAME,
        #     fileNamePrefix=bucket_img.replace('.tif', ''),
        #     dimensions='{}x{}'.format(width, height),
        #     crs=crs,
        #     crsTransform='[' + ', '.join(map(str, transform)) + ']',
        #     fileFormat='GeoTIFF',
        #     formatOptions={'cloudOptimized': True, 'noData': NODATA},
        #     # maxPixels=int(1E10),
        #     # pyramidingPolicy='mean',
        # )
    except Exception as e:
        logging.warning('Export task not built, skipping')
        return f'{export_name} - export task not built\n'

    # Try to start the task a couple of times
    for i in range(1, 4):
        try:
            task.start()
            break
        except ee.ee_exception.EEException as e:
            logging.warning(f'EE Exception, retry {i}\n{e}')
        except Exception as e:
            logging.warning(f'Unhandled Exception: {e}')
            return f'{export_name} - Unhandled Exception: {e}\n'
        time.sleep(i ** 3)

    # DEADBEEF - No longer saving ERA5-Land assets as COGs
    # # logging.debug(f'Writing properties JSON to bucket')
    # # TODO: Wrap in try/except loop
    # bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    # blob = bucket.blob(bucket_json)
    # blob.upload_from_string(json.dumps(properties))

    logging.info(f'  {export_name} - {task.id}')
    return f'{export_name} - {task.id}\n'


def era5land_daily_asset_dates(
        start_dt,
        end_dt,
        region=None,
        overwrite_flag=False,
):
    """Identify dates of missing ERA5-Land daily assets

    Parameters
    ----------
    start_dt : datetime
    end_dt : datetime
        End date (exclusive)
    region : str
    overwrite_flag : bool, optional

    Returns
    -------
    list : datetimes

    """
    logging.info('\nBuilding ERA5-Land daily asset date list')

    # TODO: Should probably pass in the asset_coll_id instead of building from region
    if not region or (region.lower() == 'global'):
        asset_coll_id = f'{ASSET_FOLDER}/{TIMESTEP}'
    else:
        asset_coll_id = f'{ASSET_FOLDER}/{region}/{TIMESTEP}'

    task_id_re = re.compile(f'openet_meteo_era5land(_\w+)?_{TIMESTEP.lower()}_(?P<date>\d{{8}})$')
    asset_id_re = re.compile(asset_coll_id.split('projects/')[-1] + '/(?P<date>\d{8})$')

    # Figure out which asset dates need to be ingested
    # Start with a list of dates to check
    # logging.debug('\nBuilding Date List')
    tgt_dt_list = list(date_range(start_dt, end_dt))
    if not tgt_dt_list:
        logging.info('Empty date range')
        return []
    logging.debug('\nInitial test dates: {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))
    ))

    # Check if any of the needed dates are currently being exported
    # Check task list before checking asset list in case a task switches
    #   from running to done before the asset list is retrieved
    # This function is making export calls so ignore ingest tasks
    logging.debug('\nChecking task list')
    task_id_list = [
        desc.replace('"', '').strip()
        for desc in get_ee_tasks().keys()
        if ('Ingest image' not in desc) and ('Asset ingest' not in desc)
    ]
    task_date_list = [
        datetime.strptime(m.group('date'), ASSET_DT_FMT).strftime('%Y-%m-%d')
        for task_id in task_id_list
        for m in [task_id_re.search(task_id)] if m
    ]
    logging.debug(f'\nTask dates: {", ".join(task_date_list)}')

    # Switch date list to be dates that are missing
    tgt_dt_list = [
        dt for dt in tgt_dt_list
        if overwrite_flag or dt.strftime('%Y-%m-%d') not in task_date_list
    ]
    if not tgt_dt_list:
        logging.info('No dates to process after checking ready/running tasks')
        return []
    logging.debug('\nDates (after filtering tasks): {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))
    ))

    # Check if the assets already exist
    # For now, assume the collection exists
    logging.debug('\nChecking existing assets')
    asset_id_list = get_ee_assets(asset_coll_id, start_dt, end_dt)
    asset_date_list = [
        datetime.strptime(m.group('date'), ASSET_DT_FMT).strftime('%Y-%m-%d')
        for asset_id in asset_id_list
        for m in [asset_id_re.search(asset_id)] if m
    ]
    logging.debug(f'\nAsset dates: {", ".join(asset_date_list)}')

    # Switch date list to be dates that are missing
    tgt_dt_list = [
        dt for dt in tgt_dt_list
        if overwrite_flag or dt.strftime('%Y-%m-%d') not in asset_date_list
    ]
    if not tgt_dt_list:
        logging.info('No dates to process after filtering existing assets')
        return []
    logging.debug('\nDates (after filtering existing assets): {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))
    ))

    # # Check if the files already exist in the bucket
    # logging.debug('\nChecking existing bucket files')
    # logging.debug(f'\n  {BUCKET_NAME}/{BUCKET_FOLDER}')
    # bucket_object = STORAGE_CLIENT.get_bucket(BUCKET_NAME)
    # bucket_files = {x.name for x in bucket_object.list_blobs(prefix=BUCKET_FOLDER)}
    # pprint.pprint(bucket_files)
    # input('ENTER')
    #
    # bucket_date_list = [
    #     datetime.strptime(m.group('date'), ASSET_DT_FMT).strftime('%Y-%m-%d')
    #     for asset_id in bucket_files
    #     for m in [bucket_re.search(asset_id)] if m
    # ]
    # logging.debug(f'\nBucket dates: {", ".join(bucket_date_list)}')
    #
    # # Switch date list to be dates that are missing
    # tgt_dt_list = [
    #     dt for dt in tgt_dt_list
    #     if overwrite_flag or dt.strftime('%Y-%m-%d') not in bucket_date_list
    # ]
    # if not tgt_dt_list:
    #     logging.info('No dates to process after checking bucket files')
    #     return []
    # logging.debug('\nIngest dates: {}'.format(
    #     ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))
    # ))

    logging.debug('\nIngest dates: {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))
    ))

    return tgt_dt_list


def cron_scheduler(request):
    """Parse JSON/request arguments and queue ingest tasks for a date range"""
    response = 'Queue ERA5-Land daily asset export tasks\n'
    args = {}

    request_json = request.get_json(silent=True)
    request_args = request.args

    # Region parameter
    if request_json and ('region' in request_json):
        args['region'] = request_json['region']
    elif request_args and ('region' in request_args):
        args['region'] = request_args['region']
    else:
        args['region'] = None

    if args['region'] and (args['region'] not in REGIONS):
        abort(400, description=f'region parameter "{args["region"]}" is not supported')

    # Reference ET timestep parameter
    if request_json and ('refet_timestep' in request_json):
        refet_timestep = request_json['refet_timestep']
    elif request_args and ('refet_timestep' in request_args):
        refet_timestep = request_args['refet_timestep']
    else:
        refet_timestep = 'hourly'
    if refet_timestep and (refet_timestep.lower() not in ['hourly', 'daily']):
        abort(400, description=f'reference ET timestep "{refet_timestep}" is not supported')

    # Days parameter
    if request_json and ('days' in request_json):
        days = request_json['days']
    elif request_args and ('days' in request_args):
        days = request_args['days']
    else:
        days = START_DAY_OFFSET - END_DAY_OFFSET
    try:
        days = int(days)
    except:
        abort(400, description=f'days parameter "{days}" could not be parsed')

    # Start/end date parameter
    if request_json and ('start' in request_json):
        start_date = request_json['start']
    elif request_args and ('start' in request_args):
        start_date = request_args['start']
    else:
        start_date = None

    if request_json and ('end' in request_json):
        end_date = request_json['end']
    elif request_args and ('end' in request_args):
        end_date = request_args['end']
    else:
        end_date = None

    if (start_date is None) and (end_date is None):
        today_dt = datetime.today()
        start_date = (today_dt - timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = (today_dt - timedelta(days=END_DAY_OFFSET)).strftime('%Y-%m-%d')
    elif (start_date is None) or (end_date is None):
        abort(400, description='Both start and end date must be specified')

    try:
        args['start_dt'] = datetime.strptime(start_date, '%Y-%m-%d')
    except:
        abort(400, description=f'Start date {start_date} could not be parsed')
    try:
        args['end_dt'] = datetime.strptime(end_date, '%Y-%m-%d')
    except:
        abort(400, description=f'End date {end_date} could not be parsed')

    if args['end_dt'] < args['start_dt']:
        abort(400, description='End date must be after start date')

    # Fill edge cells parameter
    fill_edge_cells = parse_boolean_arg(request_json, request_args, 'fill_edge_cells', 'false')
    args['overwrite_flag'] = parse_boolean_arg(request_json, request_args, 'overwrite', 'false')
    reverse_flag = parse_boolean_arg(request_json, request_args, 'reverse', 'false')

    # Process each date
    for tgt_dt in sorted(era5land_daily_asset_dates(**args), reverse=reverse_flag):
        # logging.info(f'Date: {ingest_dt.strftime("%Y-%m-%d")}')
        # response += 'Date: {}\n'.format(ingest_dt.strftime('%Y-%m-%d'))
        response += era5land_daily_export(
            tgt_dt,
            region=args['region'],
            refet_timestep=refet_timestep,
            fill_edge_cells=fill_edge_cells,
            overwrite_flag=args['overwrite_flag'],
        )

    return Response(response, mimetype='text/plain')


def parse_boolean_arg(request_json, request_args, arg_key, arg_default_str='false'):
    """Convert the input argument strings from the request JSON or ARGS to boolean"""
    if request_json and (arg_key in request_json):
        arg_str = request_json[arg_key]
    elif request_args and (arg_key in request_args):
        arg_str = request_args[arg_key]
    else:
        arg_str = arg_default_str

    if arg_str.lower() in ['true', 't']:
        arg_flag = True
    elif arg_str.lower() in ['false', 'f']:
        arg_flag = False
    else:
        abort(400, description=f'overwrite parameter "{arg_str}" could not be parsed')

    return arg_flag


def date_range(start_dt, end_dt, days=1, skip_leap_days=False):
    """Generate dates within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date (exclusive).
    days : int, optional
        Step size (the default is 1).
    skip_leap_days : bool, optional
        If True, skip leap days while incrementing (the default is True).

    Yields
    ------
    datetime

    """
    import copy
    curr_dt = copy.copy(start_dt)
    while curr_dt < end_dt:
        if not skip_leap_days or curr_dt.month != 2 or curr_dt.day != 29:
            yield curr_dt
        curr_dt += timedelta(days=days)


def millis(input_dt):
    """Convert datetime to milliseconds since epoch"""
    return int(input_dt.replace(tzinfo=timezone.utc).timestamp()) * 1000
    # return int(calendar.timegm(end_dt.timetuple())) * 1000


def get_ee_assets(asset_id, start_dt=None, end_dt=None, retries=4):
    """Return assets IDs in a collection

    Parameters
    ----------
    asset_id : str
        A folder or image collection ID.
    start_dt : datetime, optional
        Start date (inclusive).
    end_dt : datetime, optional
        End date (exclusive, similar to .filterDate()).
    retries : int, optional
        The number of times to retry the call (the default is 4).

    Returns
    -------
    list : Asset IDs

    """
    # # CGM - There is a bug in earthengine-api>=0.1.326 that causes listImages()
    # #   to return an empty list if the startTime and endTime parameters are set
    # # Switching to a .aggregate_array(system:index).getInfo() approach for now
    # #   since getList is flagged for deprecation
    coll = ee.ImageCollection(asset_id)
    if start_dt and end_dt:
        coll = coll.filterDate(start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d'))
    # params = {'parent': asset_id}
    # if start_dt and end_dt:
    #     # CGM - Do both start and end need to be set to apply filtering?
    #     params['startTime'] = start_dt.isoformat() + '.000000000Z'
    #     params['endTime'] = end_dt.isoformat() + '.000000000Z'

    asset_id_list = []
    for i in range(1, retries):
        try:
            asset_id_list = coll.aggregate_array('system:index').getInfo()
            asset_id_list = [f'{asset_id}/{id}' for id in asset_id_list]
            # asset_id_list = [x['id'] for x in ee.data.listImages(params)['images']]
            break
        except ValueError:
            logging.info('  Collection or folder does not exist')
            raise sys.exit()
        except Exception as e:
            logging.error(f'  Error getting asset list, retrying ({i}/{retries})\n  {e}')
            time.sleep(i ** 3)

    return asset_id_list


def get_ee_tasks(states=['RUNNING', 'READY'], verbose=False, retries=6):
    """Return current active tasks

    Parameters
    ----------
    states : list, optional
        List of task states to check (the default is ['RUNNING', 'READY']).
    verbose : bool, optional
        This parameter is deprecated and is no longer being used.
        To get verbose logging of the active tasks use utils.print_ee_tasks().
    retries : int, optional
        The number of times to retry getting the task list if there is an error.

    Returns
    -------
    dict : Task descriptions (key) and task IDs (value).

    """

    tasks = {}
    for i in range(1, retries):
        try:
            task_list = ee.data.getTaskList()
            # task_list = ee.data.listOperations()
            task_list = sorted([
                [t['state'], t['description'], t['id']]
                for t in task_list if t['state'] in states
            ])
            tasks = {t_desc: t_id for t_state, t_desc, t_id in task_list}
            break
        except Exception as e:
            logging.info(f'  Error getting active task list, retrying ({i}/{retries})\n  {e}')
            time.sleep(i ** 3)

    return tasks


# # CGM - This is a modified copy of openet.utils.delay_task()
# #   It was changed to take and return the number of ready tasks
# #   This change may eventually be pushed to openet.utils.delay_task()
# def delay_task(ready_task_count, delay_time=0, max_ready=3000):
#     """Delay script execution based on number of READY tasks
#
#     Parameters
#     ----------
#     ready_task_count : int
#     delay_time : float, int
#         Delay time in seconds between starting export tasks or checking the
#         number of queued tasks if "max_ready" is > 0.  The default is 0.
#         The delay time will be set to a minimum of 10 seconds if max_ready > 0.
#     max_ready : int, optional
#         Maximum number of queued "READY" tasks.
#
#     Returns
#     -------
#     ready_task_count
#
#     """
#     # Force delay time to be a positive value
#     # (since parameter used to support negative values)
#     if delay_time < 0:
#         delay_time = abs(delay_time)
#
#     if (max_ready <= 0 or max_ready >= 3000) and delay_time > 0:
#         # Assume max_ready was not set and just wait the delay time
#         logging.debug(f'  Pausing {delay_time} seconds')
#         time.sleep(delay_time)
#         ready_task_count = 0
#     elif ready_task_count < max_ready:
#         # Skip waiting if the number of ready tasks is below the max
#         logging.debug(f'  Ready tasks: {ready_task_count}')
#     else:
#         # Don't continue to the next export until the number of READY tasks
#         # is greater than or equal to "max_ready"
#
#         # Force delay_time to be at least 10 seconds if max_ready is set
#         #   to avoid excessive EE calls
#         delay_time = max(delay_time, 10)
#
#         # Make an initial pause before checking tasks lists to allow
#         #   for previous export to start up.
#         logging.debug(f'  Pausing {delay_time} seconds')
#         time.sleep(delay_time)
#
#         while True:
#             ready_task_count = len(get_ee_tasks().keys())
#             logging.debug(f'  Ready tasks: {ready_task_count}')
#             if ready_task_count >= max_ready:
#                 logging.debug(f'  Pausing {delay_time} seconds')
#                 time.sleep(delay_time)
#             else:
#                 logging.debug('  Continuing iteration')
#                 break
#     return ready_task_count


# DEADBEEF - No longer saving ERA5-Land assets as COGs
# def delete_cog_asset(asset_id, bucket_name, bucket_img, bucket_json=None):
#     # Always remove the EE asset before deleting the bucket files
#     ee.data.deleteAsset(asset_id)
#
#     bucket = STORAGE_CLIENT.get_bucket(bucket_name)
#     img_blob = bucket.blob(bucket_img)
#     if img_blob.exists():
#         img_blob.delete()
#
#     if bucket_json is None:
#         bucket_json = bucket_img.replace('.tif', '_properties.json')
#     json_blob = bucket.blob(bucket_json)
#     if json_blob.exists():
#         json_blob.delete()


def arg_valid_date(input_date):
    """Check that a date string is ISO format (YYYY-MM-DD)

    This function is used to check the format of dates entered as command
      line arguments.
    DEADBEEF - It would probably make more sense to have this function
      parse the date using dateutil parser (http://labix.org/python-dateutil)
      and return the ISO format string

    Parameters
    ----------
    input_date : string

    Returns
    -------
    datetime

    Raises
    ------
    ArgParse ArgumentTypeError

    """
    try:
        return datetime.strptime(input_date, '%Y-%m-%d')
    except ValueError:
        raise argparse.ArgumentTypeError(f'Not a valid date: "{input_date}"')


def arg_valid_file(file_path):
    """Argparse specific function for testing if file exists

    Convert relative paths to absolute paths
    """
    if os.path.isfile(os.path.abspath(os.path.realpath(file_path))):
        return os.path.abspath(os.path.realpath(file_path))
    else:
        raise argparse.ArgumentTypeError(f'{file_path} does not exist')


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Build ERA5-Land daily assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--start', type=arg_valid_date, metavar='YYYY-MM-DD',
        help='Start date')
    parser.add_argument(
        '--end', type=arg_valid_date, metavar='YYYY-MM-DD',
        help='End date (exclusive)')
    parser.add_argument(
        '--region', default='na', choices=REGIONS, help='Region')
    parser.add_argument(
        '--timestep', default='daily', choices=['hourly', 'daily'],
        help='Reference ET computation timestep')
    parser.add_argument(
        '--fill', default=False, action='store_true', help='Fill edge cells')
    # parser.add_argument(
    #     '--delay', default=0, type=float,
    #     help='Delay (in seconds) between each export tasks')
    # parser.add_argument(
    #     '--ready', default=2500, type=int,
    #     help='Maximum number of queued READY tasks')
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

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    # Build the image collection if it doesn't exist
    if not args.region or (args.region.lower() == 'global'):
        asset_coll_id = f'{ASSET_FOLDER}/{TIMESTEP}'
    else:
        asset_coll_id = f'{ASSET_FOLDER}/{args.region}/{TIMESTEP}'
    logging.debug(f'\nImage Collection: {asset_coll_id}')

    if not ee.data.getInfo(asset_coll_id.rsplit('/', 1)[0]):
        logging.info(f'\nImage collection folder does not exist and will be built'
                     f'\n  {asset_coll_id.rsplit("/", 1)[0]}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'FOLDER'}, asset_coll_id.rsplit('/', 1)[0])
    if not ee.data.getInfo(asset_coll_id):
        logging.info(f'\nImage collection does not exist and will be built'
                     f'\n  {asset_coll_id}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, asset_coll_id)

    # # ready_tasks = len(get_ee_tasks().keys())
    #
    # ingest_dt_list = era5land_daily_asset_dates(
    #     args.start, args.end, region=args.region,
    #     overwrite_flag=args.overwrite, reverse_flag=args.reverse
    # )
    #
    # for ingest_dt in ingest_dt_list:
    #     response = era5land_daily_export(
    #         ingest_dt,
    #         region=args.region,
    #         refet_timestep=args.timestep,
    #         fill_edge_cells=args.fill,
    #         overwrite_flag=args.overwrite,
    #     )
    #     # logging.info(f'  {response}')
    #
    #     # ready_tasks += 1
    #     # ready_tasks = delay_task(ready_tasks, args.delay, args.ready)

    from unittest.mock import Mock
    data = {}
    if args.region:
        data['region'] = args.region
    if args.start and args.end:
        data['start'] = args.start.strftime('%Y-%m-%d')
        data['end'] = args.end.strftime('%Y-%m-%d')
    if args.timestep:
        data['refet_timestep'] = args.timestep
    # Convert booleans to string to mimic input when deployed
    if args.fill:
        data['fill_edge_cells'] = str(args.fill)
    if args.overwrite:
        data['overwrite'] = str(args.overwrite)
    if args.reverse:
        data['reverse'] = str(args.reverse)

    req = Mock(get_json=Mock(return_value=data), args=data)
    response = cron_scheduler(req)
    #print(response.data)
