import argparse
from datetime import datetime, timedelta, timezone
import os
import re
import sys
import time

import ee
from flask import abort, Response
from importlib import metadata
import openet.refetgee

ASSET_COLL_ID = 'projects/openet/assets/meteorology/urma/hawaii/daily'
ASSET_DT_FMT = '%Y%m%d'
PROJECT_NAME = 'openet'
SOURCE_COLL_ID = 'projects/openet/assets/meteorology/urma/hawaii/hourly'
ANCILLARY_FOLDER = 'projects/openet/assets/meteorology/urma/hawaii/ancillary'
START_DAY_OFFSET = 90
END_DAY_OFFSET = 0
START_HOUR_OFFSET = 10
NODATA = -9999
TODAY_DT = datetime.now(timezone.utc)
VARIABLES = [
    'TMP',
    'DPT',
    'SPFH',
    'PRES',
    'WDIR',
    'WIND',
    'TCDC',
    'SRAD_TCDC',
    'ETO',
    'ETR',
    # 'UGRD',
    # 'VGRD',
    # 'HGT',
    # 'GUST',
    # 'VIS',
    # 'CEIL',
    # 'HTSGW',
]

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
    ee.Initialize(ee.ServiceAccountCredentials('_', key_file='../../keys/openet-gee.json'))
    # ee.Initialize(project='ee-cmorton')


def urma_hawaii_daily_export(tgt_dt, refet_timestep='hourly', overwrite_flag=False):
    """Export daily URMA Hawaii image for a single date

    Parameters
    ----------
    tgt_dt : datetime
    refet_timestep : {'daily', 'hourly' (default)}, optional
        Daily Reference ET can be computed at the hourly timestep and summed to the day,
        or at the daily timestep from the aggregated meteorology variables.
    overwrite_flag : bool, optional

    Returns
    -------
    str : response string

    """

    logging.info(f'Export URMA Hawaii daily image - {tgt_dt.strftime("%Y-%m-%d")}')
    export_name = f'openet_meteo_urma_hawaii_daily_{tgt_dt.strftime("%Y%m%d")}'
    asset_id = f'{ASSET_COLL_ID}/{tgt_dt.strftime(ASSET_DT_FMT)}'
    # logging.info(f'export_id:   {export_name}')
    # logging.info(f'asset_id:    {asset_id}')

    try:
        asset_info = ee.data.getInfo(asset_id)
    except ee.ee_exception.EEException:
        return f'{export_name} - EEException on getInfo call, skipping'
    except:
        return f'{export_name} - Unhandled exception on getInfo call, skipping'

    if asset_info:
        if overwrite_flag:
            logging.info('  Removing existing asset')
            try:
                ee.data.deleteAsset(asset_id)
            except Exception as e:
                logging.info('  Error deleting asset, skipping')
                logging.info(f'{e}')
                return f'{export_name} - Error deleting asset, skipping\n{e}'
        else:
            logging.debug('  Asset already exists, skipping')
            return f'{export_name} - Asset already exists, skipping'

    # Hardcoding the shape and projection parameters for now
    # The transform is being manually shifted 6 cells up/north for better alignment
    # This adjustment was chosen based on visual inspection of the assets in GEE
    width, height = 321, 225
    transform = [2500, 0, -16879375, 0, -2500, 2481825 - (6 * 2500)]

    # grb_transform = [2500, 0, -16879374.0603126622736454, 0, -2500, 2481825.9654569458216429]
    wkt = (
        'PROJCS["unnamed", '
        '  GEOGCS["Coordinate System imported from GRIB file", \n'
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
    # crs = rasterio.crs.CRS.from_wkt(wkt)
    # wkt_str = (
    #     'PROJCS[\"unnamed\",GEOGCS[\"Coordinate System imported from GRIB file\",'
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

    extent = [
        transform[2], transform[5] + height * transform[4],
        transform[2] + width * transform[0], transform[5]
    ]
    logging.debug(f'  Projection: {wkt}')
    logging.debug(f'  Width:      {width}')
    logging.debug(f'  Height:     {height}')
    logging.debug(f'  Transform:  {transform}')
    logging.debug(f'  Extent:     {extent}')

    logging.debug(f'  Start hour offset: {START_HOUR_OFFSET}')
    start_dt = tgt_dt + timedelta(hours=START_HOUR_OFFSET)
    start_date = ee.Date.fromYMD(tgt_dt.year, tgt_dt.month, tgt_dt.day)\
        .advance(START_HOUR_OFFSET, 'hour')
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

    if refet_timestep.lower() in ['hour', 'hourly']:
        eto = src_coll.select(['ETO']).sum()
        etr = src_coll.select(['ETR']).sum()
        # # Compute reference ET at the hourly timestep and then sum to daily
        # # Note, this calculation is using the solar radiation computed from total cloud cover
        # def hourly_refet(img):
        #     r = openet.refetgee.Hourly.rtma(
        #         img,
        #         rs=img.select(['SRAD_TCDC']).multiply(0.0036),
        #         elev=ee.Image('projects/openet/assets/meteorology/urma/hawaii/ancillary/elevation'),
        #         lat=ee.Image('projects/openet/assets/meteorology/urma/hawaii/ancillary/latitude'),
        #         lon=ee.Image('projects/openet/assets/meteorology/urma/hawaii/ancillary/latitude'),
        #     )
        #     return ee.Image([r.eto, r.etr]).copyProperties(img, ['system:time_start'])
        #
        # refet_img = ee.Image(ee.ImageCollection(src_coll.map(hourly_refet)).sum())
        # eto = refet_img.select(['eto'])
        # etr = refet_img.select(['eto'])
    elif refet_timestep.lower() in ['daily', 'day']:
        # Compute reference ET at the daily timestep
        # Note, this calculation is using the solar radiation computed from total cloud cover
        refet_obj = openet.refetgee.Daily.rtma(
            src_coll,
            rs=src_coll.select(['SRAD_TCDC']).sum().multiply(0.0864),
            elev=ee.Image('projects/openet/assets/meteorology/urma/hawaii/ancillary/elevation'),
            lat=ee.Image('projects/openet/assets/meteorology/urma/hawaii/ancillary/latitude'),
        )
        eto = refet_obj.eto
        etr = refet_obj.etr
    else:
        return f'{export_name} - Unsupported timestep {refet_timestep}, skipping'

    properties = {
        'build_date': TODAY_DT.strftime('%Y-%m-%d'),
        'date': tgt_dt.strftime('%Y-%m-%d'),
        'geerefet_version': metadata.version('openet-refet-gee'),
        'reference_et_timestep': refet_timestep.lower(),
        'system:index': tgt_dt.strftime('%Y%m%d'),
        'system:time_start': millis(start_dt),
        # 'system:time_start': start_date.millis(),
    }

    # TODO: Confirm the solar radiation band name, but using SRAD for now
    var_units = {
        'TMAX': 'C',
        'TMIN': 'C',
        'TAVG': 'C',
        'DPT': 'C',
        'SPFH': 'kg kg-1',
        'PRES': 'Pa',
        'WIND': 'm s-1',
        'TCDC': '%',
        'SRAD_TCDC': 'MJ m-2 day-1',
        'ETO': 'mm',
        'ETR': 'mm',
        # 'PCP': 'kg m-2',
    }

    for band_name, units in var_units.items():
        if units:
            properties[f'units_{band_name}'] = units

    # # CGM - Variable list is hardcoded for now so always write version above
    # if 'eto_asce' in variables or 'etr_asce' in variables:
    #     properties['geerefet_version'] = metadata.version('openet-refet-gee')
    #     # properties['geerefet_version'] = openet.refetgee.__version__

    output_img = (
        ee.Image([
            src_coll.select(['TMP']).max(),
            src_coll.select(['TMP']).min(),
            src_coll.select(['TMP']).mean(),
            src_coll.select(['DPT']).mean(),
            src_coll.select(['SPFH']).mean(),
            src_coll.select(['PRES']).mean(),
            src_coll.select(['WIND']).mean(),
            src_coll.select(['SRAD_TCDC']).sum(),
            # # Convert to MJ m-2 day-1
            # src_coll.select(['SRAD_TCDC']).sum().multiply(0.0864),
            eto,
            etr,
            # src_coll.select(['PCP']).sum(),
        ])
        .rename([
            'TMAX',
            'TMIN',
            'TAVG',
            'DPT',
            'SPFH',
            'PRES',
            'WIND',
            'SRAD_TCDC',
            'ETO',
            'ETR',
            # 'PCP',
        ])
        .set(properties)
        # TODO: Check if this unmask call is needed
        #.unmask(NODATA)
    )

    try:
        task = ee.batch.Export.image.toAsset(
            image=output_img,
            description=export_name,
            assetId=asset_id,
            dimensions='{}x{}'.format(width, height),
            crs=wkt,
            crsTransform='[' + ', '.join(map(str, transform)) + ']',
            maxPixels=int(1E10),
            # pyramidingPolicy='mean',
        )
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

    logging.info(f'  {export_name} - {task.id}')
    return f'{export_name} - {task.id}\n'


def daily_asset_dates(start_dt, end_dt, overwrite_flag=False):
    """Identify dates of missing daily assets

    Parameters
    ----------
    start_dt : datetime
        Start date (inclusive)
    end_dt : datetime
        End date (exclusive)
    overwrite_flag : bool, optional

    Returns
    -------
    list : datetimes

    """
    logging.info('\nBuilding daily asset date list')

    task_id_re = re.compile(f'openet_meteo_urma_hi_(_\w+)?_daily_(?P<date>\d{{8}})$')
    asset_id_re = re.compile(ASSET_COLL_ID.split('projects/')[-1] + '/(?P<date>\d{8})$')

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
    #   from running to done before the asset list is retrieved.
    logging.debug('\nChecking task list')
    task_id_list = [desc.replace('\nAsset ingestion: ', '') for desc in get_ee_tasks().keys()]
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
    asset_id_list = get_ee_assets(ASSET_COLL_ID, start_dt, end_dt)
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
    # logging.debug('\nDates (after filtering existing assets): {}'.format(
    #     ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))
    # ))

    logging.debug('\nIngest dates: {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))
    ))

    return tgt_dt_list


def cron_scheduler(request):
    """Parse JSON/request arguments and queue ingest tasks for a date range"""
    response = 'Queue URMA Hawaii daily asset export tasks\n'
    args = {}

    request_json = request.get_json(silent=True)
    request_args = request.args

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
        abort(400, description=f'days parameter could not be parsed')

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
        start_date = (TODAY_DT - timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = (TODAY_DT - timedelta(days=END_DAY_OFFSET)).strftime('%Y-%m-%d')
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

    args['overwrite_flag'] = parse_boolean_arg(request_json, request_args, 'overwrite', 'false')
    reverse_flag = parse_boolean_arg(request_json, request_args, 'reverse', 'false')

    # Process each date
    for ingest_dt in sorted(daily_asset_dates(**args), reverse=reverse_flag):
        # logging.info(f'Date: {ingest_dt.strftime("%Y-%m-%d")}')
        # response += 'Date: {}\n'.format(ingest_dt.strftime('%Y-%m-%d'))
        response += urma_hawaii_daily_export(
            ingest_dt,
            refet_timestep=refet_timestep,
            overwrite_flag=args['overwrite_flag']
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
    # CGM - There is a bug in earthengine-api>=0.1.326 that causes listImages()
    #   to return an empty list if the startTime and endTime parameters are set
    # Switching to a .aggregate_array(system:index).getInfo() approach for now
    #   since getList is flagged for deprecation
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
                [t['state'], t['description'], t['id']] for t in task_list if t['state'] in states
            ])
            tasks = {t_desc: t_id for t_state, t_desc, t_id in task_list}
            break
        except Exception as e:
            logging.info(f'  Error getting active task list, retrying ({i}/{retries})\n  {e}')
            time.sleep(i ** 3)

    return tasks


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
        description='Build URMA Hawaii daily assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--start', type=arg_valid_date, metavar='YYYY-MM-DD',
        help='Start date')
    parser.add_argument(
        '--end', type=arg_valid_date, metavar='YYYY-MM-DD',
        help='End date (exclusive)')
    parser.add_argument(
        '--timestep', default='daily', choices=['hourly', 'daily'],
        help='Reference ET computation timestep')
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
    logging.debug(f'\nImage Collection: {ASSET_COLL_ID}')
    if not ee.data.getInfo(ASSET_COLL_ID.rsplit('/', 1)[0]):
        logging.info(f'\nImage collection folder does not exist and will be built'
                     f'\n  {ASSET_COLL_ID.rsplit("/", 1)[0]}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'FOLDER'}, ASSET_COLL_ID.rsplit('/', 1)[0])
    if not ee.data.getInfo(ASSET_COLL_ID):
        logging.info(f'\nImage collection does not exist and will be built'
                     f'\n  {ASSET_COLL_ID}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, ASSET_COLL_ID)

    # ingest_dt_list = daily_asset_dates(args.start, args.end, overwrite_flag=args.overwrite)
    # for ingest_dt in sorted(ingest_dt_list, reverse=args.reverse):
    #     response = urma_hawaii_daily_export(
    #         ingest_dt, refet_timestep=args.timestep, overwrite_flag=args.overwrite
    #     )
    #     # logging.info(f'  {response}')
    #
    #     # ready_tasks += 1
    #     # ready_tasks = delay_task(ready_tasks, args.delay, args.ready)

    from unittest.mock import Mock
    data = {}
    if args.start and args.end:
        data['start'] = args.start.strftime('%Y-%m-%d')
        data['end'] = args.end.strftime('%Y-%m-%d')
    if args.timestep:
        data['refet_timestep'] = args.timestep
    if args.overwrite:
        data['overwrite'] = str(args.overwrite)
    if args.overwrite:
        data['reverse'] = str(args.reverse)

    req = Mock(get_json=Mock(return_value=data), args=data)
    response = cron_scheduler(req)
    print(response.data)
