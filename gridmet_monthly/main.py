import argparse
# import calendar
from datetime import datetime, timedelta, timezone
import os
# import pprint
import re
import time

from dateutil.relativedelta import relativedelta
import ee
from flask import abort, Response

# V1
# ASSET_COLL_ID = 'projects/earthengine-legacy/assets/' \
#                 'projects/openet/meteorology/conus/gridmet/monthly'
ASSET_COLL_ID = 'projects/earthengine-legacy/assets/' \
                'projects/openet/meteorology/gridmet/monthly'
SOURCE_COLL_ID = 'IDAHO_EPSCOR/GRIDMET'
ASSET_DT_FMT = '%Y%m'
START_MONTH_OFFSET = 3
END_MONTH_OFFSET = 0
VARIABLES = ['pr']
# VARIABLES = ['eto', 'etr', 'pr']
# VARIABLES = ['bi', 'erc', 'eto', 'etr', 'fm100', 'fm1000', 'pr', 'rmax',
#              'rmin', 'sph', 'srad', 'th', 'tmmn', 'tmmx', 'vs', 'vpd']
TODAY_DT = datetime.today()
# TODAY_DT = datetime.now(timezone=timezone.utc)

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
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.getLogger('earthengine-api').setLevel(logging.INFO)
    logging.getLogger('googleapiclient').setLevel(logging.INFO)
    logging.getLogger('requests').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)

if 'FUNCTION_REGION' in os.environ:
    logging.debug(f'\nInitializing GEE using application default credentials')
    import google.auth
    credentials, project_id = google.auth.default(
        default_scopes=['https://www.googleapis.com/auth/earthengine']
    )
    ee.Initialize(credentials)
else:
    ee.Initialize()


def gridmet_monthly_asset_export(start_dt, variables, overwrite_flag=False):
    """Export GRIDMET monthly meteorology assets

    Parameters
    ----------
    start_dt : datetime
    variables : list
    overwrite_flag : bool, optional

    Returns
    -------
    str : response string

    """
    end_dt = start_dt + relativedelta(months=1)
    start_date = start_dt.strftime('%Y-%m-%d')
    end_date = end_dt.strftime('%Y-%m-%d')
    logging.info(f'Build GRIDMET monthly meteorology asset ({start_date} {end_date})')

    export_name = f'openet_gridmet_meteorology_monthly_{start_dt.strftime("%Y%m%d")}'
    asset_id = f'{ASSET_COLL_ID}/{start_dt.strftime(ASSET_DT_FMT)}'
    # logging.debug(f'Image Collection: {ASSET_COLL_ID}')
    # logging.info(f'asset_id: {asset_id}')

    asset_geo = [
        0.041666666666666664, 0, -124.78749996666667,
        0, -0.041666666666666664, 49.42083333333334
    ]
    asset_crs = 'EPSG:4326'
    asset_dimensions = '1386x585'
    asset_geo_str = '[' + ','.join(list(map(str, asset_geo))) + ']'

    var_stats = {
        'eto': ['sum'],
        'etr': ['sum'],
        'pr': ['sum'],
        # 'rmax': [],
        # 'rmin': [],
        # 'sph': [],
        # 'srad': [],
        # 'tmmn': [],
        # 'tmmx': [],
        # 'vs': [],
        # 'vpd': [],
    }
    var_names = {
        'eto': 'eto',
        'etr': 'etr',
        'pr': 'pr',
    }

    if ee.data.getInfo(asset_id):
        if overwrite_flag:
            try:
                ee.data.deleteAsset(asset_id)
            except Exception as e:
                logging.info(f'  Error deleting the existing asset, skipping\n{e}')
                return f'{export_name} - Error deleting the existing asset, skipping\n' \
                       f'  {e}\n'
        else:
            logging.info('  The asset already exists and overwrite is False, skipping')
            return f'{export_name} - The asset already exists and overwrite '\
                   f'is False, skipping\n'

    source_coll = (
        ee.ImageCollection(SOURCE_COLL_ID)
        .filterDate(start_date, end_date)
        .select(variables)
    )

    # TODO: Come up with a way to do this server side to avoid the getInfo
    #   or wrap in a try/except loop
    status = (
        ee.Dictionary({'permanent': 0, 'provisional': 0, 'early': 0})
        .combine(source_coll.aggregate_histogram('status'))
        .getInfo()
    )

    # TODO: Come up with a way to do this server side to avoid the getInfo above
    if status['early'] > 0:
        status_summary = 'early'
    elif status['provisional'] > 0:
        status_summary = 'provisional'
    else:
        status_summary = 'permanent'

    properties = {
        'date_ingested': TODAY_DT.strftime('%Y-%m-%d'),
        'month': int(start_dt.month),
        'year': int(start_dt.year),
        'early': status['early'],
        'provisional': status['provisional'],
        'permanent': status['permanent'],
        # TODO: Switch to server side calls (see TODO's abovee)
        # 'early': status.get('early'),
        # 'provisional': status.get('provisional'),
        # 'permanent': status.get('permanent'),
        # 'scale_factor': 1.0,
        'status': status_summary,
        # 'system:time_start': millis(start_dt),
        'system:time_start': ee.Date(start_dt.strftime('%Y-%m-%d')).millis(),
        'system:index': start_dt.strftime(ASSET_DT_FMT),
    }

    var_img_list = []
    for var in variables:
        logging.debug(f'  Variable: {var}')
        var_coll = (
            ee.ImageCollection(SOURCE_COLL_ID)
            .filterDate(start_date, end_date)
            .select([var])
        )

        for stat in var_stats[var]:
            logging.debug(f'    Stat: {stat}')
            if stat.lower() == 'sum':
                var_img = ee.Image(var_coll.sum())
            elif stat.lower() == 'mean':
                var_img = ee.Image(var_coll.mean())
            # elif stat.lower() == 'min':
            #     var_img = ee.Image(var_coll.min())
            # elif stat.lower() == 'max':
            #     var_img = ee.Image(var_coll.max())

            # # Add stat to band name
            # var_img = var_img.rename([f'{var}_{stat}'])

            var_img_list.append(var_img.rename(var_names[var]))

    output_img = ee.Image(var_img_list).set(properties)

    try:
        export_task = ee.batch.Export.image.toAsset(
            image=output_img,
            description=export_name,
            assetId=asset_id,
            dimensions=asset_dimensions,
            crs=asset_crs,
            crsTransform=asset_geo_str,
        )
    except Exception as e:
        logging.warning('Export task not built, skipping')
        return f'{export_name} - export task not built\n'

    # Start the export task
    export_task.start()

    # # Try to start the task a couple of times
    # for i in range(1, 4):
    #     try:
    #         export_task.start()
    #         break
    #     except ee.ee_exception.EEException as e:
    #         logging.warning(f'EE Exception, retry {i}\n{e}')
    #     except Exception as e:
    #         logging.warning(f'Unhandled Exception: {e}')
    #         return f'Unhandled Exception: {e}'
    #     time.sleep(i ** 3)

    logging.info(f'  {export_name} - {export_task.id}')
    return f'{export_name} - {export_task.id}\n'


def gridmet_monthly_asset_dates(start_dt, end_dt, overwrite_flag=False):
    """Identify dates of missing GRIDMET monthly assets

    Parameters
    ----------
    start_dt : datetime
    end_dt : datetime
    overwrite_flag : bool, optional

    Returns
    -------
    list : datetimes

    """
    logging.debug('\nBuilding GRIDMET monthly asset ingest date list')
    logging.debug(f'  {start_dt.strftime("%Y-%m-%d")}')
    logging.debug(f'  {end_dt.strftime("%Y-%m-%d")}')

    task_id_re = re.compile(ASSET_COLL_ID.split('projects/')[-1] + '/(?P<date>\d{8})$')
    # asset_id_re = re.compile(
    #     ASSET_COLL_ID.split('projects/')[-1] + '/(?P<date>\d{8})$')

    # Figure out which asset dates need to be ingested
    # Start with a list of dates to check
    # logging.debug('\nBuilding Date List')
    tgt_dt_list = list(month_range(start_dt, end_dt))
    if not tgt_dt_list:
        logging.info('Empty date range')
        return []
    logging.debug('\nInitial test dates: {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))
    ))

    # Check if any of the needed dates are currently being ingested
    # Check task list before checking asset list in case a task switches
    #   from running to done before the asset list is retrieved.
    logging.debug('\nChecking task list')
    task_id_list = [
        desc.replace('\nAsset ingestion: ', '')
        for desc in get_ee_tasks(states=['RUNNING', 'READY']).keys()
    ]
    task_dates = {
        datetime.strptime(m.group('date'), '%Y%m%d').strftime('%Y-%m-%d')
        for task_id in task_id_list
        for m in [task_id_re.search(task_id)] if m
    }
    logging.debug(f'\nTask dates: {", ".join(task_dates)}')

    # Switch date list to be dates that are missing
    tgt_dt_list = [dt for dt in tgt_dt_list if dt.strftime('%Y-%m-%d') not in task_dates]
    #     if overwrite_flag or dt.strftime('%Y-%m-%d') not in task_dates]
    if not tgt_dt_list:
        logging.info('No dates to process after checking ready/running tasks')
        return []
    logging.debug('\nDates (after filtering tasks): {}'.format(
        ', '.join(map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))
    ))

    # CGM - Checking if the assets exists is happening in the main function
    # logging.debug('\nChecking existing assets')
    # asset_id_list = get_ee_assets(ASSET_COLL_ID, start_dt, end_dt + timedelta(days=1))
    # asset_date_list = [
    #     datetime.strptime(m.group('date'), ASSET_DT_FMT).strftime('%Y-%m-%d')
    #     for asset_id in asset_id_list
    #     for m in [asset_id_re.search(asset_id)] if m
    # ]
    # logging.debug(f'\nAsset dates: {", ".join(asset_date_list)}')
    #
    # # Switch date list to be dates that are missing
    # tgt_dt_list = [
    #     dt for dt in tgt_dt_list
    #     if overwrite_flag or dt.strftime('%Y-%m-%d') not in asset_date_list
    # ]
    # if not tgt_dt_list:
    #     logging.info('No dates to process after filtering existing assets')
    #     return []
    # logging.debug('\nIngest dates: {}'.format(', '.join(
    #     map(lambda x: x.strftime('%Y-%m-%d'), tgt_dt_list))
    # ))

    # # CGM - It might be easier to just always overwrite, but I'm trying to place
    # #   some limit on how often the ingest can be run
    # # Get the image IDs "current" images (have the same date_ingested as today)
    # # Since the jobs are run at 0 UTC, the local and UTC dates should be the same
    # asset_id_list = (
    #     ee.ImageCollection(ASSET_COLL_ID)
    #     .filterMetadata('date_ingested', 'equals', TODAY_DT.strftime('%Y-%m-%d UTC'))
    #     .aggregate_array('system:index')
    #     .getInfo()
    # )
    # asset_id_list = sorted(list(asset_id_list))
    # # logging.info(asset_id_list)

    return tgt_dt_list


def month_range(start_dt, end_dt):
    """Generate month dates within a range (inclusive)

    Parameters
    ----------
    start_dt : datetime
        Start date.
    end_dt : datetime
        End date.

    Yields
    ------
    datetime

    """
    import copy
    curr_dt = copy.copy(datetime(start_dt.year, start_dt.month, 1))
    while curr_dt <= end_dt:
        yield curr_dt
        curr_dt += relativedelta(months=1)


def cron_scheduler(request):
    """Responds to any HTTP request.

    Parameters
    ----------
    request (flask.Request): HTTP request object.

    Returns
    -------
    The response text or any set of values that can be turned into a
    Response object using
    `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.

    """
    response = 'Queue monthly GRIDMET meteorology asset export tasks\n'

    request_json = request.get_json(silent=True)
    request_args = request.args

    # Default start and end date to None if not set
    if request_json and 'start' in request_json:
        start_date = request_json['start']
    elif request_args and 'start' in request_args:
        start_date = request_args['start']
    else:
        start_date = None

    if request_json and 'end' in request_json:
        end_date = request_json['end']
    elif request_args and 'end' in request_args:
        end_date = request_args['end']
    else:
        end_date = None

    if not start_date and not end_date:
        start_dt = (datetime(TODAY_DT.year, TODAY_DT.month, 1) -
                    relativedelta(months=START_MONTH_OFFSET))
        end_dt = (datetime(TODAY_DT.year, TODAY_DT.month, 1) - relativedelta(days=1) -
                  relativedelta(days=END_MONTH_OFFSET))
    elif start_date and end_date:
        # Only process custom range if start and end are both set
        # Limit the end date to the last full month date
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError as e:
            response = 'Error parsing start and/or end date\n'
            response += str(e)
            abort(404, description=response)

        # Force end date to be last day of previous month
        end_dt = min(end_dt, TODAY_DT - timedelta(days=1))

        # TODO: Force start date to be at least one month before end
        # start_dt = min(start_dt, end_dt - relativedelta(months=1) + relativedelta(days=1))

        if start_dt > end_dt:
            abort(404, description='Start date must be before end date')
        # elif (end_dt - start_dt) > timedelta(days=400):
        #     abort(404, description='No more than 1 year can be processed in a single request')
        # if start_dt < datetime(1980, 1, 1):
        #     logging.debug('Start Date: {} - no GRIDMET images before '
        #                   '1980-01-01'.format(start_dt.strftime('%Y-%m-%d')))
        #     start_dt = datetime(1980, 1, 1)
    else:
        abort(404, description='Both start and end date must be specified')
    response += f'Start Date: {start_dt.strftime("%Y-%m-%d")}\n'
    response += f'End Date:   {end_dt.strftime("%Y-%m-%d")}\n'

    args = {
        'start_dt': start_dt,
        'end_dt': end_dt,
    }

    for tgt_dt in gridmet_monthly_asset_dates(**args):
        logging.info(f'Date: {tgt_dt.strftime("%Y-%m-%d")}')
        # response += f'Date: {tgt_dt.strftime("%Y-%m-%d")}\n'
        response += gridmet_monthly_asset_export(
            tgt_dt, variables=VARIABLES, overwrite_flag=True
        )

    return Response(response, mimetype='text/plain')


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
    dict : task descriptions (key) and full task info dictionary (value)

    """
    logging.debug('\nRequesting Task List')
    task_list = None
    for i in range(1, retries):
        try:
            # TODO: getTaskList() is deprecated, switch to listOperations()
            task_list = ee.data.getTaskList()
            # task_list = ee.data.listOperations()
            break
        except Exception as e:
            logging.warning(f'  Error getting task list, retrying ({i}/{retries})\n  {e}')
            time.sleep(i ** 3)
    if task_list is None:
        raise Exception('\nUnable to retrieve task list, exiting')

    task_list = sorted(
        [task for task in task_list if task['state'] in states],
        key=lambda t: (t['state'], t['description'], t['id'])
    )
    # task_list = sorted([
    #     [t['state'], t['description'], t['id']] for t in task_list
    #     if t['state'] in states])

    # Convert the task list to a dictionary with the task name as the key
    return {task['description']: task for task in task_list}


def millis(input_dt):
    """Convert datetime to milliseconds since epoch"""
    return int(input_dt.replace(tzinfo=timezone.utc).timestamp()) * 1000
    # Python 3 (or 2 with future module)
    # return 1000 * int(calendar.timegm(input_dt.timetuple()))
    # Python 2
    # return 1000 * long(calendar.timegm(input_dt.timetuple()))
    # return 1000 * long(time.mktime(input_dt.timetuple()))


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
        return datetime.strptime(input_date, "%Y-%m-%d")
    except ValueError:
        msg = f"Not a valid date: '{input_date}'."
        raise argparse.ArgumentTypeError(msg)


def arg_valid_file(file_path):
    """Argparse specific function for testing if file exists

    Convert relative paths to absolute paths
    """
    if os.path.isfile(os.path.abspath(os.path.realpath(file_path))):
        return os.path.abspath(os.path.realpath(file_path))
        # return file_path
    else:
        raise argparse.ArgumentTypeError(f'{file_path} does not exist')


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Generate GRIDMET monthly assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--start', type=arg_valid_date, metavar='YYYY-MM-DD',
        default=(datetime(TODAY_DT.year, TODAY_DT.month, 1) -
                 relativedelta(months=START_MONTH_OFFSET)).strftime('%Y-%m-%d'),
        help='Start date')
    parser.add_argument(
        '--end', type=arg_valid_date, metavar='YYYY-MM-DD',
        default=(datetime(TODAY_DT.year, TODAY_DT.month, 1) - relativedelta(days=1) -
                 relativedelta(months=END_MONTH_OFFSET)).strftime('%Y-%m-%d'),
        help='End date (inclusive)')
    parser.add_argument(
        '--variables', nargs='+', metavar='VAR', default=['pr'],
        choices=VARIABLES, help='GRIDMET variables')
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

    # # Build the image collection if it doesn't exist
    # logging.debug(f'Image Collection: {ASSET_COLL_ID}')
    # if not ee.data.getInfo(ASSET_COLL_ID.rsplit('/', 1)[0]):
    #     logging.info(f'\nFolder does not exist and will be built'
    #                  f'\n  {ASSET_COLL_ID.rsplit("/", 1)[0]}')
    #     input('Press ENTER to continue')
    #     ee.data.createAsset({'type': 'FOLDER'}, ASSET_COLL_ID.rsplit('/', 1)[0])
    if not ee.data.getInfo(ASSET_COLL_ID):
        logging.info(f'\nImage collection does not exist and will be built'
                     f'\n  {ASSET_COLL_ID}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, ASSET_COLL_ID)

    ingest_dt_list = gridmet_monthly_asset_dates(
        args.start, args.end, overwrite_flag=args.overwrite
    )

    for tgt_dt in sorted(ingest_dt_list, reverse=args.reverse):
        gridmet_monthly_asset_export(
            tgt_dt, variables=args.variables, overwrite_flag=args.overwrite
        )

    # # CGM - Is there a better way to get a fake response object?
    # #   I could also just loop over the models and regions here
    # from unittest.mock import Mock
    # # data = {'start': ','.join(args.start), 'end': ','.join(args.end)}
    # data = {}
    # req = Mock(get_json=Mock(return_value=data), args=data)
    # response = cron_worker(req)
    # print(response)
