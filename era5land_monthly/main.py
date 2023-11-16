import argparse
# import calendar
from datetime import datetime, timedelta, timezone
import json
import os
# import pprint
import re
import sys
import time

from dateutil.relativedelta import relativedelta
import ee
from flask import abort, Response
from google.cloud import storage

# The full path/ID is "projects/openet/meteorology/<dataset>/<region>/<timestep>"
ASSET_FOLDER = 'projects/openet/assets/meteorology/era5land'
ASSET_DT_FMT = '%Y%m'
BUCKET_NAME = 'openet_assets'
BUCKET_FOLDER = 'meteorology/era5land'
PROJECT_NAME = 'openet'
REGIONS = ['global', 'na', 'sa']
SOURCE_FOLDER = 'projects/openet/assets/meteorology/era5land'
SOURCE_TIMESTEP = 'daily'
# SOURCE_COLL_ID = 'ECMWF/ERA5_LAND/HOURLY'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)
START_MONTH_OFFSET = 3
END_MONTH_OFFSET = 0
NODATA = -9999
TIMESTEP = 'monthly'
VARIABLES = [
    'total_precipitation', 'eto_asce', 'etr_asce',
    # 'temperature_2m_max', 'temperature_2m_min', 'dewpoint_temperature_2m',
    # 'surface_pressure', 'wind_10m', 'surface_solar_radiation_downwards',
    # # 'surface_net_solar_radiation', 'surface_net_thermal_radiation',
]
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
    # logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.getLogger('earthengine-api').setLevel(logging.INFO)
    logging.getLogger('googleapiclient').setLevel(logging.INFO)
    logging.getLogger('requests').setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)

if 'FUNCTION_REGION' in os.environ:
    logging.debug(f'\nInitializing GEE using application default credentials')
    import google.auth
    credentials, project_id = google.auth.default(
        default_scopes=['https://www.googleapis.com/auth/earthengine']
    )
    ee.Initialize(credentials)
else:
    ee.Initialize()


def era5land_monthly_asset_export(tgt_dt, region=None, overwrite_flag=False):
    """Export ERA5-Land monthly meteorology assets

    Parameters
    ----------
    tgt_dt : datetime
    region : str, optional
    overwrite_flag : bool, optional

    Returns
    -------
    str : response string

    """

    if not region or region.lower() == 'global':
        logging.info(f'Export ERA5-Land {TIMESTEP.lower()} image - {tgt_dt.strftime("%Y-%m-%d")}')
        source_coll_id = f'{SOURCE_FOLDER}/{SOURCE_TIMESTEP.lower()}'
        export_name = f'openet_meteo_era5land_{TIMESTEP.lower()}_{tgt_dt.strftime("%Y%m%d")}'
        bucket_img = f'{BUCKET_FOLDER}/{TIMESTEP}/{tgt_dt.strftime(ASSET_DT_FMT)}.tif'
        bucket_json = bucket_img.replace('.tif', '_properties.json')
        asset_id = f'{ASSET_FOLDER}/{TIMESTEP}/{tgt_dt.strftime(ASSET_DT_FMT)}'
    else:
        logging.info(f'Export ERA5-Land {TIMESTEP.lower()} image - {region} - '
                     f'{tgt_dt.strftime("%Y-%m-%d")}')
        logging.debug(f'  Region: {region}')
        source_coll_id = f'{SOURCE_FOLDER}/{region}/{SOURCE_TIMESTEP.lower()}'
        export_name = f'openet_meteo_era5land_{region}_{TIMESTEP.lower()}_' \
                      f'{tgt_dt.strftime("%Y%m%d")}'
        bucket_img = f'{BUCKET_FOLDER}/{region}/{TIMESTEP}/' \
                     f'{tgt_dt.strftime(ASSET_DT_FMT)}.tif'
        bucket_json = bucket_img.replace('.tif', '_properties.json')
        asset_id = f'{ASSET_FOLDER}/{region}/{TIMESTEP}/{tgt_dt.strftime(ASSET_DT_FMT)}'
    logging.info(f'source_id:   {source_coll_id}')
    logging.info(f'export_id:   {export_name}')
    logging.info(f'bucket_img:  {bucket_img}')
    logging.info(f'bucket_json: {bucket_json}')
    logging.info(f'asset_id:    {asset_id}')

    start_date = tgt_dt.strftime('%Y-%m-%d')
    end_date = (tgt_dt + relativedelta(months=1)).strftime('%Y-%m-%d')
    logging.info(f'Ingest ERA5-Land monthly meteorology ({start_date} {end_date})')

    # export_name = f'openet_era5land_meteorology_monthly_{start_dt.strftime("%Y%m%d")}'
    # asset_id = f'{ASSET_COLL_ID}/{start_dt.strftime(ASSET_DT_FMT)}'
    # logging.debug(f'Image Collection: {ASSET_COLL_ID}')
    # logging.info(f'asset_id: {asset_id}')

    if not region or region == 'global':
        start_hour_offset = 0
        crs = 'EPSG:4326'
        # Setting width to 3601 to match source assets was causing issues
        # Reference ET equations don't work well above ~67 deg
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
    elif region in ['na']:
        # To include southern Greenland, include -33 east (1350 width?)
        start_hour_offset = 6
        crs = 'EPSG:4326'
        width, height = 1160, 601
        transform = [0.1, 0, -168.05, 0, -0.1, 67.05]
    elif region in ['sa']:
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
    dimensions = '{}x{}'.format(width, height)
    logging.debug(f'  Projection: {crs}')
    logging.debug(f'  Width:      {width}')
    logging.debug(f'  Height:     {height}')
    logging.debug(f'  Transform:  {transform}')
    logging.debug(f'  Extent:     {extent}')

    var_stats = {
        'total_precipitation': ['sum'],
        'eto_asce': ['sum'],
        'etr_asce': ['sum'],
        # 'temperature_2m_max': ['mean'],
        # 'temperature_2m_min': ['mean'],
        # 'dewpoint_temperature_2m': ['mean'],
        # 'surface_pressure': ['mean'],
        # 'wind_10m': ['mean'],
        # 'surface_solar_radiation_downwards': ['mean'],
        # # 'surface_net_solar_radiation': ['mean'],
        # # 'surface_net_thermal_radiation': ['mean'],
    }
    var_names = {
        'total_precipitation': 'total_precipitation',
        'eto_asce': 'eto_asce',
        'etr_asce': 'etr_asce',
        # 'temperature_2m_max': 'temperature_2m_max',
        # 'temperature_2m_min': 'temperature_2m_min',
        # 'dewpoint_temperature_2m': 'dewpoint_temperature_2m',
        # 'surface_pressure': 'surface_pressure',
        # 'wind_10m': 'wind_10m',
        # 'surface_solar_radiation_downwards': 'surface_solar_radiation_downwards',
        # 'surface_net_solar_radiation': 'surface_net_solar_radiation',
        # 'surface_net_thermal_radiation': 'surface_net_thermal_radiation',
    }

    try:
        asset_info = ee.data.getInfo(asset_id)
    except ee.ee_exception.EEException:
        logging.info('  EEException reading COG backed asset, removing')
        delete_cog_asset(asset_id, BUCKET_NAME, bucket_img, bucket_json)
        asset_info = None
        # return f'{export_name} - EEException on getInfo call, skipping'
    except:
        return f'{export_name} - Unhandled exception on getInfo call, skipping'

    if asset_info:
        if overwrite_flag:
            try:
                delete_cog_asset(asset_id, BUCKET_NAME, bucket_img, bucket_json)
                # ee.data.deleteAsset(asset_id)
            except Exception as e:
                logging.info(f'  Error deleting asset, skipping\n{e}')
                return f'{export_name} - Error deleting asset, skipping\n  {e}\n'
        else:
            logging.info('  The asset already exists and overwrite is False, skipping')
            return f'{export_name} - Asset already exists and overwrite is False, skipping\n'

    properties = {
        'date_ingested': TODAY_DT.strftime('%Y-%m-%d'),
        'month': int(tgt_dt.month),
        'year': int(tgt_dt.year),
        # 'scale_factor': 1.0,
        'system:time_start': millis(tgt_dt),
        # 'system:time_start': ee.Date(tgt_dt.strftime('%Y-%m-%d')).millis().getInfo(),
        # 'system:time_start': ee.Date(tgt_dt.strftime('%Y-%m-%d')).millis(),
        'system:index': tgt_dt.strftime(ASSET_DT_FMT),
    }

    var_img_list = []
    for var in VARIABLES:
        logging.debug(f'  Variable: {var}')
        var_coll = (
            ee.ImageCollection(source_coll_id)
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
        task = ee.batch.Export.image.toCloudStorage(
            image=output_img,
            description=export_name,
            bucket=BUCKET_NAME,
            fileNamePrefix=bucket_img.replace('.tif', ''),
            dimensions=dimensions,
            crs=crs,
            crsTransform='[' + ', '.join(map(str, transform)) + ']',
        )
        # export_task = ee.batch.Export.image.toAsset(
        #     image=output_img,
        #     description=export_name,
        #     assetId=asset_id,
        #     dimensions=dimensions,
        #     crs=crs,
        #     crsTransform='[' + ', '.join(map(str, transform)) + ']',
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

    # logging.debug(f'Writing properties JSON to bucket')
    # TODO: Wrap in try/except loop
    bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    blob = bucket.blob(bucket_json)
    blob.upload_from_string(json.dumps(properties))

    logging.info(f'  {export_name} - {task.id}')
    return f'{export_name} - {task.id}\n'


def era5land_monthly_asset_dates(start_dt, end_dt, region=None, overwrite_flag=False):
    """Identify dates of missing ERA5-Land monthly assets

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
    logging.debug('\nBuilding ERA5-Land monthly asset ingest date list')
    logging.debug(f'  {start_dt.strftime("%Y-%m-%d")}')
    logging.debug(f'  {end_dt.strftime("%Y-%m-%d")}')

    # TODO: Should probably pass in the asset_coll_id instead of building from region
    if not region or region.lower() == 'global':
        asset_coll_id = f'{ASSET_FOLDER}/{TIMESTEP}'
    else:
        asset_coll_id = f'{ASSET_FOLDER}/{region}/{TIMESTEP}'

    task_id_re = re.compile(
        f'openet_meteo_era5land(_\w+)?_{TIMESTEP.lower()}_(?P<date>\d{{8}})$'
    )
    asset_id_re = re.compile(asset_coll_id.split('projects/')[-1] + '/(?P<date>\d{8})$')

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

    # Check if the assets already exist
    # For now, assume the collection exists
    logging.debug('\nChecking existing assets')
    asset_id_list = get_ee_assets(asset_coll_id, start_dt, end_dt + timedelta(days=1))
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
    response = 'Queue monthly ERA5-Land meteorology assets COG export tasks\n'

    request_json = request.get_json(silent=True)
    request_args = request.args

    # Region parameter
    if request_json and 'region' in request_json:
        region = request_json['region']
    elif request_args and 'region' in request_args:
        region = request_args['region']
    else:
        region = None

    # # Months parameter
    # if request_json and 'months' in request_json:
    #     months = request_json['months']
    # elif request_args and 'months' in request_args:
    #     months = request_args['months']
    # else:
    #     months = START_MONTH_OFFSET - END_MONTH_OFFSET
    #
    # try:
    #     months = int(months)
    #     # args['months'] = int(months)
    # except:
    #     abort(400, description=f'months parameter could not be parsed')

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

    # CGM - For now don't allow scheduler calls to overwrite existing assets
    # if request_json and 'overwrite' in request_json:
    #     overwrite_flag = request_json['overwrite']
    # elif request_args and 'overwrite' in request_args:
    #     overwrite_flag = request_args['overwrite']
    # else:
    #     overwrite_flag = 'false'
    #
    # if overwrite_flag.lower() in ['true', 't']:
    #     args['overwrite_flag'] = True
    # elif overwrite_flag.lower() in ['false', 'f']:
    #     args['overwrite_flag'] = False
    # else:
    #     abort(400, description=f'overwrite "{overwrite_flag}" could not be parsed')

    args = {
        'start_dt': start_dt,
        'end_dt': end_dt,
        'region': region,
        'overwrite_flag': False,
    }

    for ingest_dt in era5land_monthly_asset_dates(**args):
        logging.info(f'Date: {ingest_dt.strftime("%Y-%m-%d")}')
        # response += f'Date: {tgt_dt.strftime("%Y-%m-%d")}\n'
        response += era5land_monthly_asset_export(
            ingest_dt, region=args['region'], overwrite_flag=args['overwrite_flag']
        )

    return Response(response, mimetype='text/plain')


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


def delete_cog_asset(asset_id, bucket_name, bucket_img, bucket_json=None):
    # Always remove the EE asset before deleting the bucket files
    ee.data.deleteAsset(asset_id)

    bucket = STORAGE_CLIENT.get_bucket(bucket_name)
    img_blob = bucket.blob(bucket_img)
    if img_blob.exists():
        img_blob.delete()

    if bucket_json is None:
        bucket_json = bucket_img.replace('.tif', '_properties.json')
    json_blob = bucket.blob(bucket_json)
    if json_blob.exists():
        json_blob.delete()


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
        '--region', default='na', choices=REGIONS, help='Region')
    # parser.add_argument(
    #     '--variables', nargs='+', metavar='VAR', default=['pr'],
    #     choices=VARIABLES, help='GRIDMET variables')
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
    if not args.region or args.region.lower() == 'global':
        asset_coll_id = f'{ASSET_FOLDER}/{TIMESTEP}'
    else:
        asset_coll_id = f'{ASSET_FOLDER}/{args.region}/{TIMESTEP}'
    logging.debug(f'\nImage Collection: {asset_coll_id}')

    if not ee.data.getInfo(asset_coll_id.rsplit('/', 1)[0]):
        logging.info(f'\nFolder does not exist and will be built'
                     f'\n  {asset_coll_id.rsplit("/", 1)[0]}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'FOLDER'}, asset_coll_id.rsplit('/', 1)[0])
    if not ee.data.getInfo(asset_coll_id):
        logging.info(f'\nImage collection does not exist and will be built'
                     f'\n  {asset_coll_id}')
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, asset_coll_id)

    ingest_dt_list = era5land_monthly_asset_dates(
        args.start, args.end, region=args.region, overwrite_flag=args.overwrite
    )

    for ingest_dt in sorted(ingest_dt_list, reverse=args.reverse):
        era5land_monthly_asset_export(
            ingest_dt, region=args.region, overwrite_flag=args.overwrite
        )

    # from unittest.mock import Mock
    # data = {}
    # if args.region:
    #     data['region'] = args.region
    # if args.start and args.end:
    #     data['start'] = args.start.strftime('%Y-%m-%d')
    #     data['end'] = args.end.strftime('%Y-%m-%d')
    # if args.overwrite:
    #     data['overwrite'] = args.overwrite
    #
    # req = Mock(get_json=Mock(return_value=data), args=data)
    # response = cron_scheduler(req)
    # print(response.data)
