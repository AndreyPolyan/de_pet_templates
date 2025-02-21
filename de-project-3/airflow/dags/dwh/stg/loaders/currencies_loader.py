import os
import pandas as pd
from datetime import datetime
from logging import Logger
from typing import List, Tuple
from dateutil.tz import tzlocal

from airflow.models.variable import Variable

from utils.dict_util import json2str
from lib.wf_settings import EtlSetting, EtlSettingsRepository
from lib.connection_builder import VerticaConnect, S3Connect



class CurrenciesLoader:
    """
    Loads currency exchange rate data from S3 into the staging (STG) layer of the DWH.

    This class retrieves the latest currency exchange rates from S3,
    checks for updates, and loads new data into the Vertica database.
    """
    WF_KEY = "currencies_to_stg_wf"
    LAST_MODIFIED_DATE = "last_modified_date"
    LAST_DATA_DATE = "last_data_date"

    try:
        BUCKET_NAME = Variable.get('S3_BUCKET_NAME')
    except:
        BUCKET_NAME = 'mybucket'

    def __init__(self, origin: S3Connect, dest: VerticaConnect, logger: Logger) -> None:
        """
        Initialize the CurrenciesLoader.

        Args:
            origin (S3Connect): Connection to the S3 storage.
            dest (VerticaConnect): Connection to the Vertica database.
            logger (Logger): Logger instance for logging execution details.
        """
        self.origin = origin
        self.dest = dest
        self.logger = logger
        self.settings_repository = EtlSettingsRepository('STG')
    
    def load_data(self) -> None:
        """
        Load currency exchange rate data from S3 into Vertica.

        This method:
        - Retrieves the latest file from S3.
        - Compares modification dates to check for new data.
        - Loads new currency exchange rates into Vertica.
        - Handles rejected records.
        - Updates workflow metadata.
        """
        wf_setting = self.settings_repository.get_setting(self.dest, self.WF_KEY)
        now_date = datetime.now()
        rej_table = f'{self.settings_repository.schema}.currencies_rej_{now_date.strftime("%Y_%m_%d")}'

        # If no settings at all, create blank
        if not wf_setting:
            wf_setting = EtlSetting(
                        id=-1,
                        workflow_key=self.WF_KEY,
                        workflow_settings={
                            self.LAST_MODIFIED_DATE: datetime(2020, 12, 1).strftime("%Y-%m-%d %H:%M:%S"),
                            self.LAST_DATA_DATE: datetime(2020, 12, 1).strftime('%Y-%m-%d')}
                    )
        
        # List metadata for currencies files

        s3client = self.origin.client()
        metaobj = s3client.list_objects_v2(Bucket = self.BUCKET_NAME, Prefix = 'currencies_history.csv')

        #If no file found exiting
        if 'Contents' not in metaobj:
            self.logger.error(f'No file found in buckent. Aborting..')
            raise FileNotFoundError 
        

        lm = max([x['LastModified'] for x in metaobj['Contents']])
        key = max([x['Key'] for x in metaobj['Contents']])

        try:
            lm = lm.replace(tzinfo=None, microsecond = 0)
        except Exception as e:
            self.logger.error("Failed to convert last date to local tz. Aborting...")
            raise e


        if lm <= datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_MODIFIED_DATE]):
            self.logger.info('No modifications found. Exiting...')
            return
        
        self.logger.info('Last Modified Date of the file updated. Downloading the file...')

        local_file_path = f'/data/{key.split(".")[0] + "_" + now_date.strftime("%Y_%m_%d") + ".csv"}'
        try:
            s3client.download_file(self.BUCKET_NAME
                                   , key
                                   , local_file_path)
            self.logger.info(f"File downloaded successfully to {local_file_path}")
        except Exception as e:
            self.logger.error(f"Error downloading file: {e}")
            raise e

        self.logger.info('Parsing file to check updates...')

        data_to_load = pd.read_csv(local_file_path)
        data_to_load['date_update'] = pd.to_datetime(data_to_load['date_update'])
        data_to_load = data_to_load[data_to_load['date_update'] > datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_DATA_DATE])]

        if len(data_to_load) == 0:
            self.logger.info(f"No updates found in the file. Exiting...")
            return
        
        self.logger.info(f"Found {len(data_to_load)} lines to load, processing...")
        data_to_load.to_csv(
                        path_or_buf=local_file_path, 
                        sep = ';', 
                        header = False,
                        encoding='utf-8',
                        index=False
                        )
        
        insert_sql = f"""
                    COPY {self.settings_repository.schema}.currencies (
                        currency_code, 
                        currency_code_with, 
                        date_update, 
                        currency_with_div)
                    FROM LOCAL '{local_file_path}'
                    DELIMITER ';'
                    REJECTED DATA AS TABLE {rej_table}; 
                    """
        with self.dest.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(insert_sql)
                except Exception as e:
                    self.logger.error(f'Failed to load. Error: {e}')
        
        #Check rejected lines. If blank -> delete temp table

        rej_sql = f"""SELECT count(1) rej_lines from {rej_table}"""
        
        with self.dest.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(rej_sql)
                    result = cur.fetchone()[0]
                except Exception as e:
                    self.logger.error(f'Failed to extract rejected line. Error: {e}')
        if result == 0: 
            self.logger.info(f'No rejected lines. Dropping temp table {rej_table}')
            with self.dest.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f'DROP TABLE IF EXISTS {rej_table}')

        #Updating wf metadata
        new_max_date = data_to_load['date_update'].max().strftime("%Y-%m-%d")

        wf_setting.workflow_settings[self.LAST_MODIFIED_DATE] = lm.strftime("%Y-%m-%d %H:%M:%S")
        wf_setting.workflow_settings[self.LAST_DATA_DATE] = new_max_date

        self.settings_repository.save_setting(self.dest, wf_setting.workflow_key, wf_setting.workflow_settings)
        self.logger.info(f"Finishing work. Last checkpoint: {json2str(wf_setting.workflow_settings)}")

        #Cleaning files
        try:
            os.remove(local_file_path)
            self.logger.info(f'currencies for {now_date.strftime("%Y-%m-%d")} has been removed')
        except:
            return
