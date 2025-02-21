import os
import pandas as pd
from datetime import datetime
from logging import Logger
from dateutil.tz import tzlocal

from airflow.models.variable import Variable

from utils.dict_util import json2str
from lib.wf_settings import EtlSetting, EtlSettingsRepository
from lib.connection_builder import VerticaConnect, S3Connect

class TransactionLoader:
    """
    Loads transaction data from S3 into the staging (STG) layer of the DWH.

    This class retrieves transaction data from S3, checks for updates,
    and loads new records into the Vertica database.
    """
    
    WF_KEY = "transactions_to_stg_wf"
    LAST_MODIFIED_DATE = "last_modified_date"
    LAST_DATA_DATE = "last_data_date"

    try:
        BUCKET_NAME = Variable.get('S3_BUCKET_NAME')
    except:
        BUCKET_NAME = 'mybucket'

    def __init__(self, origin: S3Connect, dest: VerticaConnect, logger: Logger) -> None:
        """
        Initialize the TransactionLoader.

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
        Load transaction data from S3 into Vertica.

        This method:
        - Retrieves the latest transaction files from S3.
        - Compares modification dates to check for new data.
        - Loads new transaction records into Vertica.
        - Handles rejected records.
        - Updates workflow metadata.
        """
        wf_setting = self.settings_repository.get_setting(self.dest, self.WF_KEY)
        now_date = datetime.now()
        rej_table = f'{self.settings_repository.schema}.transactions_rej_{now_date.strftime("%Y_%m_%d")}'

        # If no settings at all, create blank
        if not wf_setting:
            wf_setting = EtlSetting(
                        id=-1,
                        workflow_key=self.WF_KEY,
                        workflow_settings={
                            self.LAST_MODIFIED_DATE: datetime(2020, 12, 1).strftime("%Y-%m-%d %H:%M:%S"),
                            self.LAST_DATA_DATE: datetime(2020, 12, 1).strftime("%Y-%m-%d %H:%M:%S")})
            
        max_data_date = datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_DATA_DATE])
        max_lm_date = datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_MODIFIED_DATE])

        # List metadata for currencies files
        s3client = self.origin.client()
        metaobj = s3client.list_objects_v2(Bucket = self.BUCKET_NAME, Prefix = 'transactions_batch_')

        if 'Contents' not in metaobj:
            self.logger.error(f'No file found in buckent. Aborting..')
            raise FileNotFoundError 
        
        files = [{'Key' : x['Key'],
                 'LastModified': x['LastModified'].replace(tzinfo=None, microsecond = 0)} for x in metaobj['Contents'] \
                                                if x['LastModified'].replace(tzinfo=None, microsecond = 0) > \
                                                    datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_MODIFIED_DATE])]
        #Sorting if possible
        try:
            files.sort(key=lambda x: int(x['Key'].split('.')[0].split('_')[-1]))
        except:
            pass
        
        if len(files) == 0:
            self.logger.info('No modified files found. Exiting...')
            return
        self.logger.info(f'Found {len(files)} files with updated "Last Modified Date". Processing...')

        for file in files:
            self.logger.info(f'Processing file {file["Key"]}')
            local_file_path = f'/data/{file["Key"].split(".")[0] + "_" + now_date.strftime("%Y_%m_%d") + ".csv"}'
            self.logger.info(f'Local file path: {local_file_path}')
            try:
                s3client.download_file(self.BUCKET_NAME
                                   , file['Key']
                                   , local_file_path)
                self.logger.info(f"File downloaded successfully to {local_file_path}")
            except Exception as e:
                self.logger.error(f"Error downloading file {file['Key']}: {e}")
                raise e
            self.logger.info('Parsing file to check updates...')
            data_to_load = pd.read_csv(local_file_path)
            data_to_load['transaction_dt'] = pd.to_datetime(data_to_load['transaction_dt'])
            data_to_load = data_to_load[data_to_load['transaction_dt'] > datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_DATA_DATE])]

            if len(data_to_load) == 0:
                self.logger.info(f"No updates found in the file. Skipping...")
                continue

            self.logger.info(f"Found {len(data_to_load)} lines to load, processing...")
            data_to_load.to_csv(
                        path_or_buf=local_file_path, 
                        sep = ';', 
                        header = False,
                        encoding='utf-8',
                        index=False
                        )
            
            insert_sql = f"""
                    COPY {self.settings_repository.schema}.transactions (
                        operation_id, 
                        account_number_from, 
                        account_number_to,
                        currency_code, 
                        country, 
                        status, 
                        transaction_type, 
                        amount,
                        transaction_dt)
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
            
            max_data_date = max_data_date if max_data_date >= data_to_load['transaction_dt'].max() else data_to_load['transaction_dt'].max()
            max_lm_date = max_lm_date if max_lm_date >= file['LastModified'] else file['LastModified']

            try:
                os.remove(local_file_path)
                self.logger.info(f"File has been processed and deleted from local.")
            except:
                continue

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
        new_max_data_date = max_data_date.strftime("%Y-%m-%d %H:%M:%S")
        new_max_lm_date = max_lm_date.strftime("%Y-%m-%d %H:%M:%S")

        wf_setting.workflow_settings[self.LAST_MODIFIED_DATE] = new_max_lm_date
        wf_setting.workflow_settings[self.LAST_DATA_DATE] = new_max_data_date

        self.settings_repository.save_setting(self.dest, wf_setting.workflow_key, wf_setting.workflow_settings)
        self.logger.info(f"Finishing work. Last checkpoint: {json2str(wf_setting.workflow_settings)}")