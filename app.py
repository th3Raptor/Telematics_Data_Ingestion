import os
import sys
import json
import logging
import subprocess
from datetime import datetime
from collections import namedtuple
# sys.path.append(r"C:\_Development\Python\utils")
sys.path.append(r"E:\Utils")
from ryderlib.database.snowflake import SnowflakeRSABase 


class ETL_Dates(SnowflakeRSABase):
    def __init__(self, params, etl_schema: str):
        super().__init__(params)
        self.etl_schema = etl_schema

    
    def get_cycle_parameters(self, tsp: str, credential: str) -> list:
        r = namedtuple('r', ['tsp', 'credential', 'tbl_cycl_id', 'cycl_run_id', 'cycl_date'])
        logger.info("Getting CDC Parameters")

        try:
            sql = f"""
                SELECT 
                    SPLIT(cycles.SOURCE_LOC, '/')[2]::varchar(100) tsp
                    ,SPLIT(cycles.SOURCE_LOC, '/')[3]::varchar(100) credential
                    ,runs.TBL_CYCL_ID
                    ,runs.CYCL_RUN_ID
                    ,dates.dte
                FROM {self.etl_schema}.ETL_CTRL_TBL_CYCL_EDAP cycles
                    JOIN {self.etl_schema}.ETL_CTRL_TBL_CYCL_RUN_EDAP runs ON cycles.TBL_CYCL_ID = runs.TBL_CYCL_ID
                    LEFT JOIN (
                            SELECT DATEADD(DAY, -SEQ4(), CURRENT_DATE()) AS dte
                            FROM TABLE(GENERATOR(ROWCOUNT=>100000))
                        ) dates ON dates.dte BETWEEN runs.CDC_STRT_TS::DATE AND runs.CDC_END_TS::DATE
                WHERE cycles.CYCL_NAME = 'telematics_ingest_raw_history'
                AND runs.STAT_CD = 'IN_PROG'
                AND LOWER(tsp) = LOWER(TRIM('{tsp}'))
                AND LOWER(credential) = LOWER(TRIM('{credential}'))--
            """

            if not (res := self.select_all(sql)) or res is None:
                raise Exception(f"No CDC Information returned")

            return [r(*row) for row in res]
        except Exception as ex:
            logger.error(f"{ex}")
            return []


    def recreate_stage(self, stage_info: tuple, sa_name: str, sa_container: str, sa_token: str, tsp: str, credential: str, dte: datetime) -> int:
        try:
            stage_name, stage_schema, file_format, file_format_schema, comment = stage_info
            url = f"azure://{sa_name}.blob.core.windows.net/{sa_container}/{tsp}/{credential}/Prt_Year_nbr={dte.year}/Prt_month_nbr={dte.month:02d}/Prt_day_nbr={dte.day:02d}"
            sql = f"""
                CREATE OR REPLACE STAGE {stage_schema}.{stage_name}
                    URL = '{url}'
                    CREDENTIALS = (AZURE_SAS_TOKEN = '{sa_token}')
                    FILE_FORMAT = '{file_format_schema}.{file_format}'
                COMMENT = '{comment}'
            """

            logger.info(f"Recreating the stage '{stage_schema}.{stage_name}'")

            if (res := self.execute(sql)) is None:
                raise Exception(f"Failed to recreate the stage")

            return res
        except Exception as ex:
            logger.error(f"{ex}")
            return None


    def copy_from_stage(self, stage_info: tuple, cycl_run_id: int) -> int:
        try:
            stage_name, stage_schema, file_format, file_format_schema, comment = stage_info
            sql = f"""
                COPY INTO {self.schema}.TEL_STG_TELEMATICS_DATA_RAW(RAW_JSON_DATA, SOURCE_FILE_NAME, SOURCE_SYSTEM, ETL_JOB_NM, CYCL_RUN_ID)
                FROM (SELECT $1, metadata$filename, 'Blob', 'Python Script', {cycl_run_id} FROM @{stage_schema}.{stage_name});
            """

            logger.info(f"Copying data from the stage '{stage_schema}.{stage_name}'")

            if (res := self.execute(sql)) is None:
                raise Exception(f"Failed to copy data from the stage")

            return res
        except Exception as ex:
            logger.error(f"{ex}")
            return None


    def close_cycle(self, tbl_cycl_id: int, cycl_run_id: int, rows: int) -> int:
        try:
            sql = f"""
                UPDATE {self.etl_schema}.ETL_CTRL_TBL_CYCL_RUN_EDAP
                SET STAT_CD = 'SUCC', ROWS_READ = {rows}, ROWS_WRITTEN = {rows}
                WHERE TBL_CYCL_ID = {tbl_cycl_id}
                AND CYCL_RUN_ID = {cycl_run_id};
            """

            logger.info(f"Closing cycle")

            if (res := self.execute(sql)) is None:
                raise Exception(f"Failed to copy data from the stage")

            return res
        except Exception as ex:
            logger.error(f"{ex}")
            return None


def main():
    try:
        logger.info(f"Working Directory: '{os.getcwd()}'")
        if len(sys.argv)-1 < 3:
            raise Exception("Not enough paramteres passed")
        tsp_param, credential_param, action_param = sys.argv[1].strip(), sys.argv[2].strip(), sys.argv[3].lower().strip()
        stage_config = config['snowflake']['stage'][env]
        stage_config['stage_name'] = f"{stage_config['stage_name']}_{tsp_param}_{credential_param}"


        dbase = ETL_Dates(config['snowflake']['credentials'][env].values(), config['snowflake']['schemas']['etl_schema'])
        if not(cycles := dbase.get_cycle_parameters(tsp_param, credential_param)):
            raise Exception(f"No cycle information available")
        logger.info(f"Retreived cycle information")

        source_name, source_container, source_token = config['storage_account'][env]['source'].values()
        landing_name, landing_container, landing_token = config['storage_account'][env]['landing'].values()
        target_name, target_container, target_token = config['storage_account'][env]['target'].values()
        
        for cycle in cycles:
            source_url = f"https://{source_name}.blob.core.windows.net/{source_container}/{cycle.tsp}/{cycle.credential}/Prt_Year_nbr={cycle.cycl_date.year}/Prt_month_nbr={cycle.cycl_date.month:02d}/Prt_day_nbr={cycle.cycl_date.day:02d}/{source_token}"
            logger.info(f"Copying: '{source_name}/{source_container}/{cycle.tsp}/{cycle.credential}/Prt_Year_nbr={cycle.cycl_date.year}/Prt_month_nbr={cycle.cycl_date.month:02d}/Prt_day_nbr={cycle.cycl_date.day:02d}'")
            landing_url = f"https://{landing_name}.blob.core.windows.net/{landing_container}/{cycle.tsp}/{cycle.credential}/Prt_Year_nbr={cycle.cycl_date.year}/Prt_month_nbr={cycle.cycl_date.month:02d}/Prt_day_nbr={cycle.cycl_date.day:02d}/{landing_token}"
            target_url = f"https://{target_name}.blob.core.windows.net/{target_container}/{cycle.tsp}/{cycle.credential}/Prt_Year_nbr={cycle.cycl_date.year}/Prt_month_nbr={cycle.cycl_date.month:02d}/Prt_day_nbr={cycle.cycl_date.day:02d}/{target_token}"
            
            cmd_target = subprocess.run(["azcopy", "sync", f"{source_url}", f"{landing_url}", "--recursive=true"])
            if cmd_target.returncode != 0 :
                raise Exception(f"Source to landing command did not complete successfully")

            cmd_landing = subprocess.run(["azcopy", "sync", f"{landing_url}", f"{target_url}", "--recursive=true"])
            if cmd_landing.returncode != 0 :
                raise Exception(f"Landing to target command did not complete successfully")

            if action_param == 'copy_cmd':
                if (res_stage := dbase.recreate_stage(tuple(stage_config.values()), target_name, target_container, target_token, cycle.tsp, cycle.credential, cycle.cycl_date)) is None:
                    raise Exception(f"Unable to recreate the stage")
                elif res_stage == 1:
                    logger.info(f"Stage has been recreated successfully")
                    if (res_copy := dbase.copy_from_stage(tuple(stage_config.values()), cycle.cycl_run_id)) is None:
                        raise Exception(f"Unable to copy data from stage")

        if (res_close := dbase.close_cycle(cycles[0].tbl_cycl_id, cycles[0].cycl_run_id, res_copy)) is None:
            raise Exception(f"Unable to close cycle")
        logger.debug(f"Close cycle rows: {res_close}")

        logger.info(f"Done loading")
        exit(0)
    except Exception as ex:
        logging.exception(f"{ex}")
        exit(1)


if __name__ == "__main__":
    with open(os.path.join(os.path.dirname(__file__), 'config.json'), "r") as configuration_file:
        config = json.load(configuration_file)
    env = config['environment']
    
    log_level = (10, 20)[config['log_level'].lower() == 'info']
    logger = logging.getLogger()
    logger.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s', '%m-%d-%Y %H:%M:%S')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)

    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)

    logger.info(f"Script is running in '{env}' environment")

    main()
