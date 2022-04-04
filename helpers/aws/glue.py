import logging
import polling2

from typing import Optional

from assertpy import assertpy

from configuration import CONFIG

logger = logging.getLogger('glue-poc')


class Glue:

    def __init__(self):
        self.client = CONFIG.aws_client.client('glue')

    def start_glue_job(self, job_name: str, arguments=None) -> Optional[str]:
        if arguments is None:
            arguments = {}
        try:
            response = self.client.start_job_run(
                JobName=job_name,
                Arguments=arguments)
        except Exception as e:
            logger.error(e)
            return None
        return response["JobRunId"]

    def get_job_run_status(self, job_name, job_run_id):
        try:
            response = self.client.get_job_run(
                JobName=job_name,
                RunId=job_run_id,
            )
            # logger.info(response)
        except Exception as e:
            logger.error(e)
            return None
        return response["JobRun"]["JobRunState"]

    def wait_for_job_to_complete(self, job_name, job_run_id):
        try:
            polling2.poll(lambda: self.__job_run_complete(job_name=job_name, job_run_id=job_run_id) == True,
                          timeout=600, step=20)
        except polling2.TimeoutException:
            assertpy.fail(f"Job {job_name} failed to complete the execution for run id {job_run_id}")

    def __job_run_complete(self, job_name, job_run_id):
        job_run_status = self.get_job_run_status(job_name=job_name, job_run_id=job_run_id)
        if job_run_status in ["SUCCEEDED", "FAILED"]:
            return True
        else:
            return False

    def get_table_schema(self, table_name, database_name):
        response = self.client.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        return response["Table"]["StorageDescriptor"]["Columns"]
