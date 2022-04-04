import logging
import polling2

from assertpy import assertpy

from configuration import CONFIG
from helpers.aws.s3_storage import S3Storage

logger = logging.getLogger('glue-poc')


class Athena:

    def __init__(self):
        self.athena_client = CONFIG.aws_client.client("athena")

    def save_all_data_from_glue_catalog(self, glue_table, glue_database):
        query_response = self.athena_client.start_query_execution(
            QueryString=f"SELECT * FROM {glue_table}",
            QueryExecutionContext={"Database": glue_database},
            ResultConfiguration={
                "OutputLocation": CONFIG.athena_bucket_resource_uri,
            },
        )
        self.wait_for_query_to_finish(query_execution_id=query_response['QueryExecutionId'])
        return query_response['QueryExecutionId']

    def download_results_and_process_them(self, query_execution_id, temp_file_name="athena_query_results.csv"):
        s3 = S3Storage(storage_name=CONFIG.athena_data_bucket)
        s3.download_file_from_storage(file_key=f"{query_execution_id}.csv",
                                      file_location=temp_file_name)

    def wait_for_query_to_finish(self, query_execution_id):
        try:
            polling2.poll(
                lambda:
                self.check_if_query_has_completed(query_execution_id=query_execution_id) is True,
                timeout=360,
                step=10)
        except polling2.TimeoutException:
            assertpy.fail("Query was not completed within the allowed time")

    def check_if_query_has_completed(self, query_execution_id):
        try:
            self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
            return True
        except Exception as err:
            if "not yet finished" in str(err):
                return False
            else:
                assertpy.fail(f"Error in the query execution: {err}")
