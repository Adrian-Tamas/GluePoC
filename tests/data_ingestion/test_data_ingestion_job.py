import logging
import os

import pandas
import pytest

from assertpy import assertpy, assert_that
from time import time

from generate_mock_file import get_valid_data, generate_file, get_data_with_null_values
from configuration import CONFIG
from helpers.aws.athena import Athena
from helpers.aws.glue import Glue
from helpers.aws.s3_storage import S3Storage
from validators.data_ingestion_table_validator import validate_table_schema
from validators.dataframe_data_validator import (check_that_dataframe_is_contained_within_another_dataframe,
                                                 check_account_key_matches_uuid_format,
                                                 check_total_value_aggregations_are_correct)

logger = logging.getLogger('glue-poc')


class TestDataIngestionJob:

    @pytest.mark.skip()
    def test_job_execution(self):
        s3 = S3Storage(CONFIG.data_bucket)
        uploaded = s3.upload_file_to_storage("generated_data3.csv")
        assert_that(uploaded).is_equal_to(True)

        glue = Glue()
        job_run_id = glue.start_glue_job(job_name=CONFIG.data_ingestion_job)
        glue.wait_for_job_to_complete(job_name=CONFIG.data_ingestion_job, job_run_id=job_run_id)
        job_status = glue.get_job_run_status(job_name=CONFIG.data_ingestion_job,
                                             job_run_id=job_run_id)
        assert_that(job_status).is_equal_to("SUCCEEDED")

    @pytest.mark.skip()
    def test_table_schema(self):
        glue = Glue()
        table_schema = glue.get_table_schema(CONFIG.processed_data_table, CONFIG.glue_database)
        validate_table_schema(table_schema)

    @pytest.mark.skip()
    def test_number_of_items_processed(self):
        # Generate file
        s3 = S3Storage(CONFIG.data_bucket)
        file_name = f"generated_file_{int(time())}.csv"
        data = get_valid_data(number_of_value_sets=13)
        generate_file(data_content=data, csv_file=file_name)

        # Get existing row count
        athena = Athena()
        execution_id = athena.save_all_data_from_glue_catalog(glue_table=CONFIG.processed_data_table,
                                                              glue_database=CONFIG.glue_database)
        athena.download_results_and_process_them(query_execution_id=execution_id)
        data = pandas.read_csv("athena_query_results.csv")
        existing_row_count = data.shape[0]
        logging.info(f"Existing row count: {existing_row_count}")

        # Upload the file
        uploaded = s3.upload_file_to_storage(file_name)
        assert_that(uploaded).is_equal_to(True)

        # Trigger the glue job
        glue = Glue()
        job_run_id = glue.start_glue_job(job_name=CONFIG.data_ingestion_job)
        glue.wait_for_job_to_complete(job_name=CONFIG.data_ingestion_job, job_run_id=job_run_id)
        job_status = glue.get_job_run_status(job_name=CONFIG.data_ingestion_job,
                                             job_run_id=job_run_id)
        assert_that(job_status).is_equal_to("SUCCEEDED")

        # Get the current row count
        execution_id = athena.save_all_data_from_glue_catalog(glue_table=CONFIG.processed_data_table,
                                                              glue_database=CONFIG.glue_database)
        athena.download_results_and_process_them(query_execution_id=execution_id)
        data = pandas.read_csv("athena_query_results.csv")
        current_row_count = data.shape[0]
        assert_that(13).is_equal_to(current_row_count - existing_row_count)

        # Check that all values have been processed correctly
        local_file = pandas.read_csv(os.path.join(CONFIG.resource_path, file_name))

        check_that_dataframe_is_contained_within_another_dataframe(dataframe_to_check=local_file,
                                                                   dataframe_to_check_against=data,
                                                                   filename=file_name)

    @pytest.mark.skip()
    def test_data_format(self):
        athena = Athena()
        execution_id = athena.save_all_data_from_glue_catalog(glue_table=CONFIG.processed_data_table,
                                                              glue_database=CONFIG.glue_database)
        athena.download_results_and_process_them(query_execution_id=execution_id)
        data = pandas.read_csv("athena_query_results.csv")
        check_account_key_matches_uuid_format(data)

    @pytest.mark.skip()
    def test_failing_data_format_test(self):
        local_file = pandas.read_csv(os.path.join(CONFIG.resource_path, "file_with_invalid_account_keys.csv"))
        check_account_key_matches_uuid_format(local_file)

    @pytest.mark.skip()
    def test_total_value_aggregation(self):
        # Generate file
        s3 = S3Storage(CONFIG.data_bucket)
        file_name = f"generated_file_{int(time())}.csv"
        data = get_valid_data(number_of_value_sets=13)
        # data = get_data_with_null_values(number_of_value_sets=13)
        generate_file(data_content=data, csv_file=file_name)

        # Upload the file
        uploaded = s3.upload_file_to_storage(file_name)
        assert_that(uploaded).is_equal_to(True)

        # Trigger the glue job
        glue = Glue()
        job_run_id = glue.start_glue_job(job_name=CONFIG.data_ingestion_job)
        assert_that(job_run_id).is_not_none()
        glue.wait_for_job_to_complete(job_name=CONFIG.data_ingestion_job, job_run_id=job_run_id)
        job_status = glue.get_job_run_status(job_name=CONFIG.data_ingestion_job,
                                             job_run_id=job_run_id)
        assert_that(job_status).is_equal_to("SUCCEEDED")

        # Trigger the glue job
        glue = Glue()
        job_run_id = glue.start_glue_job(job_name=CONFIG.total_value_aggregation_job)
        glue.wait_for_job_to_complete(job_name=CONFIG.total_value_aggregation_job, job_run_id=job_run_id)
        job_status = glue.get_job_run_status(job_name=CONFIG.total_value_aggregation_job,
                                             job_run_id=job_run_id)
        assert_that(job_status).is_equal_to("SUCCEEDED")

        # Get the initial data
        athena = Athena()
        execution_id = athena.save_all_data_from_glue_catalog(glue_table=CONFIG.processed_data_table,
                                                              glue_database=CONFIG.glue_database)
        athena.download_results_and_process_them(query_execution_id=execution_id)
        initial_data = pandas.read_csv("athena_query_results.csv")

        # Get aggregate data
        athena = Athena()
        execution_id = athena.save_all_data_from_glue_catalog(glue_table=CONFIG.total_value_aggregation_table,
                                                              glue_database=CONFIG.glue_database)
        athena.download_results_and_process_them(query_execution_id=execution_id,
                                                 temp_file_name="athena_query_results_aggregation.csv")
        aggregated_data = pandas.read_csv("athena_query_results_aggregation.csv")
        check_total_value_aggregations_are_correct(initial_data=initial_data,
                                                   aggregation_data=aggregated_data)

    # @pytest.mark.skip()
    def test_null_values_in_input_file(self):
        local_file = pandas.read_csv(os.path.join(CONFIG.resource_path, "broken_file.csv"))
        df = local_file.isnull()
        columns = local_file.columns.values.tolist()
        for value in columns:
            series = df.index[df[value] == True].tolist()
            for index in series:
                logging.error(f"Null value in file broken_file.csv on column {value} "
                              f"and index {index}: {local_file[value].values[index]} ")
        assert_that(df.values.all()).is_false()

    # @pytest.mark.skip()
    def test_filter_data_in_dataframe(self):
        aggregated_data = pandas.read_csv("athena_query_results_aggregation.csv")
        print(f"\nInitial data rows count: {aggregated_data.shape[0]}")
        filtered_data = aggregated_data[aggregated_data["total_value"] > 10000.0]
        print(filtered_data)