import logging
import os

from helpers.aws.s3_storage import S3Storage
from helpers.aws.glue import Glue
from configuration import CONFIG
from helpers.aws.athena import Athena

from pprint import pprint as pp

logger = logging.getLogger('glue-poc')

import pandas


def mapping_function(row):
    return row.quantity * row.price

def multiply_multiple_columns(row, columns):
    result = 1
    for column in columns:
        result = result * row[column]
    return result

if __name__ == '__main__':
    # File upload
    # s3 = S3Storage(CONFIG.data_bucket)
    # uploaded = s3.upload_file_with_metadata_to_storage("generated_data3.csv", metadata={"my": "test",
    #                                                                                     "source": "generated"})
    # print(uploaded)

    # Glue jobs
    # glue = Glue()
    # job_run_id = glue.start_glue_job(job_name=CONFIG.data_ingestion_job)
    # job_status = glue.get_job_run_status(job_name=CONFIG.data_ingestion_job,
    #                                    job_run_id=job_run_id)
    # job_status = glue.get_job_run_status(job_name=CONFIG.data_ingestion_job,
    #                                    job_run_id="jr_079304bfe4cd86aa5c0558025163bb7e7bd88bd675bf0152fa6f8f68301b927f")
    # print(job_run_id)
    # # print(job_status)
    # glue.wait_for_job_to_complete(job_name=CONFIG.data_ingestion_job, job_run_id=job_run_id)
    # job_status = glue.get_job_run_status(job_name=CONFIG.data_ingestion_job,
    #                                      job_run_id=job_run_id)
    # print(job_status)

    # Data catalog
    # get_data()

    # Glue tables
    # glue = Glue()
    # table = glue.get_table_schema(CONFIG.processed_data_table, CONFIG.glue_database)
    # pp(table)

    # local file load
    # local_file = pandas.read_csv(os.path.join(CONFIG.resource_path, "file_with_invalid_account_keys.csv"))
    # local_file = pandas.read_csv(os.path.join(CONFIG.resource_path, "generated_file_1645172550.csv"))
    # print(local_file)
    # print(local_file.info())
    # print(f"Columns \n {data.columns}")
    # print(f"Size \n {data.size}")
    # print(f"Info \n")
    # data.info()

    # Athena
    # athena = Athena()
    # execution_id = athena.save_all_data_from_glue_catalog(glue_table=CONFIG.processed_data_table,
    #                                                       glue_database=CONFIG.glue_database)
    # athena.download_results_and_process_them(query_execution_id=execution_id)
    # athena_query = pandas.read_csv("athena_query_results.csv")
    # print(athena_query)
    #
    # local_file_index = local_file.values[0][0]
    # print(local_file_index)
    #
    # index_in_athena_query = athena_query[athena_query["trade_id"] == local_file_index].index.values[0]
    # print(index_in_athena_query)
    #
    # df1 = athena_query.loc[index_in_athena_query:].reset_index(drop=True)
    # print(df1)
    #
    # df = local_file.isin(athena_query)
    # print(df)
    #
    # columns = df.columns.values.tolist()
    # for value in columns:
    #     series = df.index[df[value] == False].tolist()
    #     print(f"Column {value} with indexes: {series}")
    #     for index in series:
    #         print(f"Value in the local file on column {value} and index {index}: {local_file[value].values[index]} "
    #               f"is different from the value stored "
    #               f"on column {value} and index {index + index_in_athena_query}: {athena_query[value].values[index]}")
    # df = local_file["account_key"]
    # print(df)
    # import re
    #
    # uuid = "0d4cabb8-5b0b-4404-aca4-7beee87f6f3d"
    # regex = re.compile("^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z", re.I)
    # match = regex.match(uuid)
    # print(bool(match))
    #
    # print(df.str.fullmatch(regex))

    # TODO: account format check
    # from validators.dataframe_data_validator import check_account_key_matches_uuid_format
    #
    # check_account_key_matches_uuid_format(local_file)

    # TODO: aggregation check
    # initial_data = pandas.read_csv("athena_query_results.csv")
    # print(initial_data)
    # aggregated_data = pandas.read_csv("athena_query_results_aggregation.csv")
    # print(aggregated_data)
    #
    # df = pandas.DataFrame()
    # df["account_key"] = initial_data["account_key"]
    # df["total_value"] = initial_data.apply(lambda row: round(row.quantity * row.price, 13), axis=1)
    # print(df)
    #
    # aggregated_data = aggregated_data.sort_values(by="account_key").reset_index(drop=True)
    # df = df.sort_values(by="account_key").reset_index(drop=True)
    # print(aggregated_data)
    # print(df)
    # print(df.isin(aggregated_data))
    # df0 = df.isin(aggregated_data)
    #
    # columns = df0.columns.values.tolist()
    # for value in columns:
    #     series = df0.index[df0[value] == False].tolist()
    #     logging.error(f"Differences found on column {value} at indexes: {series}")
    #     for index in series:
    #         logging.error(f" on column {value} and index {index}: {df[value].values[index]} "
    #                       f"is different from the value stored on column {value} and "
    #                       f"index {index}: {aggregated_data[value].values[index]}")

    #TODO: filter dataset
    # aggregated_data = pandas.read_csv("athena_query_results_aggregation.csv")
    # filtered_data = aggregated_data[aggregated_data["total_value"] > 10000.0]
    # print(filtered_data)

    # print all the dataframe
    pandas.set_option("display.max_rows", None, "display.max_columns", None)
    # print(initial_data)

    # broken file load
    # local_file = pandas.read_csv(os.path.join(CONFIG.resource_path, "broken_file.csv"))
    # print(local_file)
    # df = local_file.isnull()
    # print(df)
    # columns = local_file.columns.values.tolist()
    # for value in columns:
    #     series = df.index[df[value] == True].tolist()
    #     # logging.error(f"Differences found on column {value} at indexes: {series}")
    #     for index in series:
    #         logging.error(f"Null value on column {value} and index {index}: {local_file[value].values[index]} ")

    initial_data = pandas.read_csv("athena_query_results.csv")
    initial_data["price2"] = initial_data["price"]
    initial_data["price3"] = initial_data["price"]
    initial_data["price4"] = initial_data["price"]
    initial_data["price5"] = initial_data["price"]
    print(initial_data)
    print(initial_data.columns.values.tolist())
    df = pandas.DataFrame()
    df["account_key"] = initial_data["account_key"]
    df["total_value"] = initial_data.apply(mapping_function, axis=1)
    df["total_value"] = initial_data.apply(lambda row: multiply_multiple_columns(row,
                                                                                 ["price",
                                                                                  "price2",
                                                                                  "price3",
                                                                                  "price4",
                                                                                  "price5"]),
                                           axis=1)
    print(df)

