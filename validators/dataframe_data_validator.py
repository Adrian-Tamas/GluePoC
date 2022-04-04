import pandas
from assertpy import assert_that

import logging
import re

logger = logging.getLogger('glue-poc')


def check_that_dataframe_is_contained_within_another_dataframe(dataframe_to_check,
                                                               dataframe_to_check_against,
                                                               filename):
    # Get the first vale from the file
    local_file_index = dataframe_to_check.values[0][0]

    # Find the value in the Athena dataframe and get the index
    index_in_athena_query = \
        dataframe_to_check_against[dataframe_to_check_against["trade_id"] == local_file_index].index.values[0]

    # Create a slice of the Athena frame starting from the index and reset the index values
    filtered_athena_dataframe = dataframe_to_check_against.loc[index_in_athena_query:].reset_index(drop=True)
    # filtered_athena_dataframe = dataframe_to_check_against.loc[index_in_athena_query - 3:].reset_index(drop=True) # Show errors in report

    # Check that the values from the local dataframe are all present in the filtered dataframe
    df = dataframe_to_check.isin(filtered_athena_dataframe)

    columns = df.columns.values.tolist()
    for value in columns:
        series = df.index[df[value] == False].tolist()
        logging.error(f"Differences found on column {value} at indexes: {series}")
        for index in series:
            logging.error(f"Value in the test file {filename}"
                          f" on column {value} and index {index}: {dataframe_to_check[value].values[index]} "
                          f"is different from the value stored on column {value} and "
                          f"index {index + index_in_athena_query}: {dataframe_to_check_against[value].values[index]}")
    assert_that(df.eq(True).all().all()).is_true()


def check_account_key_matches_uuid_format(dataframe):
    s = dataframe["account_key"]
    regex = re.compile(r"^[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z", re.I)
    match_series = s.str.fullmatch(regex)
    index_list = match_series.index[match_series == False].tolist()
    for index in index_list:
        logging.error(f"Value at index {index}: {dataframe['account_key'].values[index]} does not match the format")
    assert_that(match_series.eq(True).all()).is_true()


def check_total_value_aggregations_are_correct(initial_data, aggregation_data):
    df = pandas.DataFrame()
    df["account_key"] = initial_data["account_key"]
    df["total_value"] = initial_data.ap


    aggregation_data = aggregation_data.sort_values(by="account_key").reset_index(drop=True)
    df = df.sort_values(by="account_key").reset_index(drop=True)
    df_isin_results = df.isin(aggregation_data)

    columns = df_isin_results.columns.values.tolist()
    for value in columns:
        series = df_isin_results.index[df_isin_results[value] == False].tolist()
        logging.error(f"Differences found on column {value} at indexes: {series}")
        for index in series:
            logging.error(f"Calculated value on column {value} and index {index}: {df[value].values[index]} "
                          f"is different from the value stored on column {value} and "
                          f"index {index}: {aggregation_data[value].values[index]}")
    assert_that(df_isin_results.eq(True).all().all()).is_true()
