from assertpy import assertpy, assert_that

expected_table_schema = [{'Name': 'trade_id', 'Type': 'string'},
                         {'Name': 'account_key', 'Type': 'string'},
                         {'Name': 'instrument_id', 'Type': 'string'},
                         {'Name': 'action', 'Type': 'string'},
                         {'Name': 'quantity', 'Type': 'bigint'},
                         {'Name': 'price', 'Type': 'double'},
                         {'Name': 'creation_date', 'Type': 'string'}]


def validate_table_schema(table_schema):
    assert_that(len(table_schema)).is_equal_to(len(expected_table_schema))
    for column in table_schema:
        assert_that(column in expected_table_schema).is_true()
