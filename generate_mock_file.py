import argparse
import csv
import logging
import os
import random
import uuid

from datetime import datetime, timedelta, timezone

logger = logging.getLogger('file_generation')
csv_columns = ["trade_id", "account_key", "instrument_id", "action", "quantity", "price", "creation_date"]
instrument_id = ["DIVIDEND", "CALL", "PUT", "STOCK"]
action = ["SELL", "BUY"]


def generate_file(data_content, csv_file="generated_data.csv"):
    try:
        with open(os.path.join(os.getcwd(), "resources", csv_file), 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data_content in data_content:
                writer.writerow(data_content)
    except IOError as e:
        logger.error("Unable to write file")
        logger.error(e)


def get_valid_data(number_of_value_sets):
    dict_data = []
    for line in range(0, number_of_value_sets):
        creation_date = datetime.timestamp(datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 180)))
        transaction = {
            "trade_id": f"{uuid.uuid4()}",
            "account_key": f"{uuid.uuid4()}",
            "instrument_id": random.choice(instrument_id),
            "action": random.choice(action),
            "quantity": random.randint(1, 200),
            "price": round(random.uniform(5.00, 250.00), 2),
            "creation_date": datetime.fromtimestamp(creation_date)
        }
        dict_data.append(transaction)
    return dict_data


def get_data_for_account_with_large_amount_of_spending(number_of_value_sets):
    dict_data = []
    trade_id = f"{uuid.uuid4()}"
    amount_limit = 75000
    medium_value_per_set = amount_limit / number_of_value_sets
    for line in range(0, number_of_value_sets):
        quantity = random.randint(5, 50)
        medium_price = medium_value_per_set / quantity
        price = round(random.uniform(medium_price - 20, medium_price + 50), 2)
        creation_date = datetime.timestamp(datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 180)))
        transaction = {
            "trade_id": trade_id,
            "account_key": f"{uuid.uuid4()}",
            "instrument_id": random.choice(instrument_id),
            "action": random.choice(action),
            "quantity": quantity,
            "price": price,
            "creation_date": datetime.fromtimestamp(creation_date)
        }
        dict_data.append(transaction)
    return dict_data


def get_data_with_future_creation_date(number_of_value_sets):
    dict_data = []
    for line in range(0, number_of_value_sets):
        creation_date = datetime.timestamp(datetime.now(timezone.utc) + timedelta(days=random.randint(1, 3)))
        transaction = {
            "trade_id": f"{uuid.uuid4()}",
            "account_key": f"{uuid.uuid4()}",
            "instrument_id": random.choice(instrument_id),
            "action": random.choice(action),
            "quantity": random.randint(1, 200),
            "price": round(random.uniform(5.00, 250.00), 2),
            "creation_date": datetime.fromtimestamp(creation_date)
        }
        dict_data.append(transaction)
    return dict_data


def get_data_with_null_values(number_of_value_sets):
    dict_data = []
    for line in range(0, number_of_value_sets):
        creation_date = datetime.timestamp(datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 180)))
        instrument_id.append("")
        transaction = {
            "trade_id": f"{uuid.uuid4()}",
            "account_key": f"{uuid.uuid4()}",
            "instrument_id": random.choice(instrument_id),
            "action": random.choice(action),
            "quantity": random.randint(1, 200),
            "price": round(random.uniform(5.00, 250.00), 2),
            "creation_date": random.choice([datetime.fromtimestamp(creation_date), ""])
        }
        dict_data.append(transaction)
    return dict_data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--number_of_lines', type=int,
                        default=10,
                        help='The number of lines')
    parser.add_argument('--over_price_limit', type=bool,
                        default=False,
                        help='Generate a large amount of spending for the same account')
    parser.add_argument('--date_error', type=bool,
                        default=False,
                        help='Generate a date validation error')
    parser.add_argument('--file_name', type=str,
                        default="generated_data.csv",
                        help='File name')
    args = parser.parse_args()
    if args.over_price_limit:
        data = get_data_for_account_with_large_amount_of_spending(number_of_value_sets=args.number_of_lines)
        generate_file(data_content=data, csv_file="max_spend_limit.csv")
    elif args.date_error:
        data = get_data_with_future_creation_date(number_of_value_sets=args.number_of_lines)
        generate_file(data_content=data, csv_file="future_creation_date.csv")
    else:
        data = get_valid_data(number_of_value_sets=args.number_of_lines)
        generate_file(data_content=data, csv_file=args.file_name)
