import os

from datetime import datetime, date, timedelta
from decimal import Decimal

import psycopg2
import yaml

from pyspark import SparkContext


def get_yesterdays_date():
    """
    Provides yesterdays date at the time of calling.
    return: list, yesterdays_date in format [year, month, day]
    """

    return date.today() - timedelta(days=1)


def get_directory_path(date):
    """
    Takes date and currency and returns the associate S3 directory.

    param: currency - string, one of the valid currencies
    param: date - list, desired date as a list in format [year, month, day]

    return: S3 directory associated with the given currency on the given date.
    """

    directory = "s3a://forex-processed-data/*/{}/{}/{}/*".format(*date.strftime('%Y %m %d').split())

    return directory


def parse_csv_to_rdd(spark_context, csv_dir):
    """
    Parses a standard CSV to an RDD using spark context.
    """

    return spark_context.textFile(csv_dir).flatMap(lambda x: x.split("\n")).map(lambda x: x.split(","))


def get_opening_values(rdd):
    """
    return opening values grouped by currency

    input RDD format: key-value pairs

    key: currency
    value: (datetime, price)

    param: rdd - rdd we want to get the opening value for
    return: key value pair with the minimum datetime, grouped by currency
    """

    return rdd.reduceByKey(lambda x, y: min(x, y, key=lambda x: x[0][-2:])).collect()


def get_closing_values(rdd):
    """
    return closing values grouped by currency

    input RDD format: key-value pairs

    key: currency
    value: (datetime, price)

    param: rdd - rdd we want to get the opening value for
    return: key value pair with the maximum datetime
    """

    return rdd.reduceByKey(lambda x, y: max(x, y, key=lambda x: x[0][-2:])).collect()


def get_max_values(rdd):
    """
    return max values grouped by currency

    input RDD format: key-value pairs

    key: currency
    value: (datetime, price)

    param: rdd - rdd we want to get the opening value for
    return: key value pair with the maximum price
    """

    return rdd.reduceByKey(lambda x, y: max(x, y, key=lambda x: x[1])).collect()


def get_min_values(rdd):
    """
    return min values grouped by currency

    input RDD format: key-value pairs

    key: currency
    value: (datetime, price)

    param: rdd - rdd we want to get the opening value for
    return: key value pair with the minimum price
    """
    
    return rdd.reduceByKey(lambda x, y: max(x, y, key=lambda x: x[1])).collect()


def get_relevant_data_from_rdd(bid_rdd, ask_rdd):
    """
    Takes RDD of full daily (minute-to-minute) and returns the daily metrics.

    param: bid_ask_rdd - spark RDD of either bids or asks

    return:
    open_bid: opening bid
    open_ask: opening ask

    close_bid: closing bid
    close_ask: clsoing ask

    max_bid: maximum bid price over course of the day
    max_ask: maximum ask price over course of the day

    min_bid: minimum bid price over course of the day
    min_ask: minimum ask price over course of the day
    """

    open_bid = get_opening_values(bid_rdd)
    open_ask = get_opening_values(ask_rdd)

    max_bid = get_max_values(bid_rdd)
    max_ask = get_max_values(ask_rdd)

    min_bid = get_min_values(bid_rdd)
    min_ask = get_min_values(ask_rdd)

    close_bid = get_closing_values(bid_rdd)
    close_ask = get_closing_values(ask_rdd)

    return zip(open_bid, open_ask, max_bid, max_ask, min_bid, min_ask, close_bid, close_ask)


def push_to_database(instrument_data_list, date, conn):
    """
    Pushes the daily values for the currency to the database.
    """

    cursor = conn.cursor()

    for instrument_data in instrument_data_list:
        # first value to be inserted is the date
        db_vals = [date]

        instrument_name = instrument_data[0][0]

        # get all the data and convert to decimal
        # decimal is compatible with the numeric type in postgres
        open_bid = Decimal(instrument_data[0][1][1])
        open_ask = Decimal(instrument_data[1][1][1])

        max_bid = Decimal(instrument_data[2][1][1])
        max_ask = Decimal(instrument_data[3][1][1])

        min_bid = Decimal(instrument_data[4][1][1])
        min_ask = Decimal(instrument_data[5][1][1])

        close_bid = Decimal(instrument_data[6][1][1])
        close_ask = Decimal(instrument_data[7][1][1])

        # insert into DB and commit changes
        db_vals.extend([open_bid, open_ask, max_bid, max_ask, min_bid, min_ask, close_bid, close_ask])
        cursor.execute("INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)".format(instrument_name.lower()), db_vals)

        conn.commit()


def main(conn):
    # instantiate spark context
    sc = SparkContext()

    # get yesterdays date and the corresponding CSV on s3
    yesterdays_date = get_yesterdays_date()
    s3_dir = get_directory_path(yesterdays_date)

    # parse the CSV into a spark RDD
    daily_rdd = parse_csv_to_rdd(sc, s3_dir)
    
    # split rdd into bids and asks and cache them both
    bids_rdd = daily_rdd.map(lambda x: (x[0], (x[1], x[2]))).cache()
    asks_rdd = daily_rdd.map(lambda x: (x[0], (x[1], x[3]))).cache()

    all_instrument_data = get_relevant_data_from_rdd(bids_rdd, asks_rdd)

    push_to_database(all_instrument_data, yesterdays_date, conn)


if __name__ == "__main__":
    # get postgres credentials and connect to db
    credentials = yaml.load(open(os.path.expanduser('postgres_credentials.yml')))

    PG_ENDPOINT = 'forex-data.cql5yf8qc4xa.us-east-1.rds.amazonaws.com'
    PG_DB_NAME = credentials['postgres']['db_name']
    PG_USERNAME = credentials['postgres']['username']
    PG_PASSWORD = credentials['postgres']['password']

    pg_conn = psycopg2.connect(host=PG_ENDPOINT, user=PG_USERNAME, password=PG_PASSWORD, dbname=PG_DB_NAME)

    main(pg_conn)
