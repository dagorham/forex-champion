import time
import io
import pytz

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2

from datetime import datetime, timedelta


def check_date_viability(date_str):
    """
    Checks the viability of a date string.
    Date may be in the format XXXX/MM/DD, XXXX-MM-DD, or XXXX MM DD.

    Returns a datetime object, or raises a ValueError.

    :param date_str: user input string for the date
    :return: datetime object
    """

    possible_formats = ["%Y/%m/%d", "%Y-%m-%d", "%Y %m %d"]

    for date_format in possible_formats:
        try:
            return datetime.strptime(date_str, date_format)

        except ValueError:
            continue

    raise ValueError


def get_historical_data(instrument_name, params, pg_cursor, pg_conn):
    """
    Pulls exchange rate data for a currency over a certain time period.

    :param params: Dictionary of parameters, including instrument name, start and end dates,
                   and what values to pull. Start and end dates automatically set to None if
                   they are left blank or if they are in the wrong format.

    :param pg_cursor: postgres database cursor
    :param pg_conn: postgres database connection
    :return: pyplot figure.
    """

    # get all parameters in proper format and
    data_to_pull = ", ".join(params['values_to_show'])

    # validate both date blocks
    try:
        start_date = check_date_viability(params['start_date'])

    except ValueError:
        start_date = None

    try:
        end_date = check_date_viability(params['end_date'])

    except ValueError:
        end_date = None

    # only do anything if data was actually provided
    if data_to_pull:

        # create SQL query
        if start_date and end_date:
            SQL_query = "SELECT date, {} FROM {} WHERE date >= '{}' AND date <= '{}' ORDER BY date" \
             .format(data_to_pull, instrument_name, str(start_date), str(end_date))

        elif start_date:
            SQL_query = "SELECT date, {} FROM {} WHERE date >= '{}' ORDER BY date" \
             .format(data_to_pull, instrument_name, str(start_date))

        elif end_date:
            SQL_query = "SELECT date, {} FROM {} WHERE date <= '{}' ORDER BY date" \
             .format(data_to_pull, instrument_name, str(end_date))

        else:
            SQL_query = "SELECT date, {} FROM {} ORDER BY date" \
             .format(data_to_pull, instrument_name)

        try:
            # execute the query
            pg_cursor.execute(SQL_query)

            # format response into a list of lists
            data_series = list(zip(*list(pg_cursor)))

            return data_series

        except psycopg2.ProgrammingError:
            pg_conn.rollback()


def plot_one_data_series(axs, dates, data_series, instrument_name, values_to_show):
    # plot all data
    for data, data_name in zip(data_series[1:], values_to_show):
        axs.plot(dates, data, label=instrument_name.upper() + "_" + data_name)

    return axs


def plot_historical_data(params, pg_cursor, pg_conn):
    """
    Plots historical data for one or two currencies.

    :param params: Dictionary of parameters, including instrument name, start and end dates,
                   and what values to pull. Start and end dates automatically set to None if
                   they are left blank or if they are in the wrong format.
    :param pg_cursor: postgres cursor.
    :param pg_conn: postgrest connection.
    :return: pyplot figure
    """

    fig = plt.figure()
    axs = fig.add_subplot(1, 1, 1)

    # grab the data needed
    instrument_one = params['instrument_one']
    instrument_two = params['instrument_two']
    values_to_show = params['values_to_show']

    # grab data for first instrument
    instrument_one_data = get_historical_data(instrument_one, params, pg_cursor, pg_conn)

    # all plots will have the same independent variables
    dates = instrument_one_data[0]

    axs = plot_one_data_series(axs, dates, instrument_one_data, instrument_one, values_to_show)

    # only get data for second instrument if requested by the user
    if instrument_two != "no_thanks":
        instrument_two_data = get_historical_data(instrument_two, params, pg_cursor, pg_conn)
        axs = plot_one_data_series(axs, dates, instrument_two_data, instrument_two, values_to_show)

    # rotate the x axis labels
    for tick in axs.get_xticklabels():
        tick.set_rotation(45)

    axs.legend(fancybox=True, shadow=True)

    return fig


def dataframe_from_s3_csv(bucket, s3_dir):
    """
    Parses a CSV file to a DataFrame without writing to disk.

    :param s3_dir: directory to CSV file on S3
    :return: pandas dataframe
    """

    # connect to bucket and get the CSV
    key = bucket.get_key(s3_dir)
    CSV_as_string = key.get_contents_as_string()

    # pretend the string is a file
    fake_file = io.StringIO()
    fake_file.write(CSV_as_string.decode())
    fake_file.seek(0)

    return pd.read_csv(fake_file, header=None)


def clean_up_dataframe(df):
    """
    Cleans up a daily data DataFrame as pulled from S3.
    Adds columns, converts strings to floats, sets values as means for repeated time keys,
    and converts datetime string to datetime object.

    :param df: uncleaned dataframe
    :return: df: cleaned dataframe.
    """

    df.columns = ['instrument', 'datetime', 'bid', 'ask']

    df['bid'] = df['bid'].apply(float)
    df['ask'] = df['ask'].apply(float)

    df = df.groupby(['instrument', 'datetime']).mean().reset_index()

    df['datetime'] = df['datetime'].apply(lambda time_str: datetime.strptime(time_str, "%Y-%m-%d %H:%M"))

    return df


def time_convert(datetime_obj, zone_one='UTC', zone_two='US/Pacific'):
    """
    Converts a naive datetime object from zone_one to zone_two.
    Defaults to UTC -> Pacific

    :param datetime_obj: datetime object to convert
    :param zone_one: current time zone of the object
    :param zone_two: target time zone of the object
    :return: datetime object converted to the target timezone.
    """

    datetime_obj = pytz.timezone(zone_one).localize(datetime_obj)

    return datetime_obj.astimezone(pytz.timezone(zone_two))


def plot_daily_data(params, bucket):
    """
    Plots last 24 hours of exchange data for one or two currencies.

    :param params: Dictionary of parameters, including instrument name, start and end dates,
                   and what values to pull. Start and end dates automatically set to None if
                   they are left blank or if they are in the wrong format.
    :param bucket: S3 bucket where daily data is located
    :return: pyplot figure
    """

    instrument_one = params['instrument_one'].upper()
    instrument_two = params['instrument_two'].upper()

    timestamp_now = time.time()

    UTC_time_now = datetime.utcfromtimestamp(timestamp_now)

    UTC_time_one_day_ago = UTC_time_now - timedelta(days=1)

    fig = plt.figure()
    axs = fig.add_subplot(1, 1, 1)

    axs = plot_one_instrument_daily(axs, instrument_one, bucket, UTC_time_now, UTC_time_one_day_ago)

    if instrument_two != "NO_THANKS":
        axs = plot_one_instrument_daily(axs, instrument_two, bucket, UTC_time_now, UTC_time_one_day_ago)

    for tick in axs.get_xticklabels():
        tick.set_rotation(45)

    axs.legend()

    return fig


def plot_one_instrument_daily(axs, instrument_name, bucket, UTC_time_now, UTC_time_one_day_ago):
    """
    Plots data for the last 24 hours for a currency.

    :param axs: matplotlib axis object
    :param instrument_name: name of currency
    :param UTC_time_now: UTC time at the time function is called
    :param UTC_time_one_day_ago: UTC time one day before function was called
    :param bucket: S3 bucket where the daily data is located
    :return: new axs object with plot of the currency over past 24 hours.
    """

    # get s3 bucket with the data from today
    first_key = "{}/{}/{}/{}/TODAYS-DATA.csv"\
                .format(instrument_name, *UTC_time_now.strftime("%Y %m %d").split())

    # get s3 bucket with the data from yesterday
    second_key = "{}/{}/{}/{}/TODAYS-DATA.csv"\
                .format(instrument_name, *UTC_time_one_day_ago.strftime("%Y %m %d").split())

    # parse CSV files and clean up the dataframes
    df_one = dataframe_from_s3_csv(bucket, first_key)
    df_two = dataframe_from_s3_csv(bucket, second_key)

    df_one = clean_up_dataframe(df_one)
    df_two = clean_up_dataframe(df_two)

    # get rid of anything that's older than 24 hours ago
    df_two = df_two[df_two['datetime'] > UTC_time_one_day_ago]

    # concatenate the two dataframes and convert to local time
    final_df = pd.concat([df_two, df_one])
    final_df['datetime'] = final_df['datetime'].apply(lambda x: time_convert(x))

    x = final_df['datetime']
    y1 = final_df['bid']
    y2 = final_df['ask']

    # instantiate matplotlib objects and plot
    axs.plot(x, y1, label=instrument_name + "_bid")
    axs.plot(x, y2, label=instrument_name + "_ask")

    return axs
