"""
Kinesis streams consumer with some preprocessing.

This will take the exchange rate JSON object and extract the instrument name,
time stamp (converted from UNIX epoch to Pacific Time), the bid price, and
the ask price.

The data will be written to a file on S3 with the format:

s3://forex-data-processed/INSTRUMENT_NAME/YEAR/MONTH/DAY
"""

import json

from datetime import datetime

import boto

from boto import kinesis

from kinesis import KinesisConsumer


class ForexConsumerProcessed(KinesisConsumer):

    def __init__(self, client, sleep_period, shard_id, stream_name, record_limit, begin_read, bucket):
        super(ForexConsumerProcessed, self).__init__(client, sleep_period, shard_id, stream_name, record_limit, begin_read)
        self.bucket = bucket

    def check_record_validity(self, record):
        """
        Checks a record's validity.

        :param record: Kinesis record to check
        :return: True if it's valid, False if it's not
        """

        try:
            # the prices key won't exist if the record is bad
            json.loads(record['Data'])['prices'][0]
            return True

        except KeyError:
            return False

    def process_record(self, record):
        """
        Takes the json object returned by the API endpoint and returns it in the format we want in our database.

        :param record: kinesis record
        :return list with format [ticker_name, rate_time, bid, ask]
        """

        exchange_data_json = json.loads(record['Data'])

        instrument_data = exchange_data_json['prices'][0]

        # pull out the relevant data
        name = instrument_data['instrument']
        rate_time = instrument_data['time']
        bid = instrument_data['bid']
        ask = instrument_data['ask']

        # we want all currencies in USD
        if name[:3] == 'USD':
            bid = 1 / bid
            ask = 1 / ask
            ticker_name = name[4:]

        else:
            ticker_name = name[:3]

        return ticker_name, rate_time, bid, ask

    def send_record(self, processed_record):
        """
        Appends processed record to the CSV on S3.

        :param processed_record: instrument data in format [instrument_name, unix_time, bid_price, ask_price]
        :return: void
        """

        # pull out the name of the instrument
        instrument_name = processed_record[0]
        UNIX_time = processed_record[1]

        # get time
        record_time = datetime.fromtimestamp(int(int(UNIX_time) / 1000000)).strftime("%Y-%m-%d %H:%M")
        time_list = record_time.split()[0].split("-")

        # convert to normal time in the list
        instrument_data_list = list(processed_record)
        instrument_data_list[1] = record_time

        # format instrument data to CSV
        instrument_data = "{},{},{},{}\n".format(*instrument_data_list)

        # create key path in format INSTRUMENT_NAME/YEAR/MONTH/DAY
        key_path = "{}/{}/{}/{}/TODAYS-DATA.csv".format(instrument_name, *time_list)

        # get key
        todays_data_file = self.bucket.get_key(key_path)

        # check if the data file exists already
        if todays_data_file:
            # get all of todays data as a string
            todays_data_as_string = todays_data_file.get_contents_as_string()

            # append the new data to it
            todays_data_as_string += instrument_data

            # push the new data to s3
            todays_data_file.set_contents_from_string(todays_data_as_string)

        else:
            # create a new data file
            todays_data_file = self.bucket.new_key(key_path)

            # start it with the bit of data
            todays_data_file.set_contents_from_string(instrument_data)

    def send_records(self, processed_records):
        for record in processed_records:
            self.send_record(record)


def main():
    # necessary AWS connections
    s3_client = boto.connect_s3()
    BUCKET = s3_client.get_bucket("forex-processed-data")
    CLIENT = kinesis.connect_to_region("us-east-1")

    # define kinesis constants
    SHARD_ID = "shardId-000000000000"
    STREAM_NAME = "forex_stream"
    RECORD_LIMIT = 11
    BEGIN_READ = "filler"
    SLEEP_PERIOD = 60

    forex_consumer_processed = ForexConsumerProcessed(CLIENT, SLEEP_PERIOD, SHARD_ID, STREAM_NAME, RECORD_LIMIT, BEGIN_READ, BUCKET)

    forex_consumer_processed.run()

if __name__ == '__main__':
    main()
