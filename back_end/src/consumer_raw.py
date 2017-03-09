"""
Kinesis streams consumer for data lake.

This will dump the exchange rate data into S3 with no pre processing.

The data will be written to a file on S3 with the format:

s3://forex-data-raw/INSTRUMENT_NAME/YEAR/MONTH/DAY/UNIX_TIME.txt
"""


import time
import json

import boto

from datetime import datetime

from boto import kinesis

from kinesis import KinesisConsumer


class ForexConsumerRaw(KinesisConsumer):
    def __init__(self, client, sleep_period, shard_id, stream_name, record_limit, begin_read, bucket):
        super(ForexConsumerRaw, self).__init__(client, sleep_period, shard_id, stream_name, record_limit, begin_read)
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
        Minimal preprocessing. Just convert from JSON to string so it can be written to S3.

        :param record: JSON object for record
        :return: record converted to string
        """

        return json.dumps(record)

    def send_records(self, processed_records):
        """
        Writes a group of records to S3 as one large JSON file.

        :param processed_records: list of processed records
        :return: void
        """

        # get date for folder names
        record_time = datetime.fromtimestamp(time.time()).strftime("%Y %m %d").split()

        all_records_together = "\n".join(processed_records)

        # new key is FOREX-RAW-DATA with the UNIX epoch time appended
        new_key_name = "{}/{}/{}/FOREX-RAW-DATA-{UNIX_TIME}".format(*record_time, UNIX_TIME=str(int(time.time())))

        # write everything to s3
        key = self.bucket.new_key(new_key_name)
        key.set_contents_from_string(all_records_together)


def main():
    # necessary AWS connections
    s3_client = boto.connect_s3()
    BUCKET = s3_client.get_bucket("forex-raw-data")
    CLIENT = kinesis.connect_to_region("us-east-1")

    # define kinesis constants
    SHARD_ID = "shardId-000000000000"
    STREAM_NAME = "forex_stream"
    RECORD_LIMIT = 11
    BEGIN_READ = "filler"
    SLEEP_PERIOD = 60

    forex_consumer_raw = ForexConsumerRaw(CLIENT, SLEEP_PERIOD, SHARD_ID, STREAM_NAME, RECORD_LIMIT, BEGIN_READ, BUCKET)

    forex_consumer_raw.run()

if __name__ == '__main__':
    main()