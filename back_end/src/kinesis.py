"""
Abstract classes for Kinesis consumers and producers.
"""

import time

from abc import ABCMeta, abstractmethod


class KinesisProducer:
    __metaclass__ = ABCMeta

    def __init__(self, client, stream_name, partition_key):
        # boto kinesis client object
        self.client = client
        # stream name to send data to
        self.stream_name = stream_name
        # key to partition data on
        self.partition_key = partition_key

    @abstractmethod
    def pull(self):
        pass

    def run(self):
        while True:
            for record in self.pull():
                self.client.put_record(self.stream_name, record, self.partition_key)


class KinesisConsumer:
    __metaclass__ = ABCMeta

    def __init__(self, client, sleep_period, shard_id, stream_name, record_limit, begin_read):
        # boto kinesis client
        self.client = client
        # how long to wait between pulls from the stream
        self.sleep_period = sleep_period
        # shard id to read from
        self.shard_id = shard_id
        # name of kinesis stream to pull from
        self.stream_name = stream_name
        # number of records to pull each time
        self.record_limit = record_limit
        # where to begin reading
        self.begin_read = begin_read
        # first shard iterator
        self.shard_iterator = self.client.get_shard_iterator(self.stream_name, self.shard_id, "LATEST")["ShardIterator"]

    def pull(self):
        """
        Pulls from the defined Kinesis stream. Checks the validity of the record, processes it,
        and then yields control to the run() method.

        :return: yields processed record
        """

        try:
            # get output from stream
            output = self.client.get_records(self.shard_iterator, self.record_limit)

            # get the next ShardIterator
            self.shard_iterator = output['NextShardIterator']

            for record in output['Records']:
                # check the record validity
                if self.check_record_validity(record):
                    # yield to the main run method
                    yield self.process_record(record)

            # pull every 60 seconds
            time.sleep(self.sleep_period)

        except ProvisionedThroughputExceededException:
            # wait another 30 seconds if AWS gets mad at us
            time.sleep(30)

    def check_record_validity(self, record):
        """
        Check the validity of a record. If the record is valid, return it.
        If it is not, return True, if it is not, return False. Defaults to valid
        if not overridden.

        :param record:
        :return: True if false, False if not, True by default if not overridden by subclass.
        """

        return True

    @abstractmethod
    def process_record(self, record):
        """
        Method to process records as they come in from Kinesis.

        :param record: Kinesis record
        :return: processed record
        """

        pass

    @abstractmethod
    def send_records(self, processed_records):
        """
        Method that defines what to do with the processed records, such as
        writing them to disk, inserting them into a database, or sending them to
        another kinesis_stream

        :param processed_records: list of records processed by the record_processor method
        """

        pass

    def run(self):
        """
        Runs the Kinesis consumer.

        Continuously loops on the pull() generator which returns records
        from the Kinesis stream, processes them, and sends them on their way
        as defined.

        :return: void
        """

        while True:
            processed_records = []

            for processed_record in self.pull():
                processed_records.append(processed_record)

                self.send_records(processed_records)