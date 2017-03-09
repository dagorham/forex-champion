"""

Producer for Forex Kinesis stream.

This will spawn 11 processes and pull exchange data from the Oanda API.
The JSON response will be pushed to the Kinesis stream "forex_stream",
where it will then be consumed by two consumers, one that pre-processes
the data and another that simply dumps it to S3.

SUPPORTED CURRENCIES:

- Euro
- Canadian Dollar
- Peso
- Pound
- Australian Dollar
- Japanese Yen
- Chinese Yuan
- Indian Rupee
- Saudi Arabian Riyal
- South African Rand
- Gold

"""


import os
import json
import time

import yaml
import requests

from boto import kinesis
from pathos.multiprocessing import ProcessingPool as Pool

from kinesis import KinesisProducer


class ForexProducer(KinesisProducer):
    def __init__(self, client, stream_name, partition_key, currencies, multi_pool):
        super(ForexProducer, self).__init__(client, stream_name, partition_key)
        self.currencies = currencies
        self.multi_pool = multi_pool
        self.credentials = yaml.load(open(os.path.expanduser('../../credentials.yml')))
        self.API_KEY = self.credentials['oanda']['api_key']
        self.request_header = {'Authorization': 'Bearer {}'.format(self.API_KEY), 'X-Accept-Datetime-Format': 'UNIX'}

    def get_exchange_rate(self, currency_pair):
        """
        Returns the exchange rate for a given currency pair from the Oanda API.

        param: currency_pair - pair in format XXX_XXX is the three letter code for some currency
        param: request_header - header with the api_key
        return: json object with time and bid/ask
        """

        api_endpoint = "https://api-fxpractice.oanda.com/v1/prices?instruments={}".format(currency_pair)

        return requests.get(api_endpoint, headers=self.request_header).text

    def pull(self):
        exchange_data = self.multi_pool.map(self.get_exchange_rate, self.currencies)

        for exchange_rate in exchange_data:
            yield json.dumps(json.loads(exchange_rate))

        time.sleep(60)


def main():
    kinesis_client = kinesis.connect_to_region("us-east-1")
    stream_name = "forex_stream"
    partition_key = "filler"

    # instantiate multiprocessing object
    multi_pool = Pool(11)

    # get currency exchange rates
    currencies = ['EUR_USD', 'USD_CAD', 'USD_MXN', 'GBP_USD', 'AUD_USD',
                  'USD_JPY', 'USD_CNH', 'USD_INR', 'USD_SAR', 'USD_ZAR', 'XAU_USD']

    forex_producer = ForexProducer(kinesis_client, stream_name, partition_key, currencies, multi_pool)

    forex_producer.run()

if __name__ == '__main__':
    main()