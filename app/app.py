import os

import yaml
import psycopg2
import boto

import plots

from spyre import server


class ForexApp(server.App):
    def __init__(self):
        self.data_cache = None
        self.param_cache = None

        credentials = yaml.load(open(os.path.expanduser('../postgres_credentials.yml')))

        PG_ENDPOINT = 'forex-data.cql5yf8qc4xa.us-east-1.rds.amazonaws.com'
        PG_DB_NAME = credentials['postgres']['db_name']
        PG_USERNAME = credentials['postgres']['username']
        PG_PASSWORD = credentials['postgres']['password']

        self.pg_conn = psycopg2.connect(host=PG_ENDPOINT, user=PG_USERNAME, password=PG_PASSWORD, dbname=PG_DB_NAME)
        self.pg_cursor = self.pg_conn.cursor()

        s3_conn = boto.connect_s3()

        self.bucket = s3_conn.get_bucket('forex-processed-data')

    inputs = [{"type": "dropdown",
               "label": "Currency One",
               "options": [{"label": "Euro (EU)", "value": "eur"},
                           {"label": "Pound (Great Britain)", "value": "gbp"},
                           {"label": "Peso (Mexico)", "value": "mxn"},
                           {"label": "Yen (Japan)", "value": "jpy"},
                           {"label": "Dollar (Australia)", "value": "aud"},
                           {"label": "Dollar (Canada)", "value": "cad"},
                           {"label": "Rupee (India)", "value": "inr"},
                           {"label": "Yuan (China)", "value": "cnh"},
                           {"label": "Riyal (Saudi Arabia)", "value": "sar"},
                           {"label": "Rand (South Africa)", "value": "zar"},
                           {"label": "Gold", "value": "xau"}],
               "key": "instrument_one"},

              {"type": "dropdown",
               "label": "Currency Two",
               "options": [{"label": "None", "value": "no_thanks"},
                           {"label": "Euro (EU)", "value": "eur"},
                           {"label": "Pound (Great Britain)", "value": "gbp"},
                           {"label": "Peso (Mexico)", "value": "mxn"},
                           {"label": "Yen (Japan)", "value": "jpy"},
                           {"label": "Dollar (Australia)", "value": "aud"},
                           {"label": "Dollar (Canada)", "value": "cad"},
                           {"label": "Rupee (India)", "value": "inr"},
                           {"label": "Yuan (China)", "value": "cnh"},
                           {"label": "Riyal (Saudi Arabia)", "value": "sar"},
                           {"label": "Rand (South Africa)", "value": "zar"},
                           {"label": "Gold", "value": "xau"}],
               "key": "instrument_two"},

              {"type": "dropdown",
               "label": "Data Range",
               "options": [{"label": "Last 24 Hours", "value": "daily"},
                           {"label": "Historical", "value": "historical"}],
               "key": "data_range"},

              {"type": "text",
               "key": "start_date",
               "label": "Starting Date"},

              {"type": "text",
               "key": "end_date",
               "label": "Ending Date"},

              {"type": "checkboxgroup",
               "label": "Values to Show",
               "options": [{"label": "Opening Bid", "value": "open_bid"},
                           {"label": "Opening Ask", "value": "open_ask"},
                           {"label": "Max Bid", "value": "max_bid"},
                           {"label": "Max Ask", "value": "max_ask"},
                           {"label": "Min Bid", "value": "min_bid"},
                           {"label": "Min Ask", "value": "min_ask"},
                           {"label": "Closing Bid", "value": "close_bid"},
                           {"label": "Closing Ask", "value": "close_ask"}],
              "key": "values_to_show"}]

    controls = [{"type": "button",
                 "id": "update_data",
                 "label": "Plot"}]

    tabs = ["Info", "Plot"]

    outputs = [{"type": "plot",
               "id": "this_plot",
               "control_id": "update_data",
                "tab": "Plot",
               "on_page_load": False},

               {"type": "html",
                "id": "opening_page",
                "control_id": "reset_data",
                "tab": "Info",
                "on_page_load": True}]

    def getPlot(self, params):
        if params['data_range'] == 'historical':
            return plots.plot_historical_data(params, self.pg_cursor, self.pg_conn)

        if params['data_range'] == 'daily':
            return plots.plot_daily_data(params, self.bucket)

    def getHTML(self, params):
        with open("info.html") as f:
            info_page = f.read()

        return info_page

app = ForexApp()
app.launch(port=8000)
