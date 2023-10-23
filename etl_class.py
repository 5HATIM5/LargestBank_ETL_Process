from datetime import datetime
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

# Code for ETL operations on Country-GDP data

class ETL:
    '''Class representing an ETL Processing'''

    LOG_FILE = "Logs/code_log.txt"
    EXCHANGE_RATE = "Assets/exchange_rate.csv"
    TARGET_STORE_FILE = "ProcessedData/Largest_banks_data.csv"
    DB_NAME = "Database/Banks.db"

    def log_progress(self, message):
        ''' This function logs the mentioned message of a given stage of the
        code execution to a log file. Function returns nothing'''

        timestamp_format = "%Y-%h-%d-%H:%M:%S"
        now = datetime.now()
        timestamp = now.strftime(timestamp_format)
        with open(ETL.LOG_FILE, "a", encoding="utf-8") as f:
            f.write(timestamp + "," + message + "\n")

    def extract(self, url, table_attribs):
        ''' This function aims to extract the required
        information from the website and save it to a data frame. The
        function returns the data frame for further processing. '''

        df = pd.DataFrame()
        html_page = requests.get(url,  timeout=10).text
        data = BeautifulSoup(html_page, "html.parser")
        tables = data.find_all("tbody")
        rows = tables[0].find_all("tr")

        for row in rows:
            col = row.find_all("td")
            if len(col) != 0:
                a = col[1].find_all("a", title=True)
                name = a[1].contents[0]
                market_cap = col[2].contents[0].replace("\n", "")
                data_dict = {
                    table_attribs[0]: name,
                    table_attribs[1]: float(market_cap),
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df1, df], ignore_index=True)

        return df

    def transform(self, df, csv_path):
        ''' This function accesses the CSV file for exchange rate
        information, and adds three columns to the data frame, each
        containing the transformed version of Market Cap column to
        respective currencies'''

        exchange_rate = {}
        with open(csv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            for line in lines[1:]:
                columns = line.strip().split(",")
                currency = columns[0]
                rate = float(columns[1])
                exchange_rate[currency] = rate

        df["MC_EUR_Billion"] = [
            np.round(x * exchange_rate["EUR"], 2) for x in df["MC_USD_Billion"]
        ]
        df["MC_GBP_Billion"] = [
            np.round(x * exchange_rate["GBP"], 2) for x in df["MC_USD_Billion"]
        ]
        df["MC_INR_Billion"] = [
            np.round(x * exchange_rate["INR"], 2) for x in df["MC_USD_Billion"]
        ]

        return df

    def load_to_csv(self, df, output_path):
        ''' This function saves the final data frame as a CSV file in
        the provided path. Function returns nothing.'''

        df.to_csv(output_path)

    def load_to_db(self, df, sql_connection, table_name):
        ''' This function saves the final data frame to a database
        table with the provided name. Function returns nothing.'''

        df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

    def run_query(self, query_statement, sql_connection):
        ''' This function runs the query on the database table and
        prints the output on the terminal. Function returns nothing. '''

        query_output = pd.read_sql(query_statement, sql_connection)
        print(query_output)


