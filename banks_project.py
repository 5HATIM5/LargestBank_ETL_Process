import sqlite3
from etl_class import ETL 

ETL_PROCESS = ETL()

ETL_PROCESS.log_progress("ETL Job Started")
ETL_PROCESS.log_progress("Preliminaries complete. Initiating ETL process")

# Log the beginning of the Extraction process
URL = "https://web.archive.org/web/20230908091635/" \
      "https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs_extraction  = ["Name", "MC_USD_Billion"]
extracted_data = ETL_PROCESS.extract(URL, table_attribs_extraction )
ETL_PROCESS.log_progress("Data extraction complete. Initiating Transformation process")


# Log the beginning of the Transformation process
transformed_data = ETL_PROCESS.transform(extracted_data, ETL.EXCHANGE_RATE)
ETL_PROCESS.log_progress("Data transformation complete. Initiating Loading process")

# Log the beginning of the Loading process
ETL_PROCESS.load_to_csv(transformed_data, ETL.TARGET_STORE_FILE)
ETL_PROCESS.log_progress("Data saved to CSV file")

# Log the SQL connection innitialization process
conn = sqlite3.connect(ETL.DB_NAME)
ETL_PROCESS.log_progress("SQL Connection initiated")

# Log the data loadedto database
TABLE_NAME = "Largest_banks"
ETL_PROCESS.load_to_db(transformed_data, conn, TABLE_NAME)
ETL_PROCESS.log_progress("Data loaded to Database as a table, Executing queries")

# Log query data from database
query_statement1 = f"SELECT * FROM {TABLE_NAME}"
ETL_PROCESS.run_query(query_statement1, conn)
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}"
ETL_PROCESS.run_query(query_statement2, conn)
query_statement3 = f"SELECT Name from {TABLE_NAME} LIMIT 5"
ETL_PROCESS.run_query(query_statement3, conn)
ETL_PROCESS.log_progress("Process Complete")

conn.close()
ETL_PROCESS.log_progress("Server Connection closed")
