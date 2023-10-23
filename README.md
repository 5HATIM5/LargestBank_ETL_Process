# LargestBank_ETL_Process

The "Automated ETL System for Top 10 Largest Banks by Market Capitalization" is a data engineering project that automates the process of extracting, transforming, and loading data to compile a list of the top 10 largest banks in the world, ranked by market capitalization in billion USD. This system is designed to ensure the efficient and consistent preparation of this financial report every quarter.


# Project Goals:

Data Extraction: The project begins by extracting data from a specific webpage, which lists the largest banks. It scrapes the relevant information such as bank names and their market capitalization in billion USD. The data is collected using web scraping techniques.

Data Transformation: After extracting the raw data, the system processes it by converting the market capitalization of each bank into three different currencies: GBP, EUR, and INR. The exchange rates required for this transformation are obtained from an external CSV file.

Data Storage: The transformed data is saved in two different forms for further analysis:

As a local CSV file for easy access and sharing.
As a database table in a SQLite database for structured and organized data storage.

# Project Components:

ETL Class: The core of the project is an ETL (Extraction, Transformation, Loading) class, which encapsulates the functions and methods required for each step of the data processing pipeline. It includes functions for logging progress, data extraction from the web, data transformation using exchange rates, saving data to CSV, and loading data into a database table.

Web Scraping: The system uses web scraping techniques to collect data from a specific webpage, extracting bank names and market capitalization.

Data Transformation: The project includes the conversion of market capitalization values from USD to GBP, EUR, and INR using exchange rates provided in an external CSV file.

Data Storage: The transformed data is saved in two formats. A CSV file is generated for easy access and sharing, while a SQLite database is used for structured and organized data storage.

Logging: The ETL class also provides a logging mechanism to record the progress of the ETL process, creating a record of each step completed.

Automation: The system is designed for quarterly execution, ensuring that the same ETL process can be repeated at regular intervals, reducing manual effort and potential errors.

This automated ETL system streamlines the process of compiling a list of the top 10 largest banks by market capitalization in different currencies. It ensures consistency and reliability in data preparation for quarterly financial reporting, making it a valuable tool for the research organization's data engineering needs.
