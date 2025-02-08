import csv
from io import StringIO
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy
import datetime

asciiArt = """
 $$$$8                                        $$$$$                                                        
 $$$$B                                        $$$$$                                                        
 $$$$$$$$$$$$$   $$$$$$$$$$$r  $$$$$$$$$$$$   $$$$$   ?$$q  b$$$$[  J$$$$   $$  $$$$$$$$$$$k `$$$$$    $$u 
 $$$$8    $$$$$         Z$$$$  $$$$$   %$$$$  $$$$$ C$$$     $$$$$  $$$$$m $$          _$$$$,  $$$$$  $$>  
 $$$$%    *$$$$ $$$$$$$$$$$$$  $$$$$   C$$$$  $$$$$$$$$$$     $$$$$$$L$$$$$$i  %$$$$$$$$$$$$<   $$$$$$$    
 $$$$$    $$$$d $$$$U   ?$$$$  $$$$$   C$$$$  $$$$$  $$$$$     $$$$$  8$$$$&   $$$$$    $$$$<    $$$$$     
 $$$$$$$$$$$$^   $$$$$$$$$$$$  $$$$$   C$$$$  $$$$$   $$$$$    [$$$-   $$$$     8$$$$$$$$$$$<     $$$      
                                                                                                 [$$       
                                                                                                C$$
"""
print(asciiArt)

def log_progess(message):
    log_message = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} : {message}"
    with open('code_log.txt', 'a') as logs:
        logs.write(f"{log_message}\n")
    print(log_message)

log_progess("Preliminaries complete. Initiating ETL process")
print("============================================================================================================================")

def extract(url):
    log_progess("[EXTRACT] Initiating extraction processs...")
    log_progess("[EXTRACT] Requesting HTML...")
    request = requests.get(url)

    log_progess("[EXTRACT] Parsing HTML...")
    parsed_html = BeautifulSoup(request.text, 'html.parser')

    log_progess("[EXTRACT] HTML parsed, loading Dataframe...")
    df = pd.read_html(StringIO(str(parsed_html)))[0]
    #df = pd.DataFrame(df)
    log_progess("[EXTRACT] Dataframe loaded:")
    print(df)
    log_progess("[EXTRACT] Extraction process completed!")
    print("============================================================================================================================")
    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    log_progess("[TRANSFORM] Initiating transform processs...")
    log_progess("[TRANSFORM] Opening conversion rates CSV...")   
    with open(csv_path, mode='r') as file:
        conversion_rates_table = list(csv.reader(file))[1:]
        log_progess("[TRANSFORM] Loading conversion rates...")
        rates = {row[0]: float(row[1]) for row in conversion_rates_table}
        log_progess("[TRANSFORM] Current rates:")
        for currency, rate in rates.items():
            print(f"{currency}: {rate}")

    log_progess("[TRANSFORM] Creating coversion rate columns...")
    df.rename(columns={"Market cap (US$ billion)": "MC_USD_Billion"}, inplace=True)
    df.rename(columns={"Bank name": "Name"}, inplace=True)
    df["MC_EUR_Billion"] = (df["MC_USD_Billion"] * rates['EUR']).round(2)
    df["MC_GBP_Billion"] = (df["MC_USD_Billion"] * rates['GBP']).round(2)
    df["MC_INR_Billion"] = (df["MC_USD_Billion"] * rates['INR']).round(2)
    print(df)
    log_progess("[TRANFORM] Transform process completed!")
    print("============================================================================================================================")
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    log_progess("[EXPORT] Exporting data to CSV file...")
    df.to_csv(output_path + '.csv')
    log_progess("[EXPORT] Transform process completed!")
    print("============================================================================================================================")

def load_to_db(df, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    log_progess("[SQLite] Establishing database connection...")
    connection = sqlite3.connect("bankdata.db")
    log_progess("[SQLite] Connected to database!")
    log_progess("[SQLite] Generating table...")
    df.to_sql(table_name, connection, if_exists='append', index=False)
    log_progess("[SQLite] Table generated successfully!")
    log_progess("[SQLite] Closing connection...")
    connection.commit()
    connection.close()
    log_progess("[SQLite] Connection closed")
    print("============================================================================================================================")

def run_query(query_batch):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    log_progess("[SQLite] Establishing database connection...")
    connection = sqlite3.connect("bankdata.db")
    for query_description, query in query_batch.items():
        log_progess(f"[SQLite] Processing query: {query_description} | Using: {query}")
        print("----------------------------------------------------------------------------------------------------------------------------")
        print(pd.read_sql_query(query,connection))
        print("----------------------------------------------------------------------------------------------------------------------------")
    log_progess("[SQLite] Closing connection...")
    connection.commit()
    connection.close() 
    log_progess("[SQLite] Connection closed")
    print("============================================================================================================================")


web_url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
data_attributes = ["Name", "MC_USD_Billion", "MC_GCP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
csv_coversion = "exchange_rate.csv"
transformed_data_path = "bank_data"
table_name = "Largest_banks"

df = extract(web_url)

df = transform(df, csv_coversion)

load_to_csv(df, transformed_data_path)

load_to_db(df, table_name)

queries = {
    "Complete table" : "SELECT * FROM Largest_banks",
    "Average market capitalization USD" : "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "Name of top 5 banks": "SELECT Name from Largest_banks LIMIT 5"
}

run_query(queries)

