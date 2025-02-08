# Bankway: A Python-based ETL Pipeline

## Overview
Bankway is a Python-based ETL (Extract, Transform, Load) pipeline that scrapes financial data from the web, transforms it using exchange rate conversions, and loads it into both CSV and an SQLite database. The system enables efficient data retrieval using batch queries and maintains logs for process tracking.

## Features
- **Web Scraping:** Extracts financial data from an online source (Wikipedia's archived list of largest banks).
- **Data Transformation:** Converts market capitalization values into multiple currencies using exchange rates from a CSV file.
- **Redundant Data Management:** Stores transformed data in both CSV and an SQLite database to ensure data integrity and backup.
- **Batch Query Processing:** Executes predefined SQL queries on the stored database.
- **Logging:** Maintains logs of each step for debugging and monitoring purposes.

## Requirements
Ensure you have the following dependencies installed before running the project:

```bash
pip install requests beautifulsoup4 pandas numpy
```

## Usage

1. **Extract Data**
   - Scrapes financial data from Wikipedia (archived link) and loads it into a Pandas DataFrame.

2. **Transform Data**
   - Reads exchange rates from a CSV file.
   - Converts market capitalization into different currencies.

3. **Load Data**
   - Saves the transformed data to a CSV file.
   - Loads the data into an SQLite database.

4. **Run Batch Queries**
   - Executes SQL queries to retrieve insights from the database.

## Execution Steps
```python
# Run the ETL process
web_url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_conversion = "exchange_rate.csv"
transformed_data_path = "bank_data"
table_name = "Largest_banks"

# Extract data
df = extract(web_url)

# Transform data
df = transform(df, csv_conversion)

# Load data into CSV and Database
load_to_csv(df, transformed_data_path)
load_to_db(df, table_name)

# Define batch queries
queries = {
    "Complete table" : "SELECT * FROM Largest_banks",
    "Average market capitalization USD" : "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "Name of top 5 banks": "SELECT Name from Largest_banks LIMIT 5"
}

# Run batch queries
run_query(queries)
```

## Logging Mechanism
The system logs each step of the ETL process into `code_log.txt`, including data extraction, transformation, loading, and query execution. This ensures transparency and aids in debugging.

## Database Schema
- **Table Name:** `Largest_banks`
- **Columns:**
  - `Name` (Bank name)
  - `MC_USD_Billion` (Market capitalization in USD)
  - `MC_EUR_Billion` (Market capitalization in EUR)
  - `MC_GBP_Billion` (Market capitalization in GBP)
  - `MC_INR_Billion` (Market capitalization in INR)

## Conclusion
Bankway provides an efficient and automated way to gather, process, and analyze financial data using Python, Pandas, SQLite, and batch SQL queries. With its logging mechanism and redundant data management, it ensures data integrity and easy retrieval of insights.
