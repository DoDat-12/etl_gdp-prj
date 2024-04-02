# python3.11 -m pip install pandas
# python3.11 -m pip install numpy
# python3.11 -m pip install bs4


import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = 'Countries_by_GDP.csv'


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    tbody_attribs = soup.find_all('tbody')
    tr_attribs = tbody_attribs[2].find_all('tr')
    for tr in tr_attribs:
        td_attribs = tr.find_all('td')
        if len(td_attribs) != 0:
            if td_attribs[0].find('a') is not None and '—' not in td_attribs[2]:
                    df = pd.concat([df, pd.DataFrame([{table_attribs[0]: td_attribs[0].a.contents[0],
                                                       table_attribs[1]: td_attribs[2].contents[0]}])],
                                    ignore_index=True)
    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    gdp_list = df['GDP_USD_millions']
    for cell in gdp_list:
        a = cell.split(',')
        b = ''
        for i in a:
            b = b + i
        cell = float(b)
        cell = np.round(cell/1000, 2)
    df['GDP_USD_millions'] = gdp_list
    df = df.rename(columns = {'GDP_USD_millions': 'GDP_USD_billions'})
    return df


def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    df = pd.read_sql(query_statement, sql_connection)
    print(df)


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('etl_project_log.txt', 'a') as f:
        f.write(timestamp + ': ' + message + '\n')


''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# extract
df = extract(url, table_attribs)
log_progress('extract successfully')
# transform
df = transform(df)
log_progress('transform successfully')
# load
load_to_csv(df, csv_path)
log_progress('csv loaded')
sql_connection = sqlite3.connect('database.db')
load_to_db(df, sql_connection, table_name)
log_progress('sql loaded')
# query
run_query(f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100", sql_connection)

