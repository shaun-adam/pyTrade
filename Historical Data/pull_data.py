from pandas_datareader import data as pdr
from yahoo_fin import stock_info as si
from pandas import ExcelWriter
import yfinance as yf
import pandas as pd
import datetime
import time
import pandas as pd
import sqlite3 as sq
from sqlite3 import Error

import requests
import json as js
yf.pdr_override()

# Variables
tickers = []
ti = requests.get(f"https://www.tsx.com/json/company-directory/search/tsx/%5E*")
ti = ti.json()
ti = ti['results']
for key in ti:
    tickers.append(key['symbol'])

#tickers = si.tickers_sp500()
#tickers = [item.replace(".", "-") for item in tickers] # Yahoo Finance uses dashes instead of dots
index_name = '^GSPC' # S&P 500
start_date = datetime.datetime.now() - datetime.timedelta(days=365)
end_date = datetime.date.today()
exportList = pd.DataFrame(columns=['Stock', "RS_Rating", "50 Day MA", "150 Day Ma", "200 Day MA", "52 Week Low", "52 week High"])
db = "historicalData.db"

def conn_exec(db_file,c=""):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sq.connect(db_file)
        print('Execute: ',c)
        if c != "":
            conn.cursor().execute(c)
            conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


tbl1 = "CREATE TABLE IF NOT EXISTS TSX (Ticker char(10), Date date,Open real,High real,Low real,Close real,AdjClose real,Volume int)"
tbl2 = "drop table if exists TMP"
tbl2a = "CREATE TABLE TMP (Ticker char(10), Date date,Open real,High real,Low real,Close real,AdjClose real,Volume int)"
setup = [tbl1,tbl2,tbl2a]
for com in setup:
    conn_exec(db,com)

def conn_insert_df(tblName,df,db_file,insertMode = 'replace'):
    conn = None
    try:
        conn = sq.connect(db_file)
        df.to_sql(tblName, conn, if_exists=insertMode, index=False)

    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


trials = ['TSLA','TD']


for trial in tickers:
    df = pdr.get_data_yahoo(trial, start_date, end_date)
    df = df.rename(columns={"Adj Close": "AdjClose"})
    df['Date']=df.index
    df['Ticker'] = trial
    conn_insert_df('TMP',df,db,'append')

mergeData = "DELETE FROM TSX where ROWID IN (SELECT F.ROWID FROM TSX F JOIN TMP T WHERE F.Ticker = T.Ticker and F.Date = T.Date)"
insData = "INSERT INTO TSX SELECT * FROM TMP"

conn_exec(db,mergeData)
conn_exec(db,insData)

#increase logging (number of tickers), status (how many are we at out of total?)

#start thinking of how to split into modules??