from pandas_datareader import data as pdr
#from yahoo_fin import stock_info as si
from pandas import ExcelWriter
import yfinance as yf
import pandas as pd
import datetime
import time
import sqlite3 as sq
from sqlite3 import Error

import requests
import json as js
yf.pdr_override()


#Function to execute commands on SQLITE db that don't return a value
def conn_exec(db_file,c="",verbose=False):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sq.connect(db_file)
        if verbose:
            print('Execute: ',c)
        if c != "":
            conn.cursor().execute(c)
            conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
#Function to inster a data frame into a SQLITE table
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

#Function to return all rows of a query against a SQLITE db. By default grabs single column
def conn_read(db_file,c="",verbose=False), single=True:
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sq.connect(db_file)
        if verbose:
            print('Execute: ',c)
        if c != "":
            recs = conn.cursor().execute(c).fetchall()
            records = []
            for r in recs:
                if single:
                    records.append(r[0])
                else:
                    records.append(r)
            return records
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

#timeframe to pull
start_date = datetime.datetime.now() - datetime.timedelta(days=365)
end_date = datetime.date.today()

#SQLITE file
db = "historicalData.db"

#Setup commands on SQLITE DB
tbl1 = "CREATE TABLE IF NOT EXISTS TSX (Ticker char(10), Date date,Open real,High real,Low real,Close real,AdjClose real,Volume int)"
tbl2 = "drop table if exists TMP"
tbl2a = "CREATE TABLE TMP (Ticker char(10), Date date,Open real,High real,Low real,Close real,AdjClose real,Volume int)"
setup = [tbl1,tbl2,tbl2a]
for com in setup:
    conn_exec(db,com)

#holds a list of securities to check
tickers = []

#Attempt to retrieve a list of securities from TSX website. If it times out just use a distinct list of securities already in DB
try:
    ti = requests.get(f"https://www.tsx.com/json/company-directory/search/tsx/%5E*",timeout=5)
    ti = ti.json()
    ti = ti['results']
    for key in ti:
        tickers.append(key['symbol'])
    tickers = [t + '.TO' for t in tickers]
except Exception as err:# requests.exceptions.Timeout as err: 
    #print(err)
    tickerSQL = "SELECT DISTINCT Ticker FROM TSX"
    tickers = conn_read(db,tickerSQL)


totTickers = len(tickers)

mergeData = "DELETE FROM TSX where ROWID IN (SELECT F.ROWID FROM TSX F JOIN TMP T WHERE F.Ticker = T.Ticker and F.Date = T.Date)"
insData = "INSERT INTO TSX SELECT Ticker, Date,Open ,High , Low , Close, AdjClose, Volume FROM TMP"

#loop through tickers and grab historicals from Yahoo Finance
#overwrite existing data for same security on same day
i = 0
for tick in tickers:
    
    df = pdr.get_data_yahoo(tick, start_date, end_date,progress=False)
    df = df.rename(columns={"Adj Close": "AdjClose"})
    df['Date']=df.index
    df['Ticker'] = tick
    conn_insert_df('TMP',df,db,'replace')
    conn_exec(db,mergeData)
    conn_exec(db,insData)
    i=i+1
    print(i,"out of ",totTickers," checked")

#create a control table per ticker for when it was last updated and setup dynamic ranges
#start thinking of how to split into modules??