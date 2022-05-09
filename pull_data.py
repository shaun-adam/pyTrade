from pandas_datareader import data as pdr
#from yahoo_fin import stock_info as si
from pandas import ExcelWriter
import yfinance as yf
import pandas as pd
import datetime
import time
import sqlite3 as sq
import logging
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
        if isinstance(c, list):
            for com in c:
                conn.cursor().execute(com)
            conn.commit()
        else:
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
        if insertMode == 'replace':
            dd = f"Delete From "+ tblName 
            conn.cursor().execute(dd)
            conn.commit()

        df.to_sql(tblName, conn,if_exists='append', index=False)

    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def conn_update_meta(val):
    db = "HistoricalData/historicalData.db"
    conn = None
    try:
        conn = sq.connect(db)
        for v in val:
            c = f"REPLACE INTO Meta (Value,Refreshed) VALUES('"+v+"',datetime(CURRENT_TIMESTAMP, 'localtime'))"
            conn.cursor().execute(c)
        conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

#Function to return all rows of a query against a SQLITE db. By default grabs single column in list.  Otherwise returns dataframe
def conn_read(db_file,c="",verbose=False, single=True,cols = None,ind = None):
    conn = None
    try:
        conn = sq.connect(db_file)
        if verbose:
            print('Execute: ',c)
        if c != "":
            recs = conn.cursor().execute(c).fetchall()
            
            if single:
                records = []
                for r in recs:
                    records.append(r[0])
            else:
                
                records = pd.DataFrame(recs,columns=cols)
                records = records.set_index(ind,True)
            return records
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def refreshTSX(p="1mo",secs = None):
    #timeframe to pull
    #start_date = datetime.datetime.now() - datetime.timedelta(days=daysBack)
    #end_date = datetime.date.today()
    print("Beginning Data Refresh")
    #SQLITE file
    db = "HistoricalData/historicalData.db"

    #Setup commands on SQLITE DB
    tbl1 = "CREATE TABLE IF NOT EXISTS TSX (Ticker char(10), Date date,Open real,High real,Low real,Close real,AdjClose real,Volume int)"
    tbl1a = "CREATE UNIQUE INDEX index_name ON TSX(Date, Ticker)"
    tbl2 = "drop table if exists TMP"
    tbl2a = "CREATE TABLE TMP (Ticker char(10), Date date,Open real,High real,Low real,Close real,AdjClose real,Volume int)"
    tbl3 = "CREATE TABLE IF NOT EXISTS Meta (Value char(20),Refreshed Date)"
    tbl3a = "CREATE UNIQUE INDEX idx_value ON Meta (Value)"
    setup = [tbl1,tbl1a,tbl2,tbl2a,tbl3,tbl3a]
    conn_exec(db,setup)

    if secs:
        tickers = secs
    else:
        #holds a list of securities to check
        tickers = []

        #Attempt to retrieve a list of securities from TSX website. If it times out just use a distinct list of securities already in DB
        try:
           # logging.basicConfig()
            #logging.getLogger().setLevel(logging.DEBUG)
            #requests_log = logging.getLogger("requests.packages.urllib3")
            #requests_log.setLevel(logging.DEBUG)
            #requests_log.propagate = True
            print("Refreshing Tickers")
            ti = requests.get(f"https://www.tsx.com/json/company-directory/search/tsx/%5E*",timeout=5)
            ti = ti.json()
            ti = ti['results']
            for key in ti:
                tickers.append(key['symbol'])
            tickers = [t + '.TO' for t in tickers]
            print("Done refreshing Tickers")

        except Exception as err:# requests.exceptions.Timeout as err: 
            print("Ticker Refresh Failed")
            tickerSQL = "SELECT DISTINCT Ticker FROM TSX"
            tickers = conn_read(db,tickerSQL)
            print("Done Tickers Fall Back")

    
    mergeData = "DELETE FROM TSX where ROWID IN (SELECT F.ROWID FROM TSX F JOIN TMP T WHERE F.Ticker = T.Ticker and F.Date = T.Date)"
    insData = "INSERT INTO TSX SELECT Ticker, Date,Open ,High , Low , Close, AdjClose, Volume FROM TMP"
    tickers = [item for item in tickers if len(item)<8]
    tickers.sort()
    test1 = 'SU.TO' in tickers
    dontRefresh = conn_read(db,"SELECT Value FROM Meta where julianday()-julianday(Refreshed) <=1")
    test2 = 'SU.TO' in dontRefresh
    tickers = list(set(tickers) - set(dontRefresh))
    totTickers = len(tickers)
    if totTickers == 0:
        print("No tickers to refresh")


    i = 0
    for ticks in chunks(tickers,80):
        s = ' '
        ss = s.join(ticks)
        ticksJoined = ss
        #“1d”, “5d”, “1mo”, “3mo”, “6mo”, “1y”, “2y”, “5y”, “10y”, “ytd”, “max”
        df = pdr.get_data_yahoo(ticksJoined,period =p ,progress=True,threads=True)
        df = df.rename(columns={"Adj Close": "AdjClose"})

        if secs or totTickers == 1:
             df=df.reset_index().rename(columns={"level_0":"Ticker"})
        else:
            df=df.stack(level = 1).reset_index().rename(columns={"level_1":"Ticker"})
        conn_insert_df('TMP',df,db,'replace')
        conn_exec(db,mergeData)
        conn_exec(db,insData)
        conn_update_meta(ticks)

        i=i+len(ticks)
        print(i,"out of ",totTickers," checked")
    print("Data Refresh Complete")
#resample to weekly to determine long or netural list