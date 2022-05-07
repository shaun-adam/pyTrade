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
        df.to_sql(tblName, conn, if_exists=insertMode,index=False)

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

    #SQLITE file
    db = "HistoricalData/historicalData.db"

    #Setup commands on SQLITE DB
    tbl1 = "CREATE TABLE IF NOT EXISTS TSX (Ticker char(10), Date date,Open real,High real,Low real,Close real,AdjClose real,Volume int)"
    tbl2 = "drop table if exists TMP"
    tbl2a = "CREATE TABLE TMP (Ticker char(10), Date date,Open real,High real,Low real,Close real,AdjClose real,Volume int)"
    setup = [tbl1,tbl2,tbl2a]
    for com in setup:
        conn_exec(db,com)

    if secs:
        tickers = secs
    else:
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

    
    mergeData = "DELETE FROM TSX where ROWID IN (SELECT F.ROWID FROM TSX F JOIN TMP T WHERE F.Ticker = T.Ticker and F.Date = T.Date)"
    insData = "INSERT INTO TSX SELECT Ticker, Date,Open ,High , Low , Close, AdjClose, Volume FROM TMP"
    tickers = [item for item in tickers if len(item)<8]

    totTickers = len(tickers)

    i = 0
    for ticks in chunks(tickers,200):
        s = ' '
        s = s.join(ticks)
        ticks = s
        #“1d”, “5d”, “1mo”, “3mo”, “6mo”, “1y”, “2y”, “5y”, “10y”, “ytd”, “max”
        df = yf.download(ticks,period =p ,progress=False,threads=True)
        df = df.rename(columns={"Adj Close": "AdjClose"})
        df['Ticker'] = ticks
        if secs:
             df=df.reset_index().rename(columns={"level_0":"Ticker"})
        else:
            df=df.stack(level = 1).reset_index().rename(columns={"level_1":"Ticker"})
        conn_insert_df('TMP',df,db,'replace')
        conn_exec(db,mergeData)
        conn_exec(db,insData)
        i=i+200
        print(i,"out of ",totTickers," checked")

#resample to weekly to determine long or netural list