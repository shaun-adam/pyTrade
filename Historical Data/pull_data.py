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
yf.pdr_override()

# Variables
tickers = si.tickers_sp500()
tickers = [item.replace(".", "-") for item in tickers] # Yahoo Finance uses dashes instead of dots
index_name = '^GSPC' # S&P 500
start_date = datetime.datetime.now() - datetime.timedelta(days=365)
end_date = datetime.date.today()
exportList = pd.DataFrame(columns=['Stock', "RS_Rating", "50 Day MA", "150 Day Ma", "200 Day MA", "52 Week Low", "52 week High"])
table_name = "test" # table and file name

def create_connection(db_file,c=""):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sq.connect(db_file)
        print(sq.version)
        if c != "":
            conn.cursor().execute(c)
            conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


createTbl = "CREATE TABLE IF NOT EXISTS TSX (Date date,Open real,High real,Low real,Close real,AdjClose real,Volume int)"
create_connection("historicalData.db",createTbl)

'''
for ticker in tickers:
    # Download historical dat
    df = pdr.get_data_yahoo(ticker, start_date, end_date)
    df.to_csv(f'{ticker}.csv')



conn = sq.connect('{}.sqlite'.format(table_name)) # creates file
df.to_sql(table_name, conn, if_exists='replace', index=False) # writes to file
conn.close() # good practice: close connection







    # Calculating returns relative to the market (returns multiple)
    df['Percent Change'] = df['Adj Close'].pct_change()
    stock_return = (df['Percent Change'] + 1).cumprod()[-1]
    
    returns_multiple = round((stock_return / index_return), 2)
    returns_multiples.extend([returns_multiple])
    
    print (f'Ticker: {ticker}; Returns Multiple against S&P 500: {returns_multiple}\n')
    time.sleep(1)

# Creating dataframe of only top 30%
rs_df = pd.DataFrame(list(zip(tickers, returns_multiples)), columns=['Ticker', 'Returns_multiple'])
rs_df['RS_Rating'] = rs_df.Returns_multiple.rank(pct=True) * 100
rs_df = rs_df[rs_df.RS_Rating >= rs_df.RS_Rating.quantile(.70)]

'''