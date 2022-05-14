from pull_data import conn_read as cn
import pull_data as pu
import pandas as pd
def getSingleDF(ticker,period = 'D'):
    cols = ['Ticker', 'Date','Open' ,'High' , 'Low' , 'Close', 'AdjClose', 'Volume']
    db = "HistoricalData/historicalData.db"
    q =  "Select Ticker, Date,Open ,High , Low , Close, AdjClose, Volume from TSX where Ticker = '"+ticker+"'"
    df = cn(db,q,False,False,cols)
    df =df.set_index(pd.DatetimeIndex(df['Date']))
    if period == 'W':
        df= df.resample('W').agg({'Ticker':'first','Open':'first','High':'max','Low':'min','Close':'last','AdjClose':'last','Volume':'sum'})
    df = df.sort_index()
    return df

def getDF(date,period = 'D"',ticker = None):
    cols = ['Ticker', 'Date','Open' ,'High' , 'Low' , 'Close', 'AdjClose', 'Volume']
    db = "HistoricalData/historicalData.db"
    if ticker:
        if type(ticker) == list and len(ticker) >1:
            ticker = "','".join(ticker)
            ticker = "'"+ticker+"'"
            q =  "Select Ticker, Date,Open ,High , Low , Close, AdjClose, Volume from TSX where Ticker in ("+ticker+") and Date >='"+date+"'"
            
        elif type(ticker) == list and len(ticker) ==1:
            q =  "Select Ticker, Date,Open ,High , Low , Close, AdjClose, Volume from TSX where Ticker ='"+ticker[0]+"' and Date >='"+date+"'"
        elif type(ticker) == str:
            q =  "Select Ticker, Date,Open ,High , Low , Close, AdjClose, Volume from TSX where Ticker ='"+ticker+"' and Date >='"+date+"'"
    else:
        q =  "Select Ticker, Date,Open ,High , Low , Close, AdjClose, Volume from TSX where Date >='"+date+"'"
    df = cn(db,q,False,False,cols)
    df =df.set_index([pd.DatetimeIndex(df['Date'])])
    if period == 'W':
        df= df.groupby('Ticker').resample('W').agg({'Open':'first','High':'max','Low':'min','Close':'last','AdjClose':'last','Volume':'sum'})
    
    if period == 'D':
        df=df.reset_index(level=0,drop = True)
        df = df.set_index(['Ticker', 'Date'])
    df = df.sort_index()
    return df

#df = getSingleDF('SU.TO')


df = getDF('2021-01-01',period = 'D', ticker = ['SU.TO','ABX.TO'])

#dfWeekly =  df.groupby(pd.Grouper(level=-1,freq='W',axis=1)) 
print(df.head())
print(df.shape)
#print(dfWeekly.head())
#print(dfWeekly.shape)