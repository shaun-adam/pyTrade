from pull_data import conn_read as cn
import pull_data as pu

pu.refreshTSX("max")

cols = ['Ticker', 'Date','Open' ,'High' , 'Low' , 'Close', 'AdjClose', 'Volume']
db = "HistoricalData/historicalData.db"
q =  "Select * from TSX where Ticker = 'SU.TO'"
df = cn(db,q,False,False,cols,'Date')
df = df.sort_index()
print(df.head())
print(df.shape)


# debug for why so slow now