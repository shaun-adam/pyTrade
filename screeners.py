from pull_data import conn_read as cn
import pull_data as pu

cols = ['Ticker', 'Date','Open' ,'High' , 'Low' , 'Close', 'AdjClose', 'Volume']
db = "HistoricalData/historicalData.db"
q =  "Select Ticker, Date,Open ,High , Low , Close, AdjClose, Volume from TSX where Ticker = 'ABX.TO'"
df = cn(db,q,False,False,cols,'Date')
df = df.sort_index()
print(df.head())
print(df.shape)
