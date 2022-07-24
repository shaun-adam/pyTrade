import pull_data as pu
import pandas_ta as ta

#df = pu.getDF('2021-01-01',period = 'D', ticker = ['SU.TO','ABX.TO'])
#dfWeekly = pu.getDF('2021-01-01',period = 'W', ticker = ['SU.TO','ABX.TO'])
dfWeekly = pu.getDF('2021-01-01',period = 'W',ticker='TCK.B.TO')

weeklyResults = []
for tick, df2 in dfWeekly.groupby(level=0):
    if len(df2) >52:
        df2.ta.macd(close='Adj_Close',append=True)
        df2['histDiff'] = df2['MACDh_12_26_9'].diff()
        df2.ta.ema(close='Adj_Close',length=13,append=True)
        df2['EMAdiff'] = df2['EMA_13'].diff()
        last = df2.iloc[-1,:]
        macd = last['histDiff']
        ema = last['EMAdiff']
        if macd > 0 and ema >0:
            weeklyResults.append([tick,2])
        elif macd >0 and ema <=0:
            weeklyResults.append([tick,1])
        elif macd <=0 and ema > 0 :
            weeklyResults.append([tick,1])
   
print(weeklyResults)

#create same checks for daily charts that just flipped from 0 or 1 to a 2