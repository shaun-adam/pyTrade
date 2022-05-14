import pull_data as pu
import pandas_ta as ta

df = pu.getDF('2021-01-01',period = 'D', ticker = ['SU.TO','ABX.TO'])
dfWeekly = pu.getDF('2021-01-01',period = 'W', ticker = ['SU.TO','ABX.TO'])

for Date, df2 in dfWeekly.groupby(level=0):
    df2 = df2.ta.macd(close='Adj_Close')
    print(df2)

#need to loop through weekly dataframes, calculate macdhistograms and check for trend
#reread trading for a living section on the impulse strategy