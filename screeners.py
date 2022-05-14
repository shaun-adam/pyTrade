import pull_data as pu

df = pu.getDF('2021-01-01',period = 'D', ticker = ['SU.TO','ABX.TO'])
dfWeekly = pu.getDF('2021-01-01',period = 'W', ticker = ['SU.TO','ABX.TO'])

print(df.head())
print(df.shape)
print(dfWeekly.head())
print(dfWeekly.shape)