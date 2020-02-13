import pandas as pd

df = pd.read_csv('/Users/wanlindu/Downloads/dataset/SFMTA_Parking_Meter_Detailed_Revenue_Transactions.csv', nrows=500) 

df.to_csv('/Users/wanlindu/Downloads/dataset/test100.csv')