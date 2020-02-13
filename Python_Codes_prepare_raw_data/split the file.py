import pandas as pd
numrows = 400000000 #number of rows threshold to be roughly half
count = 0 #keep track of chunks
chunkrows = 100000 #read 100k rows at a time
df = pd.read_csv('/Users/wanlindu/Downloads/dataset/SFMTA_Parking_Meter_Detailed_Revenue_Transactions.csv', iterator=True, chunksize=chunkrows) 
for chunk in df: #for each 100k rows
    if count <= numrows/chunkrows: #if 5GB threshold has not been reached 
        outname = "/Users/wanlindu/Downloads/dataset/SFMTA_Parking_Meter_Detailed_Revenue_Transactions_1stHalf.csv"
    else:
        outname = "/Users/wanlindu/Downloads/dataset/SFMTA_Parking_Meter_Detailed_Revenue_Transactions_2ndHalf.csv"
    #append each output to same csv, using no header
    chunk.to_csv(outname, mode='a', header=None, index=None)
    count+=1