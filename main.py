import pandas as pd

from DataStructures.Transaction import Transaction

dfTx = pd.read_csv('Datasets/transacciones.csv')
dfTx['ProductID'] = dfTx['ProductID'].map(str)
dfTxFiltered = dfTx[['OrderID', 'ProductID']].dropna()
dfTxGrouped = dfTxFiltered.groupby(['OrderID'])['ProductID'].apply(','.join).reset_index()

my_list = []
for index, row in dfTxGrouped.iterrows():
    my_list.append(Transaction(index, '0', row[1]))

print("ID", "Timestamp", "Items", sep=' | ')
for obj in my_list:
    print(obj.id, obj.timestamp, obj.items, sep=' | ')