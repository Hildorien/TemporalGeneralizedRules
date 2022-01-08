import pandas as pd

from DataStructures.Parser import Parser
from DataStructures.Transaction import Transaction

"""
dfTx = pd.read_csv('Datasets/transacciones.csv')
dfTx['ProductID'] = dfTx['ProductID'].map(str)
dfTxFiltered = dfTx[['OrderID', 'ProductID']].dropna()
dfTxGrouped = dfTxFiltered.groupby(['OrderID'])['ProductID'].apply(','.join).reset_index()

dfSalesFact98 = pd.read_csv('Datasets/salesfact1998.csv', delimiter=';')
dfSalesFact98['product_id'] = dfSalesFact98['product_id'].map(str)
dfSalesFact98Grouped = dfSalesFact98.groupby(
    ['customer_id', 'time_id', 'the_date'])['product_name'].apply(','.join).reset_index()

df = dfSalesFact98 
#df = dfTxGrouped
my_list = []
for index, row in df.iterrows():
    my_list.append(Transaction(index, row[2], row[3]))

print("ID", "Timestamp", "Items", sep=' | ')
for obj in my_list:
   print(obj.id, obj.timestamp, obj.items, sep=' | ')
"""
"""
#Uncomment this section to test parser
#Pre-format csv
df = pd.read_csv('Datasets/transacciones.csv')
dfformated = df[['OrderID', 'ProductID']].dropna()
dfformated.insert(1, 'Timestamp', '0')
#print(dfformated)
dfformated.to_csv('Datasets/transacciones_formated.csv', index=False)
transactions = Parser.parse('Datasets/transacciones_formated.csv')
print("ID", "Timestamp", "Items", sep=' | ')
for obj in transactions:
   print(obj.id, obj.timestamp, obj.items, sep=' | ')
"""