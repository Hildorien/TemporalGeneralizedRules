import unittest

import numpy as np
import pandas as pd
import timeit
import time
import datetime
from datetime import datetime
import functools
from DataStructures.Parser import Parser
from Tests.utilityTests import utilityTests
import operator
from Apriori.apriori import apriori
from Apriori.apriori import apriori2
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
"""
df = pd.read_csv('Datasets/sales.csv',
                 dtype={'time_id': int, 'customer_id': int, 'product_id': int,
                        'the_date': "string", 'product_name': "string"})
df.insert(0, 'order_id', -1)
my_dict = {}
orderId = 1
for index, row in df.iterrows():
    uniqueSale = str(row['time_id']) + "." + str(row['customer_id'])
    if uniqueSale not in my_dict:
        my_dict[uniqueSale] = orderId
        orderId += 1
    df.at[index, 'order_id'] = my_dict[uniqueSale]

    "Discard time since every date string has 00:00:00"
    df.at[index, 'the_date'] = df.at[index, 'the_date'].split()[0]

    #Cast string date input to timestamp
    df.at[index, 'the_date'] = str(int(time.mktime(datetime.strptime(df.at[index, 'the_date'], "%Y-%m-%d").timetuple())))
df.rename(columns={"the_date": "timestamp"}, inplace=True)
dfFormated = df[['order_id', 'timestamp', 'product_name']]
dfFormated.to_csv('Datasets/sales_formatted.csv', index=False)
"""
parser = Parser()
database = parser.parseAndSparse("Datasets/sales_formatted.csv")
#print(finalDf.info(memory_usage="deep"))
#print(database.sparse_dataframe.iloc[:, 0])

#RunTests
#suite = unittest.TestLoader().loadTestsFromTestCase(utilityTests)
#unittest.main()
apriori2(database, 0.003, 0)


"""
#dfG -> convertirlo a matriz esparsa usando csr_matrix(data, indices, indptr) -> devuelve un array/matriz
#convertir matriz esparsa a un dataframe:
# dataFrameSparse = pd.DataFrame(array/matriz, columns=[Los productos que aprendi])
# agregar columna extra de timestamp (deberia salir de una porque esta en orden):
# dataFrameSparse['timestamp'] = dfG['timestamp']

"""

