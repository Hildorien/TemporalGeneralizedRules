import unittest
from DataStructures.Parser import Parser
from Tests.utilityTests import utilityTests
from Apriori.apriori import apriori2

# Parse dataset in transaction format
"""
parser = Parser()
database = parser.parse("Datasets/sales_formatted.csv")
dict = apriori2(database, 0.003, 0)
print(dict)
"""

# Parse dataset in basket format
parser = Parser()
datasets = ["Datasets/data.csv",
            "Datasets/data2.csv",
            "Datasets/data3.csv",
            "Datasets/data4.csv",
            "Datasets/data5.csv",
            "Datasets/data6.csv",
            "Datasets/data7.csv"]
database = parser.parseBasketFile(datasets[0])
# Prints to get info of algorithm
print('Database size: ' + str(database.row_count))
print('Total items: ' + str((len(database.items_dic.keys()))))
dict = apriori2(database, 0.05, 0)
print('Frequent Itemsets: ')
print(dict)

# RunTests
# suite = unittest.TestLoader().loadTestsFromTestCase(utilityTests)
# unittest.main()
