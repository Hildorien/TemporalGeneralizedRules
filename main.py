import math
import unittest
from multiprocessing import freeze_support
import pandas as pd
from DataStructures.Parser import Parser
from Tests.utilityTests import utilityTests
from Apriori.apriori import apriori
from Apriori.apriori import apriori_mapreduce
from joblib import Parallel, delayed

# Parse dataset in transaction format
"""
parser = Parser()
database = parser.parse("Datasets/sales_formatted.csv")
dict = apriori2(database, 0.003, 0)
print(dict)
"""

# Parse dataset in basket format
"""parser = Parser()
datasets = ["Datasets/data.csv",
            "Datasets/data2.csv",
            "Datasets/data3.csv",
            "Datasets/data4.csv",
            "Datasets/data5.csv",
            "Datasets/data6.csv",
            "Datasets/data7.csv"]
database = parser.parseBasketFile(datasets[6])
# Prints to get info of algorithm
print('Database size: ' + str(database.row_count))
print('Total items: ' + str((len(database.items_dic.keys()))))
rules = apriori(database, 0.1, 0.5)
print(len(rules))
"""
#for rule in rules:
#    print(database.printAssociationRule(rule))
#print('Frequent Itemsets: ')
#print(dict)
# RunTests
#suite = unittest.TestLoader().loadTestsFromTestCase(utilityTests)
#unittest.main()


def main():
    parser = Parser()
    datasets = ["Datasets/data.csv",
                "Datasets/data2.csv",
                "Datasets/data3.csv",
                "Datasets/data4.csv",
                "Datasets/data5.csv",
                "Datasets/data6.csv",
                "Datasets/data7.csv"]
    #database = parser.parseBasketFile(datasets[6])
    database = parser.parse("Datasets/sales_formatted.csv")
    # Prints to get info of algorithm
    print('Database size: ' + str(database.row_count))
    print('Total items: ' + str((len(database.items_dic.keys()))))
    rules = apriori_mapreduce(database, 0.002, 0.4)
    print(len(rules))

if __name__=="__main__":
    freeze_support()
    main()


