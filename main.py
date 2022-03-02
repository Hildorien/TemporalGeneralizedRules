import math
import time
import unittest
from multiprocessing import freeze_support
import pandas as pd
from DataStructures.Parser import Parser
from Tests.utility_tests import UtilityTests
from Apriori.apriori import apriori
from Apriori.apriori import apriori_parallel_count

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
#suite = unittest.TestLoader().loadTestsFromTestCase(UtilityTests)
#unittest.main()


#def main():
#    parser = Parser()
#    datasets = ["Datasets/data.csv",
#                 "Datasets/data2.csv",
#                 "Datasets/data3.csv",
#                 "Datasets/data4.csv",
#                 "Datasets/data5.csv",
#                 "Datasets/data6.csv",
#                 "Datasets/data7.csv"]
#     print('Parsing dataset...')
#     start = time.time()
#     #database = parser.parse(datasets[6], 'basket')
#
#     #Vanilla
#     database = parser.parse("Datasets/sales_formatted.csv", 'single')
#
#     #HTG
#     #database = parser.parse("Datasets/sales_formatted.csv", 'single', None, True)
#
#     #Taxonomy
#     #database = parser.parse("Datasets/sales_formatted.csv", 'single', 'Taxonomies/salesfact_taxonomy.csv')
#     end = time.time()
#     print('Took ' + (str(end - start) + ' seconds'))
#     # Prints to get info of algorithm
#     print('Database size: ' + str(database.row_count))
#     print('Total items: ' + str((len(database.items_dic.keys()))))
#     rules = apriori(database, 0.0001, 0.6)
#     print(len(rules))
#
#
#   if __name__=="__main__":
#       freeze_support()
#       main()


