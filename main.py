import math
import sys
import time
import unittest
from datetime import datetime
from multiprocessing import freeze_support

import numpy as np

from Cumulate.cumulate import vertical_cumulate, cumulate_frequents, vertical_cumulate_frequents
from DataStructures.Parser import Parser
from Tests.utility_tests import UtilityTests
from Apriori.apriori import apriori

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
# for rule in rules:
#    print(database.printAssociationRule(rule))
# print('Frequent Itemsets: ')
# print(dict)

# RunTests
# suite = unittest.TestLoader().loadTestsFromTestCase(UtilityTests)
# unittest.main()


# def main():
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

"""
df = pd.read_csv('Taxonomies/product_category.csv', delimiter=';',
                 usecols=['product_name',
                          'product_subcategory',
                          'product_category',
                          'product_department',
                          'product_family'])

dfProductP1 = df[['product_name', 'product_subcategory']]
dfProductP2 = df[['product_subcategory', 'product_category']].drop_duplicates()
dfProductP3 = df[['product_category', 'product_department']].drop_duplicates()
dfProductP4 = df[['product_department', 'product_family']].drop_duplicates()

dfProductP1 = dfProductP1.rename(columns={'product_name': 'item', 'product_subcategory': 'parent'})
dfProductP2 = dfProductP2.rename(columns={'product_subcategory': 'item', 'product_category': 'parent'})
dfProductP3 = dfProductP3.rename(columns={'product_category': 'item', 'product_department': 'parent'})
dfProductP4 = dfProductP4.rename(columns={'product_department': 'item', 'product_family': 'parent'})

frames = [dfProductP1, dfProductP2, dfProductP3, dfProductP4]
dfSingle = pd.concat(frames)
print(dfSingle)


#dfSingle.to_csv('Taxonomies/salesfact_taxonomy_single_2.csv', header=False, sep=',', index=False)
"""
#database = Parser().parse('Datasets/synthetic_dataset_test.csv', 'single')
#apriori(database, 0.5, 0.5)

def func(orders, total_orders, target_orders):
    return round((orders / total_orders) * target_orders)

if __name__=="__main__":

    vertical_db = Parser().parse('F:\TesisSyntheticDatasets\Transaction\T100k.data', 'single',
                                 'F:\TesisSyntheticDatasets\Transaction\T100k.tax')
    # start = time.time()
    # frequents, support_dict, taxonomy = vertical_cumulate_frequents(vertical_db, 0.005)
    # end = time.time()
    # print('vertical_cumulate_frequents took ' + str(end-start))
    # print('##########################################################')
    # print('##########################################################')
    start = time.time()
    r = vertical_cumulate(vertical_db, 0.01, 0.25, 0, False, True)
    end = time.time()
    print('rule gen vertical_cumulate_frequents took ' + str(end-start))
    print('Generated ' + str(len(r)) + ' rules')

    # for k in frequents:
    #     print('vertical_cumulate_frequents generated ' + str(len(frequents[k])) + ' frequent itemsets in k=' + str(k))

    # start = time.time()
    # frequents, support_dict, taxonomy = vertical_cumulate_frequents(vertical_db, 0.005, True)
    # end = time.time()
    # print('vertical_cumulate_frequents took ' + str(end - start))
    # for k in frequents:
    #     print('vertical_cumulate_frequents generated ' + str(len(frequents[k])) + ' frequent itemsets in k=' + str(k))
    #
    #
    # horizontal_db = Parser().parse_horizontal_database('F:\TesisSyntheticDatasets\Root\R500.data',
    #                                                    'F:\TesisSyntheticDatasets\Root\R500.tax', 'single')
    # start = time.time()
    # r2 = cumulate_frequents(horizontal_db, 0.005)
    # end = time.time()
    # print('cumulate_frequents took ' + str(end - start) + ' generated ' + str(len(r2)))

    # database = Parser().parse('F:\TesisSyntheticDatasets\Root\R250.data', 'single')
    # start = time.time()
    # r = apriori(database, 0.001, 0.1)
    # end = time.time()
    # print('apriori took ' + str(end - start) + ' generated ' + str(len(r)))











