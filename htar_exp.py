

# def test_HONG_apriori(self):
    #     database = Parser().parseHONG("Datasets/sales_formatted_1997_sorted_by_timestamp.csv", 'single', True)
    #     print('---')
    #     start = time.time()
    #     apriori_rules = apriori(database, 0.006, 0.2)
    #     print(str(len(apriori_rules)) + ' rules')
    #
    #     end = time.time()
    #     print('Apriori Took ' + (str(end - start) + ' seconds'))
    #     start = time.time()
    #     rules_by_pg = HTAR_BY_PG(database, 0.006, 0.2, 0.006, [10, 5, 1])
    #     end = time.time()
    #     print('HONG Took ' + (str(end - start) + ' seconds'))
    #     print(str(len(rules_by_pg)) + ' rules')
    #
    #     self.printRulesDebugging(database, rules_by_pg, {})
import time

from Apriori.apriori import apriori
from DataStructures.Parser import Parser
from HTAR.HTAR import HTAR_BY_PG


def run_HTAR_foodmart_data_1997_correctness_and_completness():
    database = Parser().parse('Datasets/sales_formatted_1997_sorted_by_timestamp.csv', 'single', None, True)
    print('---')
    print("EXP. APRIORI/HTAR: FOODMART, 0.0002/0.6, SIN PARALELIZACION")
    start = time.time()
    apriori_rules = apriori(database, 0.0002, 0.6)
    end = time.time()
    print('Apriori Took ' + (str(end - start) + ' seconds'))
    print('Apriori produced ' + str(len(apriori_rules)) + ' rules')

    start = time.time()
    rules_by_pg = HTAR_BY_PG(database, 0.0002, 0.6, 0.0002, True)
    end = time.time()
    print('HTAR Took ' + (str(end - start) + ' seconds'))


    print(len(rules_by_pg))
    #self.testCorrectnessAndCompletness(rules_by_pg, apriori_rules)


#
# def exp_HTAR_foodmart_data_1998_correctness_and_completness(self):
#     database = Parser().parse("Datasets/sales_formatted_1998.csv", 'single', None, True)
#     rules_by_pg = HTAR_BY_PG(database, 0.05, 0.01, 0.05)
#     #apriori_rules = apriori(database, 0.00035, 0.01)
#     # print(len(rules_by_pg))
#     # print(len(apriori_rules))
#     print("--------------------NOW THE RULES----------------------------------")
#     print(len(rules_by_pg))
#     # for pg in rules_by_pg:
#     #     print(pg)
#     #     print("-----------")
#     #     print(len(rules_by_pg[pg]))
#     #     print("///////////////////////")

if __name__=="__main__":
    run_HTAR_foodmart_data_1997_correctness_and_completness()