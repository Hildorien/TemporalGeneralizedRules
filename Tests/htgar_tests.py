import time
import unittest

from Apriori.apriori import apriori
from DataStructures.Parser import Parser
from HTAR.HTAR import HTAR_BY_PG, getGranulesFrequentsAndSupports
from Tests.htar_tests import HTARTests
from utility import flatten_list


class HTGARTests(unittest.TestCase):

    def setUp(self) -> None:
        # self.foodmart_97 = Parser().parse("Datasets/sales_formatted_1997_sorted_by_timestamp.csv",
        #                                            'single',
        #                                            'Taxonomies/salesfact_taxonomy_single_2.csv', True)
        # self.foodmart_98 = Parser().parse("Datasets/sales_formatted_1998_sorted_by_timestamp.csv",
        #                                            'single',
        #                                            'Taxonomies/salesfact_taxonomy_single_2.csv', True)
        # self.synthetic_database = Parser().parse("F:\TesisSyntheticDatasets\Root\R1000T250k-timestamped.csv",
        #                                          'single',
        #                                          "F:\TesisSyntheticDatasets\Root\R1000T250k.tax", True)

        self.sales_formatted_for_test = Parser().parse("Datasets/sales_formatted_for_test.csv",
                                                       'single',
                                                        "Taxonomies/sales_formatted_for_test_taxonomy.csv",
                                                       True)

    def test_HTGAR_correctness(self):
        rules = HTAR_BY_PG(self.sales_formatted_for_test, 0.005, 0.5, 0.005, False, False, False, False, 2,
                               [24,12,4,1], True, 1.1)
        all_rules = flatten_list(list(rules.values()))
        self.assertTrue(any(self.sales_formatted_for_test.is_ancestral_rule(
            rule.getItemset()) for rule in all_rules))

    def test_HTGAR_R_interesting_prunes_rules(self):
        rules_pruned = HTAR_BY_PG(self.sales_formatted_for_test, 0.005, 0.5, 0.005, False, False, False, False, 2,
                               [24,12,4,1], True, 1.1)
        rules = HTAR_BY_PG(self.sales_formatted_for_test, 0.005, 0.5, 0.005, False, False, False, False, 2,
                               [24,12,4,1], True)
        self.assertGreater(len(flatten_list(list(rules.values()))),
                           len(flatten_list(list(rules_pruned.values()))))

