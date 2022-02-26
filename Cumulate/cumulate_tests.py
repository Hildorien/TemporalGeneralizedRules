import unittest

from Cumulate.cumulate import cumulate, vertical_cumulate
from DataStructures.Parser import Parser


class CumulateTests(unittest.TestCase):
    def setUp(self):
        self.foodmart_horizontal_db = Parser().parse_single_file_for_horizontal_database(
            '../Datasets/sales_formatted.csv', '../Taxonomies/salesfact_taxonomy.csv')
        self.foodmart_vertical_db = Parser().parse("../Datasets/sales_formatted.csv", 'single', '../Taxonomies/salesfact_taxonomy.csv')

        self.retail_horizontal_db = Parser().parse_horizontal_database('../Datasets/cumulate_test_data.csv',
                                                                       '../Taxonomies/cumulate_test_taxonomy.csv',
                                                                       'single')
        self.retail_basket_horizontal_db = Parser().parse_horizontal_database('../Datasets/cumulate_test_data_basket.csv',
            '../Taxonomies/cumulate_test_taxonomy.csv', 'basket')
        self.retail_basket_vertical_db = Parser().parse('../Datasets/cumulate_test_data_basket.csv',
                                                        'basket', '../Taxonomies/cumulate_test_taxonomy.csv' )
        self.retail_vertical_db = Parser().parse('../Datasets/cumulate_test_data.csv',
                                                 'single', '../Taxonomies/cumulate_test_taxonomy.csv' )

    def test_cumulate_is_agnostic_wrt_database_representation(self):
        rules_from_single = cumulate(self.retail_horizontal_db, 0.3, 0.6, 0)
        rules_from_basket = cumulate(self.retail_basket_horizontal_db, 0.3, 0.6, 0)
        self.assertEqual(rules_from_single, rules_from_basket, 'Output of cumulate is the same independently of the database representation')

    def test_vertical_cumulate_is_agnostic_wrt_database_representation(self):
        rules_from_single = vertical_cumulate(self.retail_vertical_db, 0.3, 0.6, 0)
        rules_from_basket = vertical_cumulate(self.retail_basket_vertical_db, 0.3, 0.6, 0)
        self.assertEqual(rules_from_single, rules_from_basket,
                         'Output of vertical_cumulate is the same independently of the database representation')

    def test_cumulate_vs_vertical_cumulate_output_is_equal(self):
        cumulate_rules = cumulate(self.retail_horizontal_db, 0.3, 0.6, 0)
        vertical_cumulate_rules = vertical_cumulate(self.retail_vertical_db, 0.3, 0.6, 0)
        self.assertEqual(cumulate_rules, vertical_cumulate_rules, 'Output of vertical_cumulate is the same as cumulate')

    def test_r_interesting_measure_prunes_rules(self):
        rules_without_pruning = cumulate(self.retail_horizontal_db, 0.3, 0.6, 0)
        rule_with_pruning = cumulate(self.retail_horizontal_db, 0.3, 0.6, 1.4)
        self.assertLess(len(rule_with_pruning), len(rules_without_pruning), 'Rules are pruned with R-interesting > 1')

    def test_vertical_cumulate_vs_parallel_count_vertical_cumulate_output_is_equal(self):
        rules_with_parallel_count = vertical_cumulate(self.foodmart_vertical_db, 0.3, 0.6, 0, True)
        rules_without_parallel_count = vertical_cumulate(self.foodmart_vertical_db, 0.3, 0.6, 0)
        self.assertEqual(rules_without_parallel_count, rules_with_parallel_count, 'Output is the same in spite of paralellization')

    def test_vertical_cumulate_vs_parallel_rule_gen_vertical_cumulate_output_is_equal(self):
        rules_with_parallel_rule_gen = vertical_cumulate(self.foodmart_vertical_db, 0.3, 0.6, 0, False, True)
        rules_without_parallel_rule_gen = vertical_cumulate(self.foodmart_vertical_db, 0.3, 0.6, 0)
        self.assertEqual(rules_with_parallel_rule_gen, rules_without_parallel_rule_gen, 'Output is the same in spite of paralellization')
