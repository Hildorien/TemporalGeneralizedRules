import unittest

from Cumulate.cumulate import vertical_cumulate
from DataStructures.Parser import Parser


class CumulateTests(unittest.TestCase):
    def setUp(self):
        self.foodmart_vertical_db = Parser().parse("Datasets/sales_formatted_1997.csv",
                                                   'Taxonomies/salesfact_taxonomy_single_2.csv')
        self.retail_vertical_db = Parser().parse('Datasets/cumulate_test_data.csv',
                                                 'Taxonomies/cumulate_test_taxonomy_single.csv')
        self.synthetic_vertical_db = Parser().parse('Datasets/synthetic_dataset_test_2.csv',
                                                    'Taxonomies/synthetic_taxonomy_test_2.csv')

    def test_vertical_cumulate_vs_parallel_count_vertical_cumulate_output_is_equal(self):
        rules_with_parallel_count = vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0, True)
        rules_without_parallel_count = vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0)
        self.assertEqual(rules_without_parallel_count, rules_with_parallel_count, 'Output is the same in spite of paralellization')

    def test_vertical_cumulate_vs_parallel_rule_gen_vertical_cumulate_output_is_equal(self):
        rules_with_parallel_rule_gen = vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0, False, True)
        rules_without_parallel_rule_gen = vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0)
        self.assertEqual(rules_with_parallel_rule_gen, rules_without_parallel_rule_gen, 'Output is the same in spite of paralellization')
