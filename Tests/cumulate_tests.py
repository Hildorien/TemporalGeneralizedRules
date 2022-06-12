import unittest

from TemporalGeneralizedRules.cumulate import _vertical_cumulate
from TemporalGeneralizedRules.Parser import Parser


class CumulateTests(unittest.TestCase):
    def setUp(self):
        self.foodmart_vertical_db = Parser().parse("Tests/Datasets/sales_formatted_1997.csv",
                                                   'Tests/Taxonomies/salesfact_taxonomy_single_2.csv')
        self.retail_vertical_db = Parser().parse('Tests/Datasets/cumulate_test_data.csv',
                                                 'Tests/Taxonomies/cumulate_test_taxonomy_single.csv')
        self.synthetic_vertical_db = Parser().parse('Tests/Datasets/synthetic_dataset_test_2.csv',
                                                    'Tests/Taxonomies/synthetic_taxonomy_test_2.csv')

    def test_vertical_cumulate_vs_parallel_count_vertical_cumulate_output_is_equal(self):
        rules_with_parallel_count = _vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0, True)
        rules_without_parallel_count = _vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0)
        self.assertEqual(rules_without_parallel_count, rules_with_parallel_count, 'Output is the same in spite of paralellization')

    def test_vertical_cumulate_vs_parallel_rule_gen_vertical_cumulate_output_is_equal(self):
        rules_with_parallel_rule_gen = _vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0, False, True)
        rules_without_parallel_rule_gen = _vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0)
        self.assertEqual(rules_with_parallel_rule_gen, rules_without_parallel_rule_gen, 'Output is the same in spite of paralellization')
