import unittest

from Cumulate.cumulate import cumulate, vertical_cumulate
from DataStructures.Parser import Parser


class CumulateTests(unittest.TestCase):
    def setUp(self):
        self.foodmart_horizontal_db = Parser().parse_horizontal_database(
            'Datasets/sales_formatted_1997.csv',
            'Taxonomies/salesfact_taxonomy_single_2.csv',
            'single')
        self.foodmart_vertical_db = Parser().parse("Datasets/sales_formatted_1997.csv",
                                                   'single',
                                                   'Taxonomies/salesfact_taxonomy_single_2.csv')

        self.retail_horizontal_db = Parser().parse_horizontal_database('Datasets/cumulate_test_data.csv',
                                                                       'Taxonomies/cumulate_test_taxonomy_single.csv',
                                                                       'single')
        self.retail_basket_horizontal_db = Parser().parse_horizontal_database(
            'Datasets/cumulate_test_data_basket.csv',
            'Taxonomies/cumulate_test_taxonomy_basket.csv', 'basket')
        self.retail_basket_vertical_db = Parser().parse('Datasets/cumulate_test_data_basket.csv',
                                                        'basket', 'Taxonomies/cumulate_test_taxonomy_basket.csv')
        self.retail_vertical_db = Parser().parse('Datasets/cumulate_test_data.csv',
                                                 'single', 'Taxonomies/cumulate_test_taxonomy_single.csv')
        self.synthetic_vertical_db = Parser().parse('Datasets/synthetic_dataset_test_2.csv', 'single',
                                                    'Taxonomies/synthetic_taxonomy_test_2.csv')
        self.synthetic_horizontal_db = Parser().parse_horizontal_database('Datasets/synthetic_dataset_test_2.csv',
                                                    'Taxonomies/synthetic_taxonomy_test_2.csv', 'single')
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
        retail_cumulate_rules = cumulate(self.retail_horizontal_db, 0.3, 0.6, 0)
        retail_vertical_cumulate_rules = vertical_cumulate(self.retail_vertical_db, 0.3, 0.6, 0)

        foodmart_cumulate_rules = cumulate(self.foodmart_horizontal_db, 0.5, 0.6, 0)
        foodmart_vertical_cumulate_rules = vertical_cumulate(self.foodmart_vertical_db, 0.5, 0.6, 0)

        synthetic_cumulate_rules = cumulate(self.synthetic_horizontal_db, 0.05, 0.6, 0)
        synthetic_verticial_cumulate_rules = vertical_cumulate(self.synthetic_vertical_db, 0.05, 0.6, 0)

        self.assertEqual(retail_cumulate_rules, retail_vertical_cumulate_rules, 'Output of vertical_cumulate is the same as cumulate')
        self.assertEqual(foodmart_cumulate_rules, foodmart_vertical_cumulate_rules, 'Output of vertical_cumulate is the same as cumulate')
        self.assertEqual(synthetic_cumulate_rules, synthetic_verticial_cumulate_rules, 'Output of vertical_cumulate is the same as cumulate')


    def test_r_interesting_measure_prunes_rules(self):
        rules_without_pruning = cumulate(self.retail_horizontal_db, 0.3, 0.6, 0)
        rule_with_pruning = cumulate(self.retail_horizontal_db, 0.3, 0.6, 1.4)
        self.assertLess(len(rule_with_pruning), len(rules_without_pruning), 'Rules are pruned with R-interesting > 1')

    def test_vertical_cumulate_vs_parallel_count_vertical_cumulate_output_is_equal(self):
        rules_with_parallel_count = vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0, True)
        rules_without_parallel_count = vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0)
        self.assertEqual(rules_without_parallel_count, rules_with_parallel_count, 'Output is the same in spite of paralellization')

    def test_vertical_cumulate_vs_parallel_rule_gen_vertical_cumulate_output_is_equal(self):
        rules_with_parallel_rule_gen = vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0, False, True)
        rules_without_parallel_rule_gen = vertical_cumulate(self.foodmart_vertical_db, 0.4, 0.6, 0)
        self.assertEqual(rules_with_parallel_rule_gen, rules_without_parallel_rule_gen, 'Output is the same in spite of paralellization')

    def test_taxonomy_is_agnostic_wrt_database_representation(self):
        taxonomy_retail_vertical_db_basket_format = self.retail_basket_vertical_db.taxonomy
        taxonomy_retail_horizontal_db_basket_format = self.retail_basket_horizontal_db.taxonomy
        taxonomy_retail_vertical_db_single_format = self.retail_vertical_db.taxonomy
        taxonomy_retail_horizontal_db_single_format = self.retail_horizontal_db.taxonomy

        taxonomy_foodmart_horizontal_db_single_format = self.foodmart_horizontal_db.taxonomy
        taxonomy_foodmart_vertical_db_single_format = self.foodmart_vertical_db.taxonomy

        self.assertEqual(taxonomy_retail_vertical_db_basket_format, taxonomy_retail_horizontal_db_basket_format)
        self.assertEqual(taxonomy_retail_vertical_db_basket_format, taxonomy_retail_vertical_db_single_format)
        self.assertEqual(taxonomy_retail_vertical_db_basket_format, taxonomy_retail_horizontal_db_single_format)
        self.assertEqual(taxonomy_retail_horizontal_db_basket_format, taxonomy_retail_vertical_db_single_format)
        self.assertEqual(taxonomy_retail_horizontal_db_basket_format, taxonomy_retail_horizontal_db_single_format)
        self.assertEqual(taxonomy_retail_vertical_db_single_format, taxonomy_retail_horizontal_db_single_format)
        self.assertEqual(taxonomy_foodmart_horizontal_db_single_format, taxonomy_foodmart_vertical_db_single_format)

