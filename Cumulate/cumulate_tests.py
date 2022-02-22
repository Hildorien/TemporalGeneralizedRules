import unittest

from Cumulate.cumulate import cumulate
from DataStructures.Parser import Parser


class CumulateTests(unittest.TestCase):
    def setUp(self):
        self.foodmart_horizontal_db = Parser().parse_single_file_for_horizontal_database(
            '../Datasets/sales_formatted.csv', '../Taxonomies/salesfact_taxonomy.csv')
        self.retail_horizontal_db = Parser().parse_horizontal_database('../Datasets/cumulate_test_data.csv',
                                                                       '../Taxonomies/cumulate_test_taxonomy.csv',
                                                                       'single')
        self.retail_basket_horizontal_db = Parser().parse_horizontal_database(
            '../Datasets/cumulate_test_data_basket.csv',
            '../Taxonomies/cumulate_test_taxonomy.csv', 'basket')

    def test_cumulate_idempotency_on_database_type(self):
        rules_from_single = cumulate(self.retail_horizontal_db, 0.3, 0.6, 0)
        rules_from_basket = cumulate(self.retail_basket_horizontal_db, 0.3, 0.6, 0)
        self.assertEqual(rules_from_single, rules_from_basket, 'Rules are the same')
