import unittest
import time

from DataStructures.Parser import Parser



class ParserTests(unittest.TestCase):

    def test_foodmart_parsing(self):

        sales_formatted_test = Parser().parse('Datasets/sales_formatted_for_test.csv', 'single')
        self.assertEqual(sales_formatted_test.tx_count, 8)
        self.assertEqual(sales_formatted_test.tx_count, len(sales_formatted_test.timestamp_mapping.keys()))

        foodmart_97_with_taxonomy_vertical = Parser().parse('Datasets/sales_formatted_1997.csv', 'single', 'Taxonomies/salesfact_taxonomy_single_2.csv')
        foodmart_98_with_taxonomy_vertical = Parser().parse('Datasets/sales_formatted_1998.csv', 'single', 'Taxonomies/salesfact_taxonomy_single_2.csv')
        foodmart_97_without_taxonomy_vertical = Parser().parse('Datasets/sales_formatted_1997.csv', 'single')
        foodmart_98_without_taxonomy_vertical = Parser().parse('Datasets/sales_formatted_1998.csv', 'single')

        self.assertEqual(foodmart_97_with_taxonomy_vertical.tx_count, 20522, 'Orders in foodmart 97')
        self.assertEqual(foodmart_97_with_taxonomy_vertical.tx_count,
                         len(foodmart_97_with_taxonomy_vertical.timestamp_mapping.keys()),
                         'Orders should be the same as keys in timestamp_mapping')
        self.assertEqual(foodmart_98_with_taxonomy_vertical.tx_count,
                         len(foodmart_98_with_taxonomy_vertical.timestamp_mapping.keys()),
                         'Orders should be the same as keys in timestamp_mapping')
        self.assertEqual(foodmart_98_with_taxonomy_vertical.tx_count, 34015, 'Orders in foodmart 98')
        self.assertEqual(foodmart_97_without_taxonomy_vertical.tx_count, 20522, 'Orders in foodmart 97')
        self.assertEqual(foodmart_98_without_taxonomy_vertical.tx_count, 34015, 'Orders in foodmart 98')
        self.assertEqual(len(foodmart_97_with_taxonomy_vertical.items_dic.keys()),
                         len(foodmart_97_with_taxonomy_vertical.taxonomy.keys()),
                         'Inventory should be equal in taxonomy and items_dic')
        self.assertEqual(len(list(foodmart_98_with_taxonomy_vertical.items_dic.keys())),
                         len(list(foodmart_98_with_taxonomy_vertical.taxonomy.keys())),
                         'Inventory should be equal in taxonomy and items_dic')

    def test_retail_parsing(self):

        retail_vertical = Parser().parse('Datasets/cumulate_test_data.csv', 'single',
                                                      'Taxonomies/cumulate_test_taxonomy_single.csv')

        retail_basket_vertical_db = Parser().parse('Datasets/cumulate_test_data_basket.csv', 'basket',
                                                                'Taxonomies/cumulate_test_taxonomy_basket.csv')

        self.assertEqual(retail_vertical.tx_count, retail_basket_vertical_db.tx_count, 'Retail database should have equal orders despite of csv format')
        self.assertEqual(len(list(retail_vertical.taxonomy.keys())),
                         len(list(retail_basket_vertical_db.taxonomy.keys())),
                         'Retail database should have equal inventory despite csv format')