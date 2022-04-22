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

    def test_synthetic_parsing(self):
        synthetic_with_header = Parser().parse('Datasets/synthetic_dataset_test.csv', 'single')
        synthetic_without_header = Parser().parse('Datasets/synthetic_dataset_test_2.csv', 'single')
        self.assertEqual(synthetic_with_header.tx_count, 6)
        self.assertEqual(synthetic_without_header.tx_count, 100)

    def test_basket_parsing(self):
        databases = list(map(lambda dataset: Parser().parse(dataset, 'basket'),
                                  ["Datasets/data.csv",   # 29 item(s), 5 transaction(s)
                                   "Datasets/data2.csv",  # 9 item(s), 274 transaction(s)
                                   "Datasets/data3.csv",  # 9 item(s), 502 transaction(s)
                                   "Datasets/data4.csv",  # 89 item(s), 9903 transaction(s)
                                   "Datasets/data5.csv",  # 2261 item(s), 95286 transaction(s)
                                   "Datasets/data6.csv",  # 1787 item(s), 83335 transaction(s)
                                   "Datasets/data7.csv"]  # 154 item(s), 522661 transaction(s)
                                  ))
        self.assertEqual(databases[0].tx_count, 5)
        self.assertEqual(databases[1].tx_count, 274)
        self.assertEqual(databases[2].tx_count, 502)
        self.assertEqual(databases[3].tx_count, 9903)
        self.assertEqual(databases[4].tx_count, 95286)
        self.assertEqual(databases[5].tx_count, 83335)
        self.assertEqual(databases[6].tx_count, 522661)

    def test_timestamp_with_taxonomy_parsing(self):
        foodmart_97_timestamped_with_taxonomy = Parser().parse('Datasets/sales_formatted_1997_sorted_by_timestamp.csv', 'single',
                                              'Taxonomies/salesfact_taxonomy_single_2.csv', True)
        self.assertEqual(foodmart_97_timestamped_with_taxonomy.tx_count, 20522, 'Orders in foodmart 97')
        self.assertEqual(foodmart_97_timestamped_with_taxonomy.tx_count,
                         len(foodmart_97_timestamped_with_taxonomy.timestamp_mapping.keys()),
                         'Orders should be the same as keys in timestamp_mapping')
        self.assertEqual(len(foodmart_97_timestamped_with_taxonomy.taxonomy),
                         len(foodmart_97_timestamped_with_taxonomy.items_dic), 'Taxonomy size is equal to items_dic')



