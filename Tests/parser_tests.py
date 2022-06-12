import unittest
from TemporalGeneralizedRules.Parser import Parser



class ParserTests(unittest.TestCase):

    def test_foodmart_parsing(self):

        sales_formatted_test = Parser().parse('Tests/Datasets/sales_formatted_for_test.csv')
        self.assertEqual(sales_formatted_test.tx_count, 8)
        self.assertEqual(sales_formatted_test.tx_count, len(sales_formatted_test.timestamp_mapping.keys()))

        foodmart_97_with_taxonomy_vertical = Parser().parse('Tests/Datasets/sales_formatted_1997.csv', 'Tests/Taxonomies/salesfact_taxonomy_single_2.csv')
        foodmart_98_with_taxonomy_vertical = Parser().parse('Tests/Datasets/sales_formatted_1998.csv',  'Tests/Taxonomies/salesfact_taxonomy_single_2.csv')
        foodmart_97_without_taxonomy_vertical = Parser().parse('Tests/Datasets/sales_formatted_1997.csv')
        foodmart_98_without_taxonomy_vertical = Parser().parse('Tests/Datasets/sales_formatted_1998.csv')

        foodmart_97_with_taxonomy_and_timestamps = Parser().parse("Tests/Datasets/sales_formatted_1997_sorted_by_timestamp.csv",
                                          'Tests/Taxonomies/salesfact_taxonomy_single_2.csv', True)
        foodmart_98_with_taxonomy_and_timestamps = Parser().parse("Tests/Datasets/sales_formatted_1998_sorted_by_timestamp.csv",
                                          'Tests/Taxonomies/salesfact_taxonomy_single_2.csv', True)

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

        self.assertEqual(len(foodmart_97_with_taxonomy_and_timestamps.items_dic.keys()),
                         len(foodmart_97_with_taxonomy_and_timestamps.taxonomy.keys()),
                         'Inventory should be equal in taxonomy and items_dic')
        self.assertEqual(len(foodmart_98_with_taxonomy_and_timestamps.items_dic.keys()),
                         len(foodmart_98_with_taxonomy_and_timestamps.taxonomy.keys()),
                         'Inventory should be equal in taxonomy and items_dic')

    def test_synthetic_parsing(self):
        synthetic_with_header = Parser().parse('Tests/Datasets/synthetic_dataset_test.csv')
        synthetic_without_header = Parser().parse('Tests/Datasets/synthetic_dataset_test_2.csv')
        self.assertEqual(synthetic_with_header.tx_count, 6)
        self.assertEqual(synthetic_without_header.tx_count, 100)

    def test_timestamp_with_taxonomy_parsing(self):
        foodmart_97_timestamped_with_taxonomy = Parser().parse('Tests/Datasets/sales_formatted_1997_sorted_by_timestamp.csv',
                                                                'Tests/Taxonomies/salesfact_taxonomy_single_2.csv', True)
        self.assertEqual(foodmart_97_timestamped_with_taxonomy.tx_count, 20522, 'Orders in foodmart 97')
        self.assertEqual(foodmart_97_timestamped_with_taxonomy.tx_count,
                         len(foodmart_97_timestamped_with_taxonomy.timestamp_mapping.keys()),
                         'Orders should be the same as keys in timestamp_mapping')
        self.assertEqual(len(foodmart_97_timestamped_with_taxonomy.taxonomy),
                         len(foodmart_97_timestamped_with_taxonomy.items_dic), 'Taxonomy size is equal to items_dic')

    def tests_only_ancestors_are_contained_in_taxonomy(self):
        foodmart_97_with_taxonomy_and_timestamps = Parser().parse("Tests/Datasets/sales_formatted_1997_sorted_by_timestamp.csv",
                                                                    'Tests/Taxonomies/salesfact_taxonomy_single_2.csv', True)
        self.ancestors_are_contained_in_taxonomy(foodmart_97_with_taxonomy_and_timestamps)


    def ancestors_are_contained_in_taxonomy(self, foodmart_97_with_taxonomy_and_timestamps):
        tids_dic = foodmart_97_with_taxonomy_and_timestamps.matrix_data_by_item
        only_ancestors = foodmart_97_with_taxonomy_and_timestamps.only_ancestors
        items_dic = foodmart_97_with_taxonomy_and_timestamps.items_dic
        taxonomy = foodmart_97_with_taxonomy_and_timestamps.taxonomy
        for item_name in tids_dic:
            is_ancestor = item_name in only_ancestors
            if is_ancestor:
                self.assertEqual(self.item_is_parent_of_somebody(self.get_key(items_dic, item_name), taxonomy), True,
                                 'Ancestor is parent of someone')

    def item_is_parent_of_somebody(self, item, taxonomy):
        for key in taxonomy:
            ancestors = taxonomy[key]
            if item in ancestors:
                return True
        return False

    def get_key(self, dict, val):
        for key, value in dict.items():
            if val == value:
                return key


