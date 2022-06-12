import unittest
from TemporalGeneralizedRules.utility import findOrderedIntersection, apriori_gen, binary_search_or_first_higher_value, \
    findOrderedIntersectionBetweenBoundaries, maximal_time_period_interval, getFilteredTIDSThatBelongToPeriod
from TemporalGeneralizedRules.utility import getValidJoin
from TemporalGeneralizedRules.Parser import Parser


# Testing guide: https://docs.python.org/2/library/unittest.html#organizing-test-code

class UtilityTests(unittest.TestCase):
    def setUp(self):
        self.arr1 = [0, 2, 50, 721]
        self.arr2 = [2, 3, 13, 23, 50]
        self.arr3 = [5, 6, 8, 10, 15, 20, 25, 30]
        self.arr4 = [4, 5, 6, 7, 11, 12, 18, 20]

    def test_binary_search(self):
        self.assertEqual(binary_search_or_first_higher_value(self.arr1, 50, 0, len(self.arr1)-1), 2, 'binary_search_test_1')
        self.assertEqual(binary_search_or_first_higher_value(self.arr1, 2, 2, len(self.arr1)-1), 2, 'binary_search_test_2')
        self.assertEqual(binary_search_or_first_higher_value(self.arr2, 14, 0, len(self.arr2)-1), 3, 'binary_search_test_3')
        self.assertEqual(binary_search_or_first_higher_value(self.arr1, 1, 0, len(self.arr1)-1), 1, 'binary_search_test_4')
        self.assertEqual(binary_search_or_first_higher_value(self.arr3, 5, 0, len(self.arr3)-1), 0, 'binary_search_test_5')


    def test_maximal_time_period_interval(self):
        tuples = [None, None, [1, 5], None, [6, 6], None, [7, 10], None, None]
        self.assertEqual(maximal_time_period_interval(tuples, 0, 3), [1, 5], 'tuple_maximal_interval_1')
        self.assertEqual(maximal_time_period_interval(tuples, 1, 4), [1, 6], 'tuple_maximal_interval_2')
        self.assertEqual(maximal_time_period_interval(tuples, 0, 8), [1, 10], 'tuple_maximal_interval_3')
        self.assertEqual(maximal_time_period_interval(tuples, 3, 6), [6, 10], 'tuple_maximal_interval_4')
        self.assertEqual(maximal_time_period_interval(tuples, 7, 8), [None, None], 'tuple_maximal_interval_5')
        self.assertEqual(maximal_time_period_interval(tuples, 0, 1), [None, None], 'tuple_maximal_interval_6')
        self.assertEqual(maximal_time_period_interval(tuples, 3, 5), [6, 6], 'tuple_maximal_interval_7')
        self.assertEqual(maximal_time_period_interval(tuples, 2, 2), [1, 5], 'tuple_maximal_interval_8')

    def test_find_ordered_itersection(self):
        result = findOrderedIntersection(self.arr1, self.arr2)
        self.assertEqual(len(result), 2, 'incorrect default size')

    def test_find_ordered_intersection_with_boundaries(self):
        self.assertEqual(findOrderedIntersectionBetweenBoundaries(self.arr3, self.arr4, 2, 10), [5, 6], 'ordered intersection with boundaries 1')
        self.assertEqual(findOrderedIntersectionBetweenBoundaries(self.arr4, self.arr3, 2, 10), [5, 6], 'ordered intersection with boundaries 2')
        self.assertEqual(findOrderedIntersectionBetweenBoundaries(self.arr4, self.arr3, 25, 30), [], 'ordered intersection with boundaries 3')
        self.assertEqual(findOrderedIntersectionBetweenBoundaries(self.arr4, self.arr3, 0, 4), [], 'ordered intersection with boundaries 4')
        self.assertEqual(findOrderedIntersectionBetweenBoundaries(self.arr4, self.arr3, 100, 300), [], 'ordered intersection with boundaries 5')
        self.assertEqual(findOrderedIntersectionBetweenBoundaries(self.arr4, self.arr3, 4, 30), [5, 6, 20], 'ordered intersection with boundaries 6')
        self.assertEqual(findOrderedIntersectionBetweenBoundaries(self.arr4, self.arr3, 9, 17), [], 'ordered intersection with boundaries 7')
        self.assertEqual(findOrderedIntersectionBetweenBoundaries(self.arr4, self.arr3, -1, -1), [], 'ordered intersection with boundaries 8')
        self.assertEqual(findOrderedIntersectionBetweenBoundaries([5], [5], 5, 5), [5], 'ordered intersection with boundaries 9')

    def test_getFiltered_TIDS_between_periods(self):
        tids = [2, 4, 5, 7, 8, 10, 14, 35, 66]
        self.assertEqual(getFilteredTIDSThatBelongToPeriod(tids, 3, 6), [4, 5], 'filter_tids_between_bounds')
        self.assertEqual(getFilteredTIDSThatBelongToPeriod(tids, 5, 14), [5, 7, 8, 10, 14], 'filter_tids_between_bounds_2')
        self.assertEqual(getFilteredTIDSThatBelongToPeriod(tids, 0, 1), [], 'filter_tids_between_bounds_3')
        self.assertEqual(getFilteredTIDSThatBelongToPeriod(tids, 100, 101), [], 'filter_tids_between_bounds_4')
        self.assertEqual(getFilteredTIDSThatBelongToPeriod(tids, 35, 35), [35], 'filter_tids_between_bounds_5')



    def test_valid_join(self):
        orderedlist1 = [1, 2, 3]
        orderedlist2 = [1, 2, 4]
        self.assertEqual(getValidJoin(orderedlist1, orderedlist2), [1, 2, 3, 4], 'valid join')
        orderedlist1 = [1, 2, 4]
        orderedlist2 = [1, 3, 5]
        self.assertEqual(getValidJoin(orderedlist1, orderedlist2), None, 'invalid join')

    def test_join_frequents_size_3(self):
        frequents_size_3 = [[1, 2, 3], [1, 2, 4], [1, 3, 4], [1, 3, 5], [2, 3, 4]]
        frequent_dictionary = {3: {}}
        frequent_dictionary[3] = frequents_size_3
        self.assertEqual(apriori_gen(frequents_size_3, frequent_dictionary), [[1, 2, 3, 4]], 'candidates of size 4')

    def test_matrix_dictionary_from_database(self):
        dataset = [['Milk', 'Onion', 'Nutmeg', 'Kidney Beans', 'Eggs', 'Yogurt'],
                   ['Dill', 'Onion', 'Nutmeg', 'Kidney Beans', 'Eggs', 'Yogurt'],
                   ['Milk', 'Apple', 'Kidney Beans', 'Eggs'],
                   ['Milk', 'Unicorn', 'Corn', 'Kidney Beans', 'Yogurt'],
                   ['Corn', 'Onion', 'Onion', 'Kidney Beans', 'Ice cream', 'Eggs']]
        parser = Parser()
        dic = parser.create_matrix_dictionary(dataset)["md"]
        self.assertEqual('Milk' in dic, True, 'Milk in dataset')
        self.assertEqual(dic['Milk']['tids'], [0, 2, 3], 'Milk in transactions 0,2 and 3')

    def test_transaction_id_intersection(self):
        database = Parser().parse('Tests/Datasets/sales_formatted_for_test.csv', None, True)
        self.assertEqual(database.transaction_ids_intersection([0, 2]), [4, 5], 'Transaction_id_intersection_test_1')
        self.assertEqual(database.transaction_ids_intersection([2, 1, 0]), [], 'Transaction_id_intersection_test_2')
        self.assertEqual(database.transaction_ids_intersection([3, 6]), [1, 3], 'Transaction_id_intersection_test_3')
        self.assertEqual(database.transaction_ids_intersection([0, 7, 8, 2]), [5], 'Transaction_id_intersection_test_4')








