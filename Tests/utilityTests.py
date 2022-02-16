import unittest

from DataStructures.PTT import PTT
from utility import findOrderedIntersection
from utility import getValidJoin
from utility import getPeriodStampFromTimestamp
from Apriori.apriori import apriori_gen
from DataStructures.Parser import Parser


# Testing guide: https://docs.python.org/2/library/unittest.html#organizing-test-code

class utilityTests(unittest.TestCase):
    def setUp(self):
        self.arr1 = [0, 2, 50, 721]
        self.arr2 = [2, 3, 13, 23, 50]
        self.t1 = 631234984  # 1/1/1990
        self.t2 = 503276584  # 12/12/1985
        self.t3 = 1118962984  # 16/6/2005
        self.t4 = 1442876584  # 21/9/2015

    def test_find_ordered_itersection(self):
        result = findOrderedIntersection(self.arr1, self.arr2)
        self.assertEqual(len(result), 2, 'incorrect default size')

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
        dic = parser.create_matrix_dictionary(dataset)
        self.assertEqual('Milk' in dic, True, 'Milk in dataset')
        self.assertEqual(dic['Milk']['tids'], [0, 2, 3], 'Milk in transactions 0,2 and 3')

    def test_parse_taxonomy(self):
        parser = Parser()
        taxonomy = parser.parse_taxonomy('Taxonomies/salesfact_taxonomy.csv')
        # Assert that taxonomy builds with all items in database
        self.assertEqual(len(list(taxonomy.keys())), 1560, 'Taxonomy has ' + str(1560) + ' items in database')

    def test_get_period_stamp_from_timestamp(self):
        self.assertEqual(getPeriodStampFromTimestamp(self.t1), [1, 1, 1], 'Periodstamp 1')
        self.assertEqual(getPeriodStampFromTimestamp(self.t2), [23, 12, 4], 'Periodstamp 2')
        self.assertEqual(getPeriodStampFromTimestamp(self.t3), [12, 6, 2], 'Periodstamp 3')
        self.assertEqual(getPeriodStampFromTimestamp(self.t4), [18, 9, 3], 'Periodstamp 4')

    def test_parse_horizontal_database(self):
        parser = Parser()
        horizontal_db = parser.parse_single_file_for_horizontal_database('Datasets/sales_formatted.csv',
                                                                         'Taxonomies/salesfact_taxonomy.csv')
        self.assertEqual(len(horizontal_db.transactions), 54537, 'Transactions in horizontal DB')

    def test_ptt(self):
        ptt = PTT()
        periodsTimestamp = [self.t1, self.t2, self.t3, self.t4, self.t4, self.t4]
        periods = list(map(getPeriodStampFromTimestamp, periodsTimestamp))
        ptt.addMultiplePeriods(periods)
        self.assertEqual(ptt.getPTTValueFromLlevelAndPeriod(1, 1), 1, 'PTT value 1')
        self.assertEqual(ptt.getPTTValueFromLlevelAndPeriod(1, 2), 0, 'PTT value 2')
        self.assertEqual(ptt.getPTTValueFromLlevelAndPeriod(2, 12), 1, 'PTT value 3')
        self.assertEqual(ptt.getPTTValueFromLlevelAndPeriod(1, 18), 3, 'PTT value 4')
        self.assertEqual(ptt.getPTTValueFromLlevelAndPeriod(3, 3), 3, 'PTT value 5')




