import unittest
from utility import findOrderedIntersection
from utility import getValidJoin
from Apriori.apriori import apriori_gen
from DataStructures.Parser import Parser

#Testing guide: https://docs.python.org/2/library/unittest.html#organizing-test-code

class utilityTests(unittest.TestCase):
    def setUp(self):
        self.arr1 = [0,2,50,721]
        self.arr2 = [2,3,13,23,50]
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
        for l1 in frequents_size_3:
            frequent_dictionary[3][','.join(str(x) for x in l1)] = 0
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
        self.assertEqual(dic['Milk'] , [0,2,3], 'Milk in transactions 0,2 and 3')
