import unittest
from utility import findOrderedIntersection

#Testing guide: https://docs.python.org/2/library/unittest.html#organizing-test-code

class utilityTests(unittest.TestCase):
    def setUp(self):
        self.arr1 = [0,2,50,721]
        self.arr2 = [2,3,13,23,50]
    def test_find_ordered_itersection(self):
        result = findOrderedIntersection(self.arr1, self.arr2)
        self.assertEqual(len(result), 2, 'incorrect default size')