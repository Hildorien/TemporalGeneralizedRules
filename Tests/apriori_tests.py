import unittest
from Apriori.apriori import apriori, apriori_parallel_count
from DataStructures.Parser import Parser


class AprioriTests(unittest.TestCase):
    def setUp(self):
        self.databases = list(map(lambda dataset: Parser().parse(dataset, 'basket'),
                                  ["Datasets/data.csv",   # 29 item(s), 5 transaction(s)
                                   "Datasets/data2.csv",  # 9 item(s), 274 transaction(s)
                                   "Datasets/data3.csv",  # 9 item(s), 502 transaction(s)
                                   "Datasets/data4.csv",  # 89 item(s), 9903 transaction(s)
                                   "Datasets/data5.csv",  # 2261 item(s), 95286 transaction(s)
                                   "Datasets/data6.csv",  # 1787 item(s), 83335 transaction(s)
                                   "Datasets/data7.csv"]  # 154 item(s), 522661 transaction(s)
                                  ))

    def test_apriori_correctness(self):
        # All outputs where checked against apriori from arules package in R with the same parameters
        rules_database_1 = apriori(self.databases[0], 0.2, 0.6)
        rules_database_2 = apriori(self.databases[1], 0.7, 0.6)
        rules_database_3 = apriori(self.databases[2], 0.7, 0.6)
        rules_database_4 = apriori(self.databases[3], 0.3, 0.6)
        rules_database_5 = apriori(self.databases[4], 0.2, 0.6)
        rules_database_6 = apriori(self.databases[5], 0.01, 0.4)
        rules_database_7 = apriori(self.databases[6], 0.09, 0.6)

        # Assert Quantity
        self.assertEqual(len(rules_database_1), 3097, 'apriori(0.2, 0.6) with Database 1 has 3097 rules')
        self.assertEqual(len(rules_database_2), 2, 'apriori(0.7, 0.6) with Database 2 has 2 rules')
        self.assertEqual(len(rules_database_3), 2, 'apriori(0.7, 0.6) with Database 3 has 2 rules')
        self.assertEqual(len(rules_database_4), 54, 'apriori(0.3, 0.6) with Database 4 has 54 rules')
        self.assertEqual(len(rules_database_5), 0, 'apriori(0.2, 0.6) with Database 5 has 0 rules')
        self.assertEqual(len(rules_database_6), 33, 'apriori(0.01, 0.4) with Database 6 has 33 rules')
        self.assertEqual(len(rules_database_7), 17, 'apriori(0.09, 0.6) with Database 7 has 17 rules')

    def test_apriori_parallel_count_correctness(self):
        # All outputs where checked against apriori from arules package in R with the same parameters
        # Only testing with one dataset in map_reduce to reduce the time to run all tests
        rules_database_6 = apriori_parallel_count(self.databases[5], 0.01, 0.4)

        # Assert Quantity
        self.assertEqual(len(rules_database_6), 33, 'apriori_mapreduce(0.09, 0.6) with Database 6 has 33 rules')

    def test_apriori_vs_apriori_parallel_count_output_is_equal(self):
        apriori_rules = apriori(self.databases[5], 0.01, 0.4)
        apriori_map_reduce_rules = apriori_parallel_count(self.databases[5], 0.01, 0.4)
        self.assertEqual(apriori_rules, apriori_map_reduce_rules, 'Output is the same in spite of paralellization')
