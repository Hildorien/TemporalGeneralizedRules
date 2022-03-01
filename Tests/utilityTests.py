import unittest

from Apriori.apriori import findIndividualTFI, HTAR_BY_PG
from DataStructures.PTT import PTT
from utility import findOrderedIntersection, apriori_gen, getTFIUnion, getPeriodsIncluded, \
    getMCPOfItemsetBetweenBoundaries
from utility import getValidJoin
from utility import getPeriodStampFromTimestamp
from DataStructures.Parser import Parser


# Testing guide: https://docs.python.org/2/library/unittest.html#organizing-test-code

class utilityTests(unittest.TestCase):
    def setUp(self):
        self.arr1 = [0, 2, 50, 721]
        self.arr2 = [2, 3, 13, 23, 50]
        self.t1 = 631234984  # 1/1/1990 - [1,1,1]
        self.t2 = 503276584  # 12/12/1985 - [23,12,4]
        self.t3 = 1118962984  # 16/6/2005 - [12, 6, 2]
        self.t4 = 1442876584  # 21/9/2015 - [18, 9, 3]
        self.t5 = 1117811573  # 3/6/2005 - [11, 6, 2]

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
        dic = parser.create_matrix_dictionary(dataset)["md"]
        self.assertEqual('Milk' in dic, True, 'Milk in dataset')
        self.assertEqual(dic['Milk']['tids'], [0, 2, 3], 'Milk in transactions 0,2 and 3')


    def test_get_period_stamp_from_timestamp(self):
        self.assertEqual(getPeriodStampFromTimestamp(self.t1), [1, 1, 1, 1], 'Periodstamp 1')
        self.assertEqual(getPeriodStampFromTimestamp(self.t2), [23, 12, 4, 1], 'Periodstamp 2')
        self.assertEqual(getPeriodStampFromTimestamp(self.t3), [12, 6, 2, 1], 'Periodstamp 3')
        self.assertEqual(getPeriodStampFromTimestamp(self.t4), [18, 9, 3, 1], 'Periodstamp 4')

    def test_parse_horizontal_database(self):
        parser = Parser()
        horizontal_db = parser.parse_single_file_for_horizontal_database('Datasets/sales_formatted.csv',
                                                                         'Taxonomies/salesfact_taxonomy.csv')
        self.assertEqual(len(horizontal_db.transactions), 54537, 'Transactions in horizontal DB')

    def test_ptt(self):
        customPtt = PTT()
        periodsTimestamp = [self.t1, self.t2, self.t3, self.t4, self.t4, self.t4]
        periods = list(map(getPeriodStampFromTimestamp, periodsTimestamp))
        customPtt.addMultiplePeriods(periods)
        self.assertEqual(customPtt.getPTTValueFromLlevelAndPeriod(0, 1)['totalTransactions'], 1, 'PTT value 1')
        self.assertEqual(customPtt.getPTTValueFromLlevelAndPeriod(0, 2)['totalTransactions'], 0, 'PTT value 2')
        self.assertEqual(customPtt.getPTTValueFromLlevelAndPeriod(1, 12)['totalTransactions'], 1, 'PTT value 3')
        self.assertEqual(customPtt.getPTTValueFromLlevelAndPeriod(0, 18)['totalTransactions'], 3, 'PTT value 4')
        self.assertEqual(customPtt.getPTTValueFromLlevelAndPeriod(0, 12)['totalTransactions'], 1, 'PTT value 5')
        self.assertEqual(customPtt.getPTTValueFromLlevelAndPeriod(0, 23)['totalTransactions'], 1, 'PTT value 6')
        self.assertEqual(customPtt.getPTTValueFromLlevelAndPeriod(2, 3)['totalTransactions'], 3, 'PTT value 7')
        self.assertEqual(customPtt.getPTTValueFromLlevelAndPeriod(3, 1)['totalTransactions'], 6, 'PTT value 7')

        self.assertEqual(customPtt.getTotalPTTSumWithinPeriodsInLevel0([1, 2]), 1, 'PTT SUM 1')
        self.assertEqual(customPtt.getTotalPTTSumWithinPeriodsInLevel0([1, 18]), 5, 'PTT SUM 2')

    def test_support_with_time_period(self):
        # T1 = ([A,B], [23,12,4])
        # T2 = ([L,F,G,D], [11,6,2])
        # T3 = ([C,D,A,F], [12,6,2])
        # T4 = ([A,J,K,C],  [18,9,3])
        # T5 = ([A], [18,9,3])
        # T6 = ([C,E,G,K], [8,4,2])
        # T7 = ([D,F,G], [8,4,2])
        # ItemsDic = {0: 'A', 1: 'B', 2: 'C', 3: 'D', 4: 'E', 5: 'F', 6: 'G', 7: 'J', 8: 'K', 9:'L'}
        parser = Parser()
        database = parser.parse("Datasets/sales_formatted_for_test.csv", 'single', None, True)
        self.assertEqual(database.supportOf([0]), 4/7, 'SupportTestVanilla1')
        self.assertEqual(database.supportOf([0,2]), 2/7, 'SupportTestVanilla2')
        self.assertEqual(database.supportOf([0], 0, 18), 1, 'SupportTestWithTimePeriod1')
        self.assertEqual(database.supportOf([3], 0, 8), 1/2, 'SupportTestWithTimePeriod2')
        self.assertEqual(database.supportOf([3], 0, 8), 1/2, 'SupportTestWithTimePeriod3')
        self.assertEqual(database.supportOf([0], 2, 2), 1 / 4, 'SupportTestWithTimePeriod4')
        self.assertEqual(database.supportOf([3, 5], 2, 2), 3 / 4, 'SupportTestWithTimePeriod5')
        self.assertEqual(database.supportOf([3, 5, 6], 1, 4), 1 / 2, 'SupportTestWithTimePeriod6')
        self.assertEqual(database.supportOf([8], 0, 1), 0, 'SupportTestWithTimePeriod7')

        #Now assert items =============================================================
        self.assertEqual(len(database.getItemsByDepthAndPeriod(0,1)), 0, 'Items_in_time_period_test_1')
        items_in_0_8 = database.getItemsByDepthAndPeriod(0,8)
        self.assertEqual(len(items_in_0_8), 6, 'Items_in_time_period_test_2a')
        self.assertEqual(5 in items_in_0_8, True, 'Items_in_time_period_test_2b')
        self.assertEqual(7 in items_in_0_8, False, 'Items_in_time_period_test_2c')
        self.assertEqual(len(database.getItemsByDepthAndPeriod(2, 2)), 8, 'Items_in_time_period_test_3')
        self.assertEqual(len(database.getItemsByDepthAndPeriod(1, 2)), 0, 'Items_in_time_period_test_3b')
        self.assertEqual(len(database.getItemsByDepthAndPeriod(1, 6)), 6, 'Items_in_time_period_test_3c')

    def test_TFI(self):
        parser = Parser()
        database = parser.parse("Datasets/sales_formatted_for_test.csv", 'single', None, True)
        tfi_0_8 = findIndividualTFI(database, 0, 8, 0.5)["TFI"]
        tfi_0_8_lower_lamda = findIndividualTFI(database, 0, 8, 0.02)["TFI"]

        tfi_2_2 = findIndividualTFI(database, 2, 2, 0.49)["TFI"]
        tfi_0_5 = findIndividualTFI(database, 0, 5, 0.02)["TFI"]
        self.assertEqual(tfi_0_8[1], {(6,)}, 'TFI-1')
        self.assertEqual(len(tfi_0_8.keys()), 1, 'TFI-1B')
        self.assertEqual((5,) in tfi_2_2[1], True, 'TFI-2')
        self.assertEqual((3, 5) in tfi_2_2[2], True, 'TFI-2B')
        self.assertEqual(len(tfi_0_5.keys()), 0, 'TFI-3')

        tfi_0_11 = findIndividualTFI(database, 0, 11, 0.02)["TFI"]
        tfi_0_12 = findIndividualTFI(database, 0, 12, 0.02)["TFI"]
        TFI_by_period = {11: tfi_0_11, 12: tfi_0_12, 8: tfi_0_8_lower_lamda}

        mergedTFIUnion_1 = getTFIUnion(TFI_by_period, [11,12])
        self.assertEqual(len(mergedTFIUnion_1[1]), 6, 'TFI-MERGE-1a')
        self.assertEqual(len(mergedTFIUnion_1[2]), 11, 'TFI-MERGE-1b')

        mergedTFIUnion_2 = getTFIUnion(TFI_by_period, [7, 12])
        self.assertEqual(len(mergedTFIUnion_2[1]), 8, 'TFI-MERGE-2a')
        self.assertEqual(len(mergedTFIUnion_2[3]), 12, 'TFI-MERGE-2b')
        self.assertEqual(len(mergedTFIUnion_2[4]), 1, 'TFI-MERGE-2c')

    def test_periods_included(self):
        self.assertEqual(getPeriodsIncluded(1, 4), [7, 8], 'Periods_boundaries_included')
        self.assertEqual(getPeriodsIncluded(2, 3), [13, 18], 'Periods_boundaries_included-2')
        self.assertEqual(getPeriodsIncluded(2, 4), [19, 24], 'Periods_boundaries_included-3')
        self.assertEqual(getPeriodsIncluded(3, 1), [1, 24], 'Periods_boundaries_included-4')



    def test_get_MCP_Between_Boundaries(self):
        faps = [2, 4, 7, 9, 12]
        faps2 = [2, 5, 6]
        self.assertEqual(getMCPOfItemsetBetweenBoundaries(faps, [13, 18]), [13, 18], 'MCP_between_boundaries')
        self.assertEqual(getMCPOfItemsetBetweenBoundaries(faps, [11, 18]), [12, 18], 'MCP_between_boundaries_2')
        self.assertEqual(getMCPOfItemsetBetweenBoundaries(faps2, [1, 5]), None, 'MCP_between_boundaries_3')
        self.assertEqual(getMCPOfItemsetBetweenBoundaries(faps2, [1, 8]), [6, 8], 'MCP_between_boundaries_4')

    #TODO: TESTS getItemsetRelativeSupportLowerBound

    def test_HTFI(self):
        parser = Parser()
        database = parser.parse("Datasets/sales_formatted_for_test.csv", 'single', None, True)
        rules_by_pg = HTAR_BY_PG(database, 0.4, 0.6, 0.4)
        # for pg in rules_by_pg.keys():
        #     print(pg + ": " + str(len(rules_by_pg[pg])) +" rules found")
        self.assertEqual(len(rules_by_pg.keys()), 8, 'HTFI-succeed-1')

    #WIP: HTFI with real data
    # def test_HTFI_with_real_data(self):
    #     parser = Parser()
    #     database = parser.parse("Datasets/sales_formatted.csv", 'single', None, True)
    #     rules_by_pg = HTAR_BY_PG(database, 0.002, 0.4, 0.002)
    #     for pg in rules_by_pg.keys():
    #         print(pg + ": " + str(len(rules_by_pg[pg])) +" rules found")


