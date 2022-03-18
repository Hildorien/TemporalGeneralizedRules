import time
import unittest

from Apriori.apriori import findIndividualTFI, HTAR_BY_PG, apriori
from DataStructures.PTT import PTT
from DataStructures.Parser import Parser
from utility import getPeriodStampFromTimestamp, getTFIUnion, getPeriodsIncluded


class HTARTests(unittest.TestCase):

    def setUp(self):
        self.t1 = 631234984  # 1/1/1990 - [1,1,1]
        self.t2 = 503276584  # 12/12/1985 - [23,12,4]
        self.t3 = 1118962984  # 16/6/2005 - [12, 6, 2]
        self.t4 = 1442876584  # 21/9/2015 - [18, 9, 3]
        self.t5 = 1117811573  # 3/6/2005 - [11, 6, 2]
        self.t6 = 486655073  # 3/6/1985 - [11, 6 , 2]

        def printRulesDebugging(database, rules_by_pg, apriori_rules):
            print("/////////////////////////////////////////")
            print("PG-RULES")
            print("/////////////////////////////////////////")

            for pg in rules_by_pg:
                print(pg)
                print("-----------")
                for r in rules_by_pg[pg]:
                    print(database.printAssociationRule(r))
            print("/////////////////////////////////////////")
            print("APRIORI RULES")
            print("/////////////////////////////////////////")
            for r in apriori_rules:
                print(database.printAssociationRule(r))

        self.printRulesDebugging = printRulesDebugging

        def testCorrectnessAndCompletness(rules_by_pg, apriori_rules):
            leafRulesHashes = set()
            upperLevelRules = set()
            aprioriRulesHashes = set()
            for pg in rules_by_pg:
                level = pg.split('-')[0]
                if level == '0':
                    leafRulesHashes = set.union(leafRulesHashes,
                                                set(map(lambda rule: rule.identifyRuleOnlyByLhsAndRhs(),
                                                        rules_by_pg[pg])))
                elif pg == '3-1':
                    upperLevelRules = set.union(upperLevelRules,
                                                set(map(lambda rule: rule.identifyRuleOnlyByLhsAndRhs(),
                                                        rules_by_pg[pg])))

            for apriori_rule in apriori_rules:
                ruleHash = apriori_rule.identifyRuleOnlyByLhsAndRhs()
                aprioriRulesHashes.add(ruleHash)

            if not apriori_rules:
                self.assertEqual(('3-1' not in rules_by_pg) or (rules_by_pg['3-1'] == apriori_rules), True,
                                 'HTFI-complete-upper-level')
            else:
                self.assertEqual(len(rules_by_pg['3-1']), len(apriori_rules), 'HTFI-complete-upper-level')
                self.assertEqual(upperLevelRules, aprioriRulesHashes, 'HTFI-complete-upper-level')
                self.assertEqual(aprioriRulesHashes.issubset(leafRulesHashes), True,
                                 'Apriori-rules-contained-in-HTFI-leafRules')

        self.testCorrectnessAndCompletness = testCorrectnessAndCompletness

    def test_get_period_stamp_from_timestamp(self):
        self.assertEqual(getPeriodStampFromTimestamp(self.t1), [1, 1, 1, 1], 'Periodstamp 1')
        self.assertEqual(getPeriodStampFromTimestamp(self.t2), [23, 12, 4, 1], 'Periodstamp 2')
        self.assertEqual(getPeriodStampFromTimestamp(self.t3), [12, 6, 2, 1], 'Periodstamp 3')
        self.assertEqual(getPeriodStampFromTimestamp(self.t4), [18, 9, 3, 1], 'Periodstamp 4')

    def test_parse_horizontal_database(self):
        parser = Parser()
        horizontal_db = parser.parse_single_file_for_horizontal_database('Datasets/sales_formatted.csv',
                                                                         'Taxonomies/salesfact_taxonomy_single.csv')
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
        # T1 = ([C,E,G,K], [8,4,2])
        # T2 = ([D,F,G], [8,4,2])
        # T3 = ([T,C]), [8,4,2])
        # T4 = ([L,F,G,D], [11,6,2])
        # T5 = ([C,D,A,F], [12,6,2])
        # T6 = ([A,J,K,C],  [18,9,3])
        # T7 = ([A], [18,9,3])
        # T8 = ([A,B], [23,12,4])

        # ItemsDic = {
        # 0: 'A',
        # 1: 'B',
        # 2: 'C',
        # 3: 'D',
        # 4: 'E',
        # 5: 'F',
        # 6: 'G',
        # 7: 'J',
        # 8: 'K',
        # 9:'L'
        # 10:'T',
        # }
        parser = Parser()
        database = parser.parse("Datasets/sales_formatted_for_test.csv", 'single', None, True)
        self.assertEqual(database.supportOf([0]), 0.5, 'SupportTestVanilla1')
        self.assertEqual(database.supportOf([0,2]), 2/8, 'SupportTestVanilla2')
        self.assertEqual(database.supportOf([0], 0, 18), 1, 'SupportTestWithTimePeriod1')
        self.assertEqual(database.supportOf([3], 0, 8), 1/3, 'SupportTestWithTimePeriod2')
        self.assertEqual(database.supportOf([3], 0, 8), 1/3, 'SupportTestWithTimePeriod3')
        self.assertEqual(database.supportOf([0], 2, 2), 1/5, 'SupportTestWithTimePeriod4')
        self.assertEqual(database.supportOf([3, 5], 2, 2), 3 / 5, 'SupportTestWithTimePeriod5')
        self.assertEqual(database.supportOf([3, 5, 6], 1, 4), 1 / 3, 'SupportTestWithTimePeriod6')
        self.assertEqual(database.supportOf([8], 0, 1), 0, 'SupportTestWithTimePeriod7')

        #Now assert items =============================================================
        self.assertEqual(len(database.getItemsByDepthAndPeriod(0,1)), 0, 'Items_in_time_period_test_1')
        items_in_0_8 = database.getItemsByDepthAndPeriod(0,8)
        self.assertEqual(len(items_in_0_8), 7, 'Items_in_time_period_test_2a')
        self.assertEqual(5 in items_in_0_8, True, 'Items_in_time_period_test_2b')
        self.assertEqual(7 in items_in_0_8, False, 'Items_in_time_period_test_2c')
        self.assertEqual(len(database.getItemsByDepthAndPeriod(2, 2)), 9, 'Items_in_time_period_test_3')
        self.assertEqual(len(database.getItemsByDepthAndPeriod(1, 2)), 0, 'Items_in_time_period_test_3b')
        self.assertEqual(len(database.getItemsByDepthAndPeriod(1, 6)), 6, 'Items_in_time_period_test_3c')

    def test_TFI(self):
        parser = Parser()
        database = parser.parse("Datasets/sales_formatted_for_test.csv", 'single', None, True)
        tfi_0_8 = findIndividualTFI(database, 0, 8, 0.55)["TFI"]
        tfi_0_8_lower_lamda = findIndividualTFI(database, 0, 8, 0.02)["TFI"]

        tfi_2_2 = findIndividualTFI(database, 2, 2, 0.49)["TFI"]
        tfi_0_5 = findIndividualTFI(database, 0, 5, 0.02)["TFI"]
        self.assertEqual(tfi_0_8[1], {(6,), (2,)}, 'TFI-1')
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
        self.assertEqual(len(mergedTFIUnion_2[1]), 9, 'TFI-MERGE-2a')
        self.assertEqual(len(mergedTFIUnion_2[3]), 12, 'TFI-MERGE-2b')
        self.assertEqual(len(mergedTFIUnion_2[4]), 3, 'TFI-MERGE-2c')

    def test_periods_included(self):
        self.assertEqual(getPeriodsIncluded(1, 4), [7, 8], 'Periods_boundaries_included')
        self.assertEqual(getPeriodsIncluded(2, 3), [13, 18], 'Periods_boundaries_included-2')
        self.assertEqual(getPeriodsIncluded(2, 4), [19, 24], 'Periods_boundaries_included-3')
        self.assertEqual(getPeriodsIncluded(3, 1), [1, 24], 'Periods_boundaries_included-4')


    # Deprecated since MCP won't be used
    # def test_get_MCP_Between_Boundaries(self):
    #     faps = [2, 4, 7, 9, 12]
    #     faps2 = [2, 5, 6]
    #     self.assertEqual(getMCPOfItemsetBetweenBoundaries(faps, [13, 18]), [13, 18], 'MCP_between_boundaries')
    #     self.assertEqual(getMCPOfItemsetBetweenBoundaries(faps, [11, 18]), [12, 18], 'MCP_between_boundaries_2')
    #     self.assertEqual(getMCPOfItemsetBetweenBoundaries(faps2, [1, 5]), None, 'MCP_between_boundaries_3')
    #     self.assertEqual(getMCPOfItemsetBetweenBoundaries(faps2, [1, 8]), [6, 8], 'MCP_between_boundaries_4')


    def test_TID_and_faps_sales_for_Test(self):
        # {0: 'A', 1: 'B', 2: 'C', 3: 'D', 4: 'E', 5: 'F', 6: 'G', 7: 'J', 8: 'K', 9: 'L'}
        database = Parser().parse("Datasets/sales_formatted_for_test.csv", 'single', None, True)
        self.assertEqual(database.getItemTids(0), [4, 5, 6, 7], 'test_TID_A')
        self.assertEqual(database.getItemTids(1), [7], 'test_TID_B')
        self.assertEqual(database.getItemTids(2), [0, 2, 4, 5], 'test_TID_C')
        self.assertEqual(database.getItemTids(3), [1, 3, 4], 'test_TID_D')
        self.assertEqual(database.getItemTids(4), [0], 'test_TID_E')
        self.assertEqual(database.getItemTids(5), [1, 3, 4], 'test_TID_F')
        self.assertEqual(database.getItemTids(6), [0, 1, 3], 'test_TID_G')
        self.assertEqual(database.getItemTids(7), [5], 'test_TID_J')
        self.assertEqual(database.getItemTids(8), [0, 5], 'test_TID_K')
        self.assertEqual(database.getItemTids(9), [3], 'test_TID_L')

    def test_HTAR_generic_data_correctness_and_completeness(self):
        database = Parser().parse("Datasets/sales_formatted_for_test.csv", 'single', None, True)
        #3 levels of complexity
        sups = [0.4, 0.35, 0.002]
        confs= [0.6, 0.6, 0,4]

        for i in range(0, 3):
            rules_by_pg = HTAR_BY_PG(database, sups[i], confs[i], sups[i])
            apriori_rules = apriori(database, sups[i], confs[i])
            self.testCorrectnessAndCompletness(rules_by_pg, apriori_rules)

            #if i == 1:
             #   self.testCorrectnessAndCompletness(rules_by_pg, apriori_rules)
             #   self.printRulesDebugging(database, rules_by_pg, apriori_rules)
             #    print("------------------------------------")



    # def test_HTAR_foodmart_data_1997_correctness_and_completness(self):
    #     database = Parser().parse("Datasets/sales_formatted_1997.csv", 'single', None, True)
    #     print('---')
    #     start = time.time()
    #     apriori_rules = apriori(database, 0.5, 0.5)
    #     end = time.time()
    #     print('Apriori Took ' + (str(end - start) + ' seconds'))
    #
    #     start = time.time()
    #     rules_by_pg = HTAR_BY_PG(database, 0.5, 0.5, 0.5)
    #     end = time.time()
    #     print('HTAR Took ' + (str(end - start) + ' seconds'))
    #
    #     print(len(rules_by_pg))
    #     print(len(apriori_rules))
    #     self.testCorrectnessAndCompletness(rules_by_pg, apriori_rules)

    # def test_HTAR_foodmart_data_1998_correctness_and_completness(self):
    #     database = Parser().parse("Datasets/sales_formatted_1997.csv", 'single', None, True)
    #     rules_by_pg = HTAR_BY_PG(database, 0.006, 0.01, 0.006)
    #     apriori_rules = apriori(database,0.006, 0.01)
    #     print(len(rules_by_pg))
    #     print(len(apriori_rules))
    #     self.testCorrectnessAndCompletness(rules_by_pg, apriori_rules)


