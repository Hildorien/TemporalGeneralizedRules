from HTAR.HTAR_utility import getPeriodsIncluded
from utility import findOrderedIntersection, findOrderedIntersectionBetweenBoundaries, maximal_time_period_interval, \
    getFilteredTIDSThatBelongToPeriod


class Database:
    timestamp_mapping = {}  # {transactionIndex(int) : timestamp(long)}
    items_dic = {}  # {itemIndex(int) : itemName(string)}
    row_count = 0

    ptt = None

    # {item : {tids:int[], fap:HTG}}
    # tids: Array of transaction id, in order, where the item i appears
    # fap: First Appereance Periods from the transaction where the item i was first discovered
    matrix_data_by_item = {}

    # { item_id : [ancestor_id] }
    taxonomy = {}

    def __init__(self, matrix_data_by_item,
                 timestamp_dict, items_dict, row_count, taxonomy_dict, ptt=None):
        self.matrix_data_by_item = matrix_data_by_item
        self.timestamp_mapping = timestamp_dict
        self.items_dic = items_dict
        self.row_count = row_count
        self.taxonomy = taxonomy_dict
        self.ptt = ptt

    #Methods for debugging purposes
    def getItemDic(self):
        return self.items_dic

    def getItemTids(self, item_number):
        item = self.items_dic[item_number]
        if item in self.matrix_data_by_item:
            return self.matrix_data_by_item[item]['tids']
        else:
            return []
    #Methods for debugging purposes

    def transaction_ids_intersection(self, itemset, lb=None, ub=None):
        final_intersection = None
        for itemColumnIndex in itemset:
            all_tids_indexes = self.getItemTids(itemColumnIndex)
            item_valid_indexes = getFilteredTIDSThatBelongToPeriod(all_tids_indexes, lb, ub)
            if final_intersection is None:
                final_intersection = item_valid_indexes
            elif len(final_intersection) == 0:
                return []
            elif lb is not None and ub is not None:
                final_intersection = findOrderedIntersectionBetweenBoundaries(final_intersection, item_valid_indexes, lb, ub)
            else:
                final_intersection = findOrderedIntersection(final_intersection, item_valid_indexes)

        return final_intersection

    def supportOf(self, itemset, l_level=None, period=None):
        """
        :param itemset: set/list of integers
        :param l_level: L_level of HTG of the period
        :param period: time-period within the l_level
        :return: float
        """

        if l_level is not None and period is not None:
            # # Now we have the intersactions in common, we need to filter those who aren't in the selected period and level
            #
            # # Since final_intersection is ordered, essentialy we have 3 phases:
            # # 1: Look for the first transaction within the time period. Discard all tids until then
            # # 2: Once found, append every transaction to final array.
            # # 3: When a new transaction is read and isn't within the period, stop iterating and return what you got until then.
            # period_reach_phase = 1
            # filtered_by_period_tids = 0
            # for tid in final_intersection:
            #     transactionHTG = getPeriodStampFromTimestamp(self.timestamp_mapping[tid])
            #     if transactionHTG[l_level] == period:
            #         period_reach_phase = 2
            #         filtered_by_period_tids += 1
            #     elif period_reach_phase == 2:
            #         break
            # #ptt_j = self.ptt.getPTTValueFromLeafLevelGranule(l_level, period)['totalTransactions']
            # ptt_j = self.ptt.getTotalPTTSumWithinPeriodsInLevel0(boundaries)
            # if ptt_j == 0:
            #     return 0
            # else:
            #     return filtered_by_period_tids / ptt_j


            level_0_periods_included = getPeriodsIncluded(l_level, period)
            return self.getItemsetRelativeSupport(itemset, level_0_periods_included)
        else:
            final_intersection = self.transaction_ids_intersection(itemset)
            return len(final_intersection) / self.row_count

    def getItemsByDepthAndPeriod(self, l_level, period):
        return self.ptt.getEveryItemInPTTInPG(l_level, period)

    def getPTTValueFromLeafLevelGranule(self, pi):
        return self.ptt.getPTTValueFromLeafLevelGranule(pi)

    def getTotalPTTSumWithinPeriodsInLevel0(self, boundaries):
        return self.ptt.getTotalPTTSumWithinPeriodsInLevel0(boundaries)

    def getPTTPeriodTIDBoundaryTuples(self):
        return self.ptt.getPTTPeriodTIDBoundaryTuples()

    # def getItemsetRelativeSupports(self, itemset, level_0_periods_included_in_pg):
    #     """
    #     :param itemset: set/list of integers
    #     :param level_0_periods_included_in_pg: Two-item list which refer to the range of periods of level 0.
    #     [lowerBoundary, upperBoundary]
    #     :return: {"rslb": float, "rsup": float} or None
    #     """
    #
    #     faps_period_of_level_0 = list(map(lambda x: self.matrix_data_by_item[self.items_dic[x]]['fap'][0], itemset))
    #     maximum_common_period_with_boundaries = getMCPOfItemsetBetweenBoundaries(faps_period_of_level_0, level_0_periods_included_in_pg)
    #     if maximum_common_period_with_boundaries is None:
    #         return None
    #     else:
    #         C_total_X_actual = 0
    #         transaction_id_intersection = self.transaction_ids_intersection(itemset)
    #         for tid in transaction_id_intersection:
    #             transaction_period_in_level_0 = getPeriodStampFromTimestamp(self.timestamp_mapping[tid])[0]
    #             if maximum_common_period_with_boundaries[0] <= transaction_period_in_level_0 <= maximum_common_period_with_boundaries[1]:
    #                 C_total_X_actual += 1
    #
    #         PTT_total_sum_with_boundaries_and_MCP = self.getTotalPTTSumWithinPeriodsInLevel0(maximum_common_period_with_boundaries)
    #         PTT_total_sum_with_boundaries = self.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included_in_pg)
    #
    #         relative_support_lower_bound = C_total_X_actual/PTT_total_sum_with_boundaries_and_MCP
    #         relative_support = C_total_X_actual/PTT_total_sum_with_boundaries
    #
    #         return {"rslb": relative_support_lower_bound, "rsup": relative_support}


    def getItemsetRelativeSupport(self, itemset, level_0_periods_included_in_pg):
        """
        :param itemset: set/list of integers
        :param level_0_periods_included_in_pg: Two-item list which refer to the range of periods of level 0.
        :return: float
        """
        starters_tid = self.getPTTPeriodTIDBoundaryTuples()

        tidLimits = maximal_time_period_interval(starters_tid, level_0_periods_included_in_pg[0]-1, level_0_periods_included_in_pg[1]-1)
        lb = tidLimits[0]
        ub = tidLimits[1]
        transaction_id_intersection = len(self.transaction_ids_intersection(itemset, lb, ub))

        # C_total_X_actual = 0
        # for tid in transaction_id_intersection:
        #     transaction_period_in_level_0 = getPeriodStampFromTimestamp(self.timestamp_mapping[tid])[0]
        #     if level_0_periods_included_in_pg[0] <= transaction_period_in_level_0 <= level_0_periods_included_in_pg[1]:
        #        C_total_X_actual += 1firstTID

        PTT_total_sum_with_boundaries = self.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included_in_pg)
        if PTT_total_sum_with_boundaries != 0:
            return transaction_id_intersection/PTT_total_sum_with_boundaries
        else:
            return 0

    def confidenceOf(self, lhs, rhs):
        """
        :param lhs: a list of items
        :param rhs: a list of one item
        :return: float
        """
        return self.supportOf(lhs) / self.supportOf(rhs)

    def printAssociationRule(self, association_rule):
        consequent_name = self.items_dic[association_rule.rhs[0]]
        antecedent_names = ','.join(list(map(lambda x: self.items_dic[x], association_rule.lhs)))
        return antecedent_names + " => " + consequent_name + " support: " + str(
            association_rule.support) + " confidence: " + str(association_rule.confidence)
