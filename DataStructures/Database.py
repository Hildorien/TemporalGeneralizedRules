from HTAR.HTAR_utility import getPeriodsIncluded, getPeriodStampFromTimestamp
from utility import findOrderedIntersection, findOrderedIntersectionBetweenBoundaries, maximal_time_period_interval, \
    getFilteredTIDSThatBelongToPeriod, create_ancestor_set


class Database:
    timestamp_mapping = {}  # {transactionIndex(int) : timestamp(long)}
    items_dic = {}  # {itemIndex(int) : itemName(string)}
    tx_count = 0

    ptt = None

    # {item : {tids:int[], fap:HTG}}
    # tids: Array of transaction id, in order, where the item i appears
    # fap: First Appereance Periods from the transaction where the item i was first discovered
    matrix_data_by_item = {}

    # { item_id : [ancestor_id] }
    taxonomy = {}
    ancestor_set = set()

    def __init__(self, matrix_data_by_item,
                 timestamp_dict, items_dict, row_count, taxonomy_dict, ptt=None):
        self.matrix_data_by_item = matrix_data_by_item
        self.timestamp_mapping = timestamp_dict
        self.items_dic = items_dict
        self.tx_count = row_count
        self.taxonomy = taxonomy_dict
        self.ancestor_set = create_ancestor_set(self.taxonomy)
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

    def transaction_ids_intersection(self, itemset, lb=None, ub=None, rsc = 2):
        final_intersection = None
        for itemColumnIndex in itemset:
            item_valid_indexes = self.getItemTids(itemColumnIndex)
            if lb is not None and ub is not None:
                item_valid_indexes = getFilteredTIDSThatBelongToPeriod(item_valid_indexes, lb, ub, rsc)
            if final_intersection is None:
                final_intersection = item_valid_indexes
            elif len(final_intersection) == 0 or len(item_valid_indexes) == 0:
                return []
            else:
                final_intersection = findOrderedIntersection(final_intersection, item_valid_indexes)

        return final_intersection

    def supportOf(self, itemset, l_level=None, period=None, relativeSupportCalculationType = 2, hong = False):
        """
        :param itemset: set/list of integers
        :param l_level: L_level of HTG of the period
        :param period: time-period within the l_level
        :return: float
        """

        if l_level is not None and period is not None:
            level_0_periods_included_in_pg = getPeriodsIncluded(l_level, period)
            starters_tid = self.getPTTPeriodTIDBoundaryTuples()
            tidLimits = maximal_time_period_interval(starters_tid, level_0_periods_included_in_pg[0] - 1,
                                                     level_0_periods_included_in_pg[1] - 1)

            lb = tidLimits[0]
            ub = tidLimits[1]

            transaction_id_intersection = len(self.transaction_ids_intersection(itemset, lb, ub, relativeSupportCalculationType))
            PTT_total_sum_with_boundaries = self.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included_in_pg)
            if PTT_total_sum_with_boundaries != 0:
                return transaction_id_intersection / PTT_total_sum_with_boundaries
            else:
                return 0
        else:
            final_intersection = self.transaction_ids_intersection(itemset)
            return len(final_intersection) / self.tx_count

    def getItemsByDepthAndPeriod(self, l_level, period):
        return self.ptt.getEveryItemInPTTInPG(l_level, period)

    def getPTTValueFromLeafLevelGranule(self, pi):
        return self.ptt.getPTTValueFromLeafLevelGranule(pi)

    def getTotalPTTSumWithinPeriodsInLevel0(self, boundaries):
        return self.ptt.getTotalPTTSumWithinPeriodsInLevel0(boundaries)

    def getPTTPeriodTIDBoundaryTuples(self):
        return self.ptt.getPTTPeriodTIDBoundaryTuples()

    def confidenceOf(self, lhs, rhs):
        """
        :param lhs: a list of items
        :param rhs: a list of one item
        :return: float
        """
        return self.supportOf(lhs) / self.supportOf(rhs)

    def printAssociationRule(self, association_rule):
        rhs = list(association_rule.rhs)
        consequent_name = self.items_dic[rhs[0]]
        antecedent_names = ','.join(list(map(lambda x: self.items_dic[x], association_rule.lhs)))
        return antecedent_names + " => " + consequent_name + " support: " + str(
            association_rule.support) + " confidence: " + str(association_rule.confidence)
