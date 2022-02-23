from DataStructures.PTT import PTT
from utility import findOrderedIntersection, getPeriodStampFromTimestamp, allSubsetofSizeKMinus1


class Database:
    timestamp_mapping = {}  # {transactionIndex(int) : timestamp(long)}
    items_dic = {}  # {itemIndex(int) : itemName(string)}
    row_count = 0

    ptt = None

    # {item : {tids:int[], fap:HTG}}
    # tids: Array of transaction id, in order, where the item i appears
    # fap: First Appereance Periods from the transaction where the item i was first discovered
    matrix_data_by_item = {}

    # { 'product_name' : [ancestor] }
    taxonomy = {}

    def __init__(self, matrix_data_by_item,
                 timestamp_dict, items_dict, row_count, taxonomy_dict, ptt=None):
        self.matrix_data_by_item = matrix_data_by_item
        self.timestamp_mapping = timestamp_dict
        self.items_dic = items_dict
        self.row_count = row_count
        self.taxonomy = taxonomy_dict
        self.ptt = ptt

    def supportOf(self, itemset, l_level=None, period=None):
        """
        :param itemset: set/list of integers
        :param l_level: L_level of HTG of the period
        :param period: time-period within the l_level
        :return: float
        """
        final_intersection = []
        for itemColumnIndex in itemset:
            item_valid_indexes = self.matrix_data_by_item[self.items_dic[itemColumnIndex]]['tids']

            if len(final_intersection) == 0:
                final_intersection = item_valid_indexes
            else:
                final_intersection = findOrderedIntersection(final_intersection, item_valid_indexes)

        if l_level is not None and period is not None:
            # Now we have the intersactions in common, we need to filter those who aren't in the selected period and level

            # Since final_intersection is ordered, essentialy we have 3 phases:
            # 1: Look for the first transaction within the time period. Discard all tids until then
            # 2: Once found, append every transaction to final array.
            # 3: When a new transaction is read and isn't within the period, stop iterating and return what you got until then.
            period_reach_phase = 1
            filtered_by_period_tids = 0
            for tid in final_intersection:
                transactionHTG = getPeriodStampFromTimestamp(self.timestamp_mapping[tid])
                if transactionHTG[l_level] == period:
                    period_reach_phase = 2
                    filtered_by_period_tids += 1
                elif period_reach_phase == 2:
                    break
            ptt_j = self.ptt.getPTTValueFromLlevelAndPeriod(l_level, period)['totalTransactions']

            if ptt_j == 0:
                return 0
            else:
                return filtered_by_period_tids/ptt_j
        else:
            return len(final_intersection) / self.row_count

    def getItemsByDepthAndPeriod(self, l_level, period):
        return self.ptt.getPTTValueFromLlevelAndPeriod(l_level, period)['itemsSet']

    # WIP
    # def findIndividualTFI(self, l_level, pj, lam):
    #     # Returns every Temporal Frequent Itemsets (of every length) TFI_j, for the j-th time period p_j of llevel-period.
    #     ptt_entry = self.ptt.getPTTValueFromLlevelAndPeriod(l_level, pj)
    #     TFI_j = set()
    #     totalTransactions = ptt_entry['totalTransactions']
    #     r = 1
    #     C_j = list(ptt_entry['itemsSet'])
    #     C_j.sort()
    #     TFI_r = set()
    #     while TFI_r > 0 or r == 1:
    #         for k_itemset in C_j:
    #             if self.supportOf(k_itemset, l_level, pj) > lam:
    #                 TFI_r.add(k_itemset)
    #         TFI_j.add(TFI_r)
    #        # C_j = allSubsetofSizeKMinus1(a_candidate_of_size_k, k - 1)  # a_candidate_of_size_k is a list
    #
    #     return TFI_j


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
