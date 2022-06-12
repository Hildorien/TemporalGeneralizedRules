import multiprocessing
from .HTAR_utility import getPeriodsIncluded
from .utility import findOrderedIntersection, maximal_time_period_interval, \
    getFilteredTIDSThatBelongToPeriod, create_ancestor_set, append_tids_for_HTAR, \
    calculate_tid_intersections_HTAR_with_boundaries, generateCanidadtesOfSizeK, hash_candidate, \
    calculate_tid_intersections_HTAR_with_boundaries_for_paralel, create_minimal_items_dic, \
    append_tids_for_HTAR_for_single_item
from .cumulate_utility import prune_candidates_in_same_family

class Database:
    timestamp_mapping = {}  # {transactionIndex(int) : timestamp(long)}
    items_dic = {}  # {itemIndex(int) : itemName(string)}
    tx_count = 0

    ptt = None

    # {item_id : {tids:int[]}}
    # tids: Array of transaction id, in order, where the item i appears
    matrix_data_by_item = {}

    # { item_id : [ancestor_id] }
    taxonomy = {}
    ancestor_set = set()
    only_ancestors = set()

    def __init__(self, matrix_data_by_item,
                 timestamp_dict, items_dict, row_count, taxonomy_dict, ptt=None, only_ancestors=None):
        self.matrix_data_by_item = matrix_data_by_item
        self.timestamp_mapping = timestamp_dict
        self.items_dic = items_dict
        self.tx_count = row_count
        self.taxonomy = taxonomy_dict
        self.ancestor_set = create_ancestor_set(self.taxonomy)
        self.ptt = ptt
        self.only_ancestors= only_ancestors

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

    def supportOf(self, itemset, l_level=None, period=None):
        """
        :param itemset: set/list of integers
        :param l_level: L_level of HTG of the period
        :param period: time-period within the l_level
        :return: float
        """

        if l_level is not None and period is not None:
            #For Testing only. Try to use calculate_tid_intersections_HTAR_with_boundaries and precalculate other parameters instead.
            level_0_periods_included_in_pg = getPeriodsIncluded(l_level, period)
            starters_tid = self.getPTTPeriodTIDBoundaryTuples()
            tidLimits = maximal_time_period_interval(starters_tid, level_0_periods_included_in_pg[0] - 1,
                                                     level_0_periods_included_in_pg[1] - 1)

            candidates_tid = append_tids_for_HTAR(itemset,
                                 self.items_dic,
                                 self.matrix_data_by_item)
            transaction_id_intersection = calculate_tid_intersections_HTAR_with_boundaries(itemset, candidates_tid, tidLimits)[1]
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

    #HTAR

    def findIndividualTFIForParalel(self, pi, allItems, total_transactions, tid_limits, lam, paralel_processing_on_k = False, generalized_rules=False):
        # Returns every Temporal Frequent Itemsets (of every length) TFI_j, for the j-th time period p_j of llevel-period.
        TFI_j = {}
        r = 1
        C_j = allItems
        TFI_r = list()
        frequent_dictionary = {}
        support_dictionary = {}

        while (r == 1 or frequent_dictionary[r - 1] != []) and r <= len(allItems):
            frequent_dictionary[r] = []
            old_C_J = C_j

            C_j = generateCanidadtesOfSizeK(r, old_C_J, frequent_dictionary)
            if generalized_rules and r == 2:
                C_j = prune_candidates_in_same_family(C_j, self.ancestor_set)  # Cumulate Optimization 3 in original paper

            timeUsagePerk = 0

            if paralel_processing_on_k > 1:
                pool = multiprocessing.Pool(paralel_processing_on_k)
                items_transactions_that_belong_to_perdiod = {}
                for candidate_size_k in C_j:
                    for item in candidate_size_k:
                        if item not in items_transactions_that_belong_to_perdiod:
                            item_filtered_tids = getFilteredTIDSThatBelongToPeriod(append_tids_for_HTAR_for_single_item(item, self.items_dic, self.matrix_data_by_item), tid_limits[0], tid_limits[1])
                            items_transactions_that_belong_to_perdiod[item] = item_filtered_tids

                list_to_parallel = [create_minimal_items_dic(x, items_transactions_that_belong_to_perdiod) for x in C_j]


                results = pool.starmap(calculate_tid_intersections_HTAR_with_boundaries_for_paralel, list_to_parallel)
                for a_result in results:
                    timeUsagePerk += a_result[2]
                    candidate_support = a_result[1] / total_transactions
                    if candidate_support >= lam:
                        TFI_r.append(tuple(a_result[0]))
                        support_dictionary[hash_candidate(a_result[0])] = candidate_support
                        frequent_dictionary[r].append(a_result[0])
                pool.close()
                pool.join()
            else:
                for k_size_itemset in C_j:
                    a_result = calculate_tid_intersections_HTAR_with_boundaries(k_size_itemset, append_tids_for_HTAR(k_size_itemset, self.items_dic, self.matrix_data_by_item), tid_limits)
                    timeUsagePerk += a_result[2]
                    support = a_result[1] / total_transactions
                    if support >= lam:
                        TFI_r.append(tuple(k_size_itemset))
                        support_dictionary[hash_candidate(k_size_itemset)] = support
                        frequent_dictionary[r].append(k_size_itemset)

            if len(TFI_r) > 0:
                TFI_j[r] = set(TFI_r)
            TFI_r = list()
            r += 1

        return {"pi": pi, "TFI": TFI_j, "supportDict": support_dictionary}

    def getIndividualTFIForNotLeafsGranules(self, possible_itemsets_in_pg, total_transactions,
                                            tid_limits, paralel_processing_on_k, min_rsup, pgStringKey):
        HTFI = {}
        pg_spupport_dictionary = {}
        if total_transactions == 0:
            return pgStringKey, HTFI, pg_spupport_dictionary
        for k in possible_itemsets_in_pg:
            itemsets_length_k = possible_itemsets_in_pg[k]
            frequent_itemsets_length_k = set()
            if paralel_processing_on_k > 1:
                pool = multiprocessing.Pool(paralel_processing_on_k)
                list_to_parallel = [
                    (x, append_tids_for_HTAR(x, self.items_dic, self.matrix_data_by_item), tid_limits) for x
                    in itemsets_length_k]
                results = pool.starmap(calculate_tid_intersections_HTAR_with_boundaries, list_to_parallel)
                for a_result in results:
                    itemsetSupport = a_result[1] / total_transactions
                    if itemsetSupport >= min_rsup:
                        frequent_itemsets_length_k.add(a_result[0])
                        pg_spupport_dictionary[hash_candidate(a_result[0])] = itemsetSupport
                pool.close()
                pool.join()
            else:
                for itemset in itemsets_length_k:
                    a_result = calculate_tid_intersections_HTAR_with_boundaries(itemset,
                                                                                    append_tids_for_HTAR(itemset,
                                                                                                         self.items_dic,
                                                                                                         self.matrix_data_by_item),
                                                                                    tid_limits)
                    itemsetSupport = a_result[1] / total_transactions
                    if itemsetSupport >= min_rsup:
                        frequent_itemsets_length_k.add(itemset)
                        pg_spupport_dictionary[hash_candidate(itemset)] = itemsetSupport

            if len(frequent_itemsets_length_k) > 0:
                HTFI[k] = frequent_itemsets_length_k
        return pgStringKey, HTFI, pg_spupport_dictionary

    def is_ancestral_rule(self, rule_itemset):
        for item in rule_itemset:
            if self.items_dic[item] in self.only_ancestors:
                return True
        return False