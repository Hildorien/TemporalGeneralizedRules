import multiprocessing
import time
from HTAR.HTAR_utility import getPeriodsIncluded, getPeriodStampFromTimestamp
from utility import findOrderedIntersection, findOrderedIntersectionBetweenBoundaries, maximal_time_period_interval, \
    getFilteredTIDSThatBelongToPeriod, create_ancestor_set, append_tids_for_HTAR, \
    calculate_tid_intersections_HTAR_with_boundaries, generateCanidadtesOfSizeK, hash_candidate


class Database:
    timestamp_mapping = {}  # {transactionIndex(int) : timestamp(long)}
    items_dic = {}  # {itemIndex(int) : itemName(string)}
    tx_count = 0

    ptt = None

    # {item : {tids:int[]}}
    # tids: Array of transaction id, in order, where the item i appears
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
            #For Testing only. Try to use calculate_tid_intersections_HTAR_with_boundaries and precalculate other parameters instead.
            level_0_periods_included_in_pg = getPeriodsIncluded(l_level, period, hong)
            starters_tid = self.getPTTPeriodTIDBoundaryTuples()
            tidLimits = maximal_time_period_interval(starters_tid, level_0_periods_included_in_pg[0] - 1,
                                                     level_0_periods_included_in_pg[1] - 1)

            candidates_tid = append_tids_for_HTAR(itemset,
                                 self.items_dic,
                                 self.matrix_data_by_item)
            transaction_id_intersection = calculate_tid_intersections_HTAR_with_boundaries(itemset, candidates_tid, tidLimits, relativeSupportCalculationType)[1]
            PTT_total_sum_with_boundaries = self.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included_in_pg)
            if PTT_total_sum_with_boundaries != 0:
                return transaction_id_intersection / PTT_total_sum_with_boundaries
            else:
                return 0
        else:
            final_intersection = self.transaction_ids_intersection(itemset)
            return len(final_intersection) / self.tx_count

    def getItemsByDepthAndPeriod(self, l_level, period, hong=False):
        return self.ptt.getEveryItemInPTTInPG(l_level, period, hong)

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

    def findIndividualTFIForParalel(self, pi, allItems, total_transactions, tid_limits, lam, paralel_processing_on_k = False,
                                    debugging=False, debuggingK = False, rsm=2):
        # Returns every Temporal Frequent Itemsets (of every length) TFI_j, for the j-th time period p_j of llevel-period.
        start = time.time()
        TFI_j = {}
        r = 1
        C_j = allItems
        TFI_r = list()
        frequent_dictionary = {}
        support_dictionary = {}
        totalCandidates = 0

        while (r == 1 or frequent_dictionary[r - 1] != []) and r <= len(allItems):
            frequent_dictionary[r] = []
            old_C_J = C_j
            startj = time.time()

            C_j = generateCanidadtesOfSizeK(r, old_C_J, frequent_dictionary)
            endj = time.time()
            if debuggingK:
                totalCandidates += len(C_j)
                print('Candidates of size ' + str(r) + ' is ' + str(len(C_j)))
                print('It lasted '+ str(endj-startj))
                print('Calculating support of each candidate of size ' + str(r))
            totalCandidates += len(C_j)

            if paralel_processing_on_k > 1:
                pool = multiprocessing.Pool(paralel_processing_on_k)
                list_to_parallel = [(x, append_tids_for_HTAR(x, self.items_dic, self.matrix_data_by_item), tid_limits, rsm) for x
                                    in C_j]
                results = pool.starmap(calculate_tid_intersections_HTAR_with_boundaries, list_to_parallel)
                for a_result in results:
                    candidate_support = a_result[1] / total_transactions
                    if candidate_support >= lam:
                        TFI_r.append(tuple(a_result[0]))
                        support_dictionary[hash_candidate(a_result[0])] = candidate_support
                        frequent_dictionary[r].append(a_result[0])
                pool.close()
                pool.join()
            else:
                #startFreq= time.time()
                for k_size_itemset in C_j:
                    a_result = calculate_tid_intersections_HTAR_with_boundaries(k_size_itemset,
                                                                                append_tids_for_HTAR(k_size_itemset,
                                                                                                     self.items_dic,
                                                                                                     self.matrix_data_by_item),
                                                                                tid_limits, rsm)
                    support = a_result[1] / total_transactions
                    if support >= lam:
                        TFI_r.append(tuple(k_size_itemset))
                        support_dictionary[hash_candidate(k_size_itemset)] = support
                        frequent_dictionary[r].append(k_size_itemset)
                # endFreq = time.time()
                # if debuggingK:
                #      print('Took ' + (str(endFreq - startFreq)) + ' seconds in k =' + str(r))
                #      print("**********************")

            if len(TFI_r) > 0:
                TFI_j[r] = set(TFI_r)
            TFI_r = list()
            r += 1

        end = time.time()
        if debugging:
            print(str(pi) + " leaf finished candidate calculation and lasted " + str(end - start))
        return {"pi": pi, "TFI": TFI_j, "supportDict": support_dictionary}

    def getIndividualTFIForNotLeafsGranules(self, possible_itemsets_in_pg, total_transactions,
                                            tid_limits, paralel_processing_on_k, rsm, min_rsup, pgStringKey, debugging):
        HTFI = {}
        start = time.time()
        pg_spupport_dictionary = {}
        if total_transactions == 0:
            return pgStringKey, HTFI, pg_spupport_dictionary
        for k in possible_itemsets_in_pg:
            itemsets_length_k = possible_itemsets_in_pg[k]
            frequent_itemsets_length_k = set()
            if paralel_processing_on_k > 1:
                pool = multiprocessing.Pool(paralel_processing_on_k)
                list_to_parallel = [
                    (x, append_tids_for_HTAR(x, self.items_dic, self.matrix_data_by_item), tid_limits, rsm) for x
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
                                                                                    tid_limits, rsm)
                    itemsetSupport = a_result[1] / total_transactions
                    if itemsetSupport >= min_rsup:
                        frequent_itemsets_length_k.add(itemset)
                        pg_spupport_dictionary[hash_candidate(itemset)] = itemsetSupport

            if len(frequent_itemsets_length_k) > 0:
                HTFI[k] = frequent_itemsets_length_k
        end = time.time()
        if debugging:
            print(pgStringKey + " finished candidate calculation and lasted " + str(end - start))
        return pgStringKey, HTFI, pg_spupport_dictionary