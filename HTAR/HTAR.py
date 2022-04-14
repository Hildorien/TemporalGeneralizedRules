import itertools
import time

from HTAR.HTAR_utility import getPeriodsIncluded, getTFIUnion
from rule_generator import rule_generation
from utility import generateCanidadtesOfSizeK, stringifyPg, hash_candidate, maximal_time_period_interval, \
    append_tids_for_HTAR, calculate_tid_intersections_HTAR_with_boundaries
import multiprocessing

import multiprocessing.pool

class NoDaemonProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass

class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess


# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class NestablePool(multiprocessing.pool.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(NestablePool, self).__init__(*args, **kwargs)

def findIndividualTFI(database, pj, lam, parallel_count_in_k_level=False, debugging = False, rsm= 2, hong = False):
    # Returns every Temporal Frequent Itemsets (of every length) TFI_j, for the j-th time period p_j of llevel-period.
    ptt_entry = database.getPTTValueFromLeafLevelGranule(pj)
    TFI_j = {}
    r = 1
    allItems = sorted(ptt_entry['itemsSet'])
    C_j = allItems
    C_j.sort()
    TFI_r = list()
    frequent_dictionary = {}
    support_dictionary = {}
    totalCandidates = 0

    items_dic = database.items_dic
    matrix_data_by_item = database.matrix_data_by_item
    level_0_periods_included_in_pg = getPeriodsIncluded(0, pj, hong)
    total_transactions = database.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included_in_pg)
    starters_tid = database.getPTTPeriodTIDBoundaryTuples()
    tidLimits = maximal_time_period_interval(starters_tid, level_0_periods_included_in_pg[0] - 1,
                                             level_0_periods_included_in_pg[1] - 1)

    if not parallel_count_in_k_level:
        return database.findIndividualTFIForParalel(pj, allItems, total_transactions, tidLimits, lam, parallel_count_in_k_level, debugging, False, rsm)

    else:
        while (r == 1 or frequent_dictionary[r - 1] != []) and r <= len(allItems):
            frequent_dictionary[r] = []
            old_C_J = C_j
            C_j = generateCanidadtesOfSizeK(r, old_C_J, frequent_dictionary)
            if debugging:
                totalCandidates += len(C_j)

            #Paralizing support calculation for each itemset which len = r
            pool = multiprocessing.Pool(multiprocessing.cpu_count())
            list_to_parallel = [(x, append_tids_for_HTAR(x, items_dic, matrix_data_by_item), tidLimits, rsm) for x in C_j]
            results = pool.starmap(calculate_tid_intersections_HTAR_with_boundaries, list_to_parallel)
            for a_result in results:
                candidate_support = a_result[1] / total_transactions
                if candidate_support >= lam:
                    TFI_r.append(tuple(a_result[0]))
                    support_dictionary[hash_candidate(a_result[0])] = candidate_support
                    frequent_dictionary[r].append(a_result[0])
            pool.close()
            pool.join()

            if len(TFI_r) > 0:
                TFI_j[r] = set(TFI_r)
            TFI_r = list()
            r += 1

        return {"TFI": TFI_j, "supportDict": support_dictionary}


def getGranulesFrequentsAndSupports(database, min_rsup,  lam, paralelExcecution = False, paralelExcecutionOnK = False,  debugging = False, debuggingK = False, HTG = [24, 12, 4, 1], rsm= 2):
    TFI_by_period_in_l_0 = {}
    support_dictionary_by_pg = {}
    HTFI_by_pg = {}

    usable_cpu_count = multiprocessing.cpu_count()
    paralel_processing_on_k = 1
    if paralelExcecutionOnK:
        paralel_processing_on_k = usable_cpu_count

    starters_tid = database.getPTTPeriodTIDBoundaryTuples()
    hong = False
    if HTG == [10, 5, 1]:
        hong = True

    list_to_parallel = []

    for pi_index in range(HTG[0]):
        pi = pi_index+1
        allItems = sorted(database.getPTTValueFromLeafLevelGranule(pi)['itemsSet'])
        level_0_periods_included_in_pg = [pi, pi]
        total_transactions = database.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included_in_pg)
        tid_limits = maximal_time_period_interval(starters_tid, pi_index, pi_index)
        list_to_parallel.append((pi, allItems, total_transactions, tid_limits, lam, paralel_processing_on_k, debugging, debuggingK, rsm))

    if paralelExcecution:
        pool = NestablePool(usable_cpu_count)
        results = pool.starmap(database.findIndividualTFIForParalel, list_to_parallel, chunksize=usable_cpu_count)
        for a_result in results:
            pi = a_result["pi"]
            if len(a_result["TFI"]) > 0:
                TFI_by_period_in_l_0[pi + 1] = a_result["TFI"]
                pgStringKey = stringifyPg(0, pi + 1)
                support_dictionary_by_pg[pgStringKey] = a_result["supportDict"]
                HTFI_by_pg[pgStringKey] = TFI_by_period_in_l_0[pi + 1]
        pool.close()
        pool.join()
    else:
        for param in list_to_parallel:
            #start = time.time()
            (pi, ai, totalTrans, tidlimits, lam, pOnK, deb, debk, rsm) = param
            individualTFI = database.findIndividualTFIForParalel(pi, ai, totalTrans, tidlimits, lam, pOnK, deb, debk, rsm)
            pi = individualTFI["pi"]

            if len(individualTFI["TFI"]) > 0:
                TFI_by_period_in_l_0[pi + 1] = individualTFI["TFI"]
                pgStringKey = stringifyPg(0, pi + 1)
                support_dictionary_by_pg[pgStringKey] = individualTFI["supportDict"]
                HTFI_by_pg[pgStringKey] = TFI_by_period_in_l_0[pi + 1]

            # end = time.time()
            # if debugging:
            #     totalFrequent = 0
            #     if pi + 1 in TFI_by_period_in_l_0:
            #         for k in TFI_by_period_in_l_0[pi + 1]:
            #             totalFrequent += len(TFI_by_period_in_l_0[pi + 1][k])
            #         print(str(pi) + ' leaf-TFI took ' + (str(end - start) + ' seconds'))
            #         print('(' + str(totalFrequent) + ' frecuent itemsets )')
            #         print('-------------')

    # PHASE 2: FIND ALL HIERARCHICAL TEMPORAL FREQUENT ITEMSETS}
    HTFI = {}
    for l_level in range(len(HTG)):
        if l_level != 0:
            list_to_parallel = []
            for period in range(HTG[l_level]):
                pgStringKey = stringifyPg(l_level, period + 1)
                support_dictionary_by_pg[pgStringKey] = {}
                level_0_periods_included = getPeriodsIncluded(l_level, period + 1)
                total_transactions = database.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included)
                tid_limits = maximal_time_period_interval(starters_tid, level_0_periods_included[0]-1, level_0_periods_included[1]-1)

                # Get a single merged TFI of 0-level periods involved
                possible_itemsets_in_pg = getTFIUnion(TFI_by_period_in_l_0, level_0_periods_included)

                list_to_parallel.append((possible_itemsets_in_pg, total_transactions, tid_limits, paralel_processing_on_k, rsm, min_rsup, pgStringKey, debugging))

            if paralelExcecution:
                pool = NestablePool(usable_cpu_count)
                results = pool.starmap(database.getIndividualTFIForNotLeafsGranules, list_to_parallel)
                for a_result in results:
                    pgKey, HTFI_res, sup_dict = a_result
                    if len(HTFI_res) > 0:
                        HTFI_by_pg[pgKey] = HTFI_res
                        support_dictionary_by_pg[pgKey] = sup_dict
                    else:
                        del support_dictionary_by_pg[pgKey]
            else:
                for param in list_to_parallel:
                    pis, ttx, tidlts, pOnK, rsm, min_rsup, pgk, dbg = param
                    pgKey, HTFI_res, sup_dict = database.getIndividualTFIForNotLeafsGranules(pis, ttx, tidlts, pOnK, rsm,
                     min_rsup, pgk, debugging)
                    if len(HTFI_res) > 0:
                        HTFI_by_pg[pgKey] = HTFI_res
                        support_dictionary_by_pg[pgKey] = sup_dict
                    else:
                        del support_dictionary_by_pg[pgKey]
                # if debugging:
                #     end = time.time()
                #     if pgStringKey in HTFI_by_pg:
                #         totalFrequent = 0
                #         for k in HTFI_by_pg[pgStringKey]:
                #             totalFrequent += len(HTFI_by_pg[pgStringKey][k])
                #
                #         print(pgStringKey + ' took ' + (str(end - start) + ' seconds'))
                #         print('(' + str(totalCandidates) + ' candidates itemsets )')
                #         print('(' + str(totalFrequent) + ' frecuent itemsets )')
                #         print('-------------')
                #     else:
                #         print('(no frecuent itemsets)')
                #         print('-------------')


    return {'HTFI_by_pg': HTFI_by_pg, 'support_dictionary_by_pg': support_dictionary_by_pg}



def HTAR_BY_PG(database, min_rsup, min_rconf, lam, paralelExecution = False, paralelExcecutionOnK= False, debugging = False, debuggingK= False, HTG = [24, 12, 4, 1], rsm= 2):
    """
    :param database:
    :return: a set of AssociationRules
    """
    # PHASE 1: FIND TEMPORAL FREQUENT ITEMSETS (l_level = 0) AND PHASE 2: FIND ALL HIERARCHICAL TEMPORAL FREQUENT ITEMSETS

    phase_1_and_2_res = getGranulesFrequentsAndSupports(database, min_rsup, lam, paralelExecution, paralelExcecutionOnK, debugging, debuggingK, HTG, rsm)

    # PHASE 3: FIND ALL HIERARCHICAL TEMPORAL ASSOCIATION RULES
    HTFS_by_pg = {}
    pgKeys = phase_1_and_2_res['HTFI_by_pg']
    suppDictionaryByPg = phase_1_and_2_res['support_dictionary_by_pg']
    totalRules = 0
    for pg in pgKeys.keys():
        pg_rules = rule_generation(pgKeys[pg], suppDictionaryByPg[pg], min_rconf)
        totalRules += len(pg_rules)
        if debugging:
            print(pg + " RULES")
            print(str(len(pg_rules)))
            print("----")
        if len(pg_rules) > 0:
            HTFS_by_pg[pg] = pg_rules

    if debugging:
        print("Total amount of HTAR rules: " + str(totalRules))
    return HTFS_by_pg
