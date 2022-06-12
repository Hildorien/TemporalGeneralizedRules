from .HTAR_utility import getPeriodsIncluded, getTFIUnion
from .rule_generator import rule_generation
from .utility import generateCanidadtesOfSizeK, stringifyPg, hash_candidate, maximal_time_period_interval, \
    append_tids_for_HTAR, calculate_tid_intersections_HTAR_with_boundaries
import multiprocessing

import multiprocessing.pool


def findIndividualTFI(database, pj, lam, parallel_count_in_k_level=False):
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

    items_dic = database.items_dic
    matrix_data_by_item = database.matrix_data_by_item
    level_0_periods_included_in_pg = getPeriodsIncluded(0, pj)
    total_transactions = database.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included_in_pg)
    starters_tid = database.getPTTPeriodTIDBoundaryTuples()
    tidLimits = maximal_time_period_interval(starters_tid, level_0_periods_included_in_pg[0] - 1,
                                             level_0_periods_included_in_pg[1] - 1)

    if not parallel_count_in_k_level:
        return database.findIndividualTFIForParalel(pi=pj, allItems=allItems, total_transactions=total_transactions,
                                                    tid_limits=tidLimits, lam=lam,
                                                    paralel_processing_on_k=parallel_count_in_k_level,
                                                    generalized_rules=False)
    else:
        while (r == 1 or frequent_dictionary[r - 1] != []) and r <= len(allItems):
            frequent_dictionary[r] = []
            old_C_J = C_j
            C_j = generateCanidadtesOfSizeK(r, old_C_J, frequent_dictionary)

            # Paralizing support calculation for each itemset which len = r
            pool = multiprocessing.Pool(multiprocessing.cpu_count())
            list_to_parallel = [(x, append_tids_for_HTAR(x, items_dic, matrix_data_by_item), tidLimits) for x in C_j]
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


def getGranulesFrequentsAndSupports(database, min_rsup, lam, paralelExcecution=False, paralelExcecutionOnK=False,
                                    HTG=[24, 12, 4, 1],
                                    generalized_rules=False):
    """
    :param database: The database you are using
    :param min_rsup: minimum support to consider frequent in inner time granules
    :param lam: minimum support to consider frequent in leaf time granules
    :param paralelExcecution: Enable paralel Excecution on each time granule per level.
    :param paralelExcecutionOnK: Enable paralel excecution on each element of each K when calculating frequent itemsets
    :param HTG: Temporal Time Hierarchy
    :return: {'HTFI_by_pg', 'support_dictionary_by_pg'}

    """
    TFI_by_period_in_l_0 = {}
    support_dictionary_by_pg = {}
    HTFI_by_pg = {}
    usable_cpu_count = multiprocessing.cpu_count() // 3
    paralel_processing_on_k = 1
    if paralelExcecutionOnK:
        paralel_processing_on_k = usable_cpu_count

    starters_tid = database.getPTTPeriodTIDBoundaryTuples()

    list_to_parallel = []

    for pi_index in range(HTG[0]):
        pi = pi_index + 1
        allItems = sorted(database.getPTTValueFromLeafLevelGranule(pi)['itemsSet'])
        level_0_periods_included_in_pg = [pi, pi]
        total_transactions = database.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included_in_pg)
        tid_limits = maximal_time_period_interval(starters_tid, pi_index, pi_index)
        list_to_parallel.append(
            (pi, allItems, total_transactions, tid_limits, lam, paralel_processing_on_k, generalized_rules))

    if paralelExcecution:
        pool = multiprocessing.Pool(usable_cpu_count)
        results = pool.starmap(database.findIndividualTFIForParalel, list_to_parallel)
        for a_result in results:
            pi = a_result["pi"]
            if len(a_result["TFI"]) > 0:
                TFI_by_period_in_l_0[pi + 1] = a_result["TFI"]
                pgStringKey = stringifyPg(0, pi)
                support_dictionary_by_pg[pgStringKey] = a_result["supportDict"]
                HTFI_by_pg[pgStringKey] = TFI_by_period_in_l_0[pi + 1]
        pool.close()
        pool.join()
    else:
        for param in list_to_parallel:
            (pi, ai, totalTrans, tidlimits, lam, pOnK, generalized_rules) = param
            individualTFI = database.findIndividualTFIForParalel(pi, ai, totalTrans, tidlimits, lam, pOnK,
                                                                 generalized_rules)
            pi = individualTFI["pi"]

            if len(individualTFI["TFI"]) > 0:
                TFI_by_period_in_l_0[pi + 1] = individualTFI["TFI"]
                pgStringKey = stringifyPg(0, pi)
                support_dictionary_by_pg[pgStringKey] = individualTFI["supportDict"]
                HTFI_by_pg[pgStringKey] = TFI_by_period_in_l_0[pi + 1]

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
                tid_limits = maximal_time_period_interval(starters_tid, level_0_periods_included[0] - 1,
                                                          level_0_periods_included[1] - 1)

                # Get a single merged TFI of 0-level periods involved
                possible_itemsets_in_pg = getTFIUnion(TFI_by_period_in_l_0, level_0_periods_included)

                list_to_parallel.append((possible_itemsets_in_pg, total_transactions, tid_limits,
                                         paralel_processing_on_k, min_rsup, pgStringKey))

            if paralelExcecution:
                pool = multiprocessing.Pool(usable_cpu_count)
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
                    pis, ttx, tidlts, pOnK, min_rsup, pgk = param
                    pgKey, HTFI_res, sup_dict = database.getIndividualTFIForNotLeafsGranules(pis, ttx, tidlts, pOnK,
                                                                                             min_rsup, pgk)
                    if len(HTFI_res) > 0:
                        HTFI_by_pg[pgKey] = HTFI_res
                        support_dictionary_by_pg[pgKey] = sup_dict
                    else:
                        del support_dictionary_by_pg[pgKey]

    return {'HTFI_by_pg': HTFI_by_pg, 'support_dictionary_by_pg': support_dictionary_by_pg}


def HTAR_BY_PG(database, min_rsup, min_rconf, lam, generalized_rules=False, min_r=None, paralelExecution=False,
               paralelExcecutionOnK=False, HTG=[24, 12, 4, 1], paralel_rule_generation=False):
    """
    :param database: The database you are using
    :param min_rsup: minimum support to consider frequent in inner time granules
    :param min_rconf: minimum confidence to consider a frequent rule
    :param generalized_rules: enables HTGAR
    :param min_r: minimum r used for Cumulate in HTGAR (only required if generalized_rules is enabled)
    :param lam: minimum support to consider frequent in leaf time granules
    :param paralelExcecution: Enable paralel Excecution on each time granule per level.
    :param paralelExcecutionOnK: Enable paralel excecution on each element of each K when calculating frequent itemsets
    :param paralel_rule_generation: Enable paralel excecution on each time granule frequent itemsets while generating association rules
    :param HTG: Temporal Time Hierarchy

    :return: A set of Association rules indexed by time granule.
    """
    # PHASE 1: FIND TEMPORAL FREQUENT ITEMSETS (l_level = 0) AND PHASE 2: FIND ALL HIERARCHICAL TEMPORAL FREQUENT ITEMSETS
    phase_1_and_2_res = getGranulesFrequentsAndSupports(database, min_rsup, lam, paralelExecution, paralelExcecutionOnK,
                                                        HTG, generalized_rules)
    # PHASE 3: FIND ALL HIERARCHICAL TEMPORAL ASSOCIATION RULES
    pgKeys = phase_1_and_2_res['HTFI_by_pg']
    suppDictionaryByPg = phase_1_and_2_res['support_dictionary_by_pg']

    return getRulesForEachTimeGranule(min_rconf, pgKeys, suppDictionaryByPg, paralel_rule_generation, min_r, database,
                                      generalized_rules)


def getRulesForEachTimeGranule(min_rconf, pgKeys, suppDictionaryByPg, paralel_rule_generation=False, min_r=None,
                               database=None, generalized_rules=False):
    """
    :param min_rconf: minimum confidence to consider a frequent rule
    :param pgKeys: Dictionary (timeGranuleKey -> Dictionary(k-length -> Set of k-size itemset frequent))
    :param suppDictionaryByPg: Dictionary (timeGranuleKey -> Dictionary(itemset -> support))
    :param paralel_rule_generation: Enable paralel excecution on each time granule frequent itemsets while generating association rules
    :param min_r: minimum r used for Cumulate in HTGAR (only required if generalized_rules is enabled)
    :param database: The database you are using
    :param generalized_rules: enables HTGAR

    :return: A set of Association rules indexed by time granule.
    """
    HTFS_by_pg = {}
    list_to_parallel = []
    pool = None
    if paralel_rule_generation:
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
    for pg in pgKeys.keys():
        list_to_parallel.append((pg, pgKeys[pg], suppDictionaryByPg[pg], min_rconf,database, min_r))

    if paralel_rule_generation:
        rulesByPg = pool.starmap(rule_generation_paralel, list_to_parallel)
        for pgRule in rulesByPg:
            if len(pgRule[1]) > 0:
                HTFS_by_pg[pgRule[0]] = pgRule[1]
        pool.close()
        pool.join()
    else:
        total_rules = 0
        for param in list_to_parallel:
            pg, candidates, supDic, min_rconf, database, min_r = param
            pg_rules = rule_generation(frequent_dictionary=candidates, support_dictionary=supDic,
                                       min_confidence=min_rconf, parallel_rule_gen=False, min_r=min_r,
                                       database=database, pg=pg)
            if len(pg_rules) > 0:
                HTFS_by_pg[pg] = pg_rules
                ancestral_rules = 0
                total_rules += len(pg_rules)
                for rule in pg_rules:  # rule : AssociationRule
                    itemset_rule = rule.getItemset()  # set(item_id)
                    if generalized_rules and database.is_ancestral_rule(itemset_rule):
                        ancestral_rules += 1
    return HTFS_by_pg


def rule_generation_paralel(pg, pgKeys, pgSupDic, min_rconf, database, min_r):
    return pg, rule_generation(frequent_dictionary=pgKeys, support_dictionary=pgSupDic, min_confidence=min_rconf,
                               parallel_rule_gen=False, min_r=min_r, database=database, pg=pg)
