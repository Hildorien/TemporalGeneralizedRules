import itertools
import time

from HTAR.HTAR_utility import getPeriodsIncluded, getTFIUnion
from rule_generator import rule_generation
from utility import generateCanidadtesOfSizeK, stringifyPg, hash_candidate
import multiprocessing

def calculateSupportInPJ(a_candidate, database, l_level, pj, hong=False):
    return a_candidate, database.supportOf(a_candidate, l_level, pj, hong)

def findIndividualTFI(database, pj, lam, parallel_count=False, hong = False):
    # Returns every Temporal Frequent Itemsets (of every length) TFI_j, for the j-th time period p_j of llevel-period.
    ptt_entry = database.getPTTValueFromLeafLevelGranule(pj)
    TFI_j = {}
    r = 1
    allItems = list(ptt_entry['itemsSet'])
    C_j = allItems
    C_j.sort()
    TFI_r = list()
    frequent_dictionary = {}
    support_dictionary = {}
    while (r == 1 or frequent_dictionary[r - 1] != []) and r <= len(allItems):
        frequent_dictionary[r] = []
        C_j = generateCanidadtesOfSizeK(r, C_j, frequent_dictionary)
        # print("NUEVO R")
        # print(str(r))
        # print(str(len(C_j)))
        # print("/////////////////")
        if parallel_count:
            pool = multiprocessing.Pool(multiprocessing.cpu_count())
            results = pool.starmap(calculateSupportInPJ, zip(C_j, itertools.repeat(database), itertools.repeat(0), itertools.repeat(pj)))
            for a_result in results:
                if a_result[1] >= lam:
                    TFI_r.append(tuple(a_result[0]))
                    support_dictionary[hash_candidate(a_result[0])] = a_result[1]
                    frequent_dictionary[r].append(a_result[0])
            pool.close()
            pool.join()
        else:
            for k_size_itemset in C_j:
                support = database.supportOf(k_size_itemset, 0, pj)
                if support >= lam:
                    TFI_r.append(tuple(k_size_itemset))
                    support_dictionary[hash_candidate(k_size_itemset)] = support
                    frequent_dictionary[r].append(k_size_itemset)
        if len(TFI_r) > 0:
            TFI_j[r] = set(TFI_r)
        TFI_r = list()
        r += 1
    return {"TFI": TFI_j, "supportDict": support_dictionary}


def HTAR_BY_PG(database, min_rsup, min_rconf, lam, HTG=[24, 12, 4, 1], paralelExecution = False):
    """
    :param database:
    :return: a set of AssociationRules
    """
    # PHASE 1: FIND TEMPORAL FREQUENT ITEMSETS (l_level = 0)

    TFI_by_period_in_l_0 = {}
    support_dictionary_by_pg = {}
    HTFI_by_pg = {}

    hong = False
    if HTG == [10, 5, 1]:
        hong = True

    for pi in range(HTG[0]):
        # start = time.time()
        individualTFI = findIndividualTFI(database, pi + 1, lam, paralelExecution)

        if len(individualTFI["TFI"]) > 0:
            TFI_by_period_in_l_0[pi + 1] = individualTFI["TFI"]
            pgStringKey = stringifyPg(0, pi + 1)
            support_dictionary_by_pg[pgStringKey] = individualTFI["supportDict"]
            HTFI_by_pg[pgStringKey] = TFI_by_period_in_l_0[pi + 1]
        # end = time.time()

        # totalFrequent = 0
        # for k in TFI_by_period_in_l_0[pi + 1]:
        #     totalFrequent += len(TFI_by_period_in_l_0[pi + 1][k])
        # print(str(pi) + ' leaf-TFI took ' + (str(end - start) + ' seconds'))
        # print('('+ str(totalFrequent) + ' frecuent itemsets )')
        # print('-------------')


    # PHASE 2: FIND ALL HIERARCHICAL TEMPORAL FREQUENT ITEMSETS

    HTFI = {}
    for l_level in range(len(HTG)):
        if l_level != 0:
            for period in range(HTG[l_level]):
                #start = time.time()

                pgStringKey = stringifyPg(l_level, period + 1)
                support_dictionary_by_pg[pgStringKey] = {}
                level_0_periods_included = getPeriodsIncluded(l_level, period + 1)

                # Get a single merged TFI of 0-level periods involved
                possible_itemsets_in_pg = getTFIUnion(TFI_by_period_in_l_0, level_0_periods_included)

                for k in possible_itemsets_in_pg:
                    itemsets_length_k = possible_itemsets_in_pg[k]
                    frequent_itemsets_length_k = set()
                    for itemset in itemsets_length_k:
                        itemsetSupport = database.supportOf(itemset, l_level, period + 1)
                        if itemsetSupport is not None and itemsetSupport >= min_rsup:
                            frequent_itemsets_length_k.add(itemset)
                            support_dictionary_by_pg[pgStringKey][hash_candidate(itemset)] = itemsetSupport

                    if len(frequent_itemsets_length_k) > 0:
                        HTFI[k] = frequent_itemsets_length_k

                if len(HTFI) > 0:
                    HTFI_by_pg[pgStringKey] = HTFI
                else:
                    del support_dictionary_by_pg[pgStringKey]
                HTFI = {}

                # end = time.time()
                # print(pgStringKey + 'took ' + (str(end - start) + ' seconds'))
                # if pgStringKey in HTFI_by_pg:
                #     print('(' +  str(len(HTFI_by_pg[pgStringKey])) + ' frecuent itemsets)')
                # else:
                #     print('(no frecuent itemsets)')


    # PHASE 3: FIND ALL HIERARCHICAL TEMPORAL ASSOCIATION RULES
    HTFS_by_pg = {}
    for pg in HTFI_by_pg.keys():
        pg_rules = rule_generation(HTFI_by_pg[pg], support_dictionary_by_pg[pg], min_rconf)
        # print(pg + " RULES")
        # print(len(pg_rules))
        # print("----")
        if len(pg_rules) > 0:
            HTFS_by_pg[pg] = pg_rules

    return HTFS_by_pg
