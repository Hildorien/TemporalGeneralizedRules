import itertools

from rule_generator import rule_generation
from utility import generateCanidadtesOfSizeK, getPeriodsIncluded, getTFIUnion, stringifyPg, hash_candidate
import multiprocessing

def apriori(database, min_support, min_confidence, parallel_count=False, parallel_rule_gen=False):
    """
    :param database:
    :param min_support:
    :param min_confidence:
    :param parallel_count: Optional parameter to perform candidate count in parallel
    :return: a set of AssociationRules
    """
    # STEP 1: Frequent itemset generation
    all_items = sorted(list(database.items_dic.keys()))
    k = 1
    support_dictionary = {}
    frequent_dictionary = {}
    while (k == 1 or frequent_dictionary[k - 1] != []) and k <= len(all_items):
        candidates_size_k = generateCanidadtesOfSizeK(k, all_items, frequent_dictionary)
        # print('Candidates of size ' + str(k) + ' is ' + str(len(candidates_size_k)))
        # print('Calculating support of each candidate of size ' + str(k))
        # start = time.time()
        frequent_dictionary[k] = []
        if parallel_count:
            pool = multiprocessing.Pool(multiprocessing.cpu_count())
            results = pool.starmap(calculateSupport, zip(candidates_size_k, itertools.repeat(database)))
            for a_result in results:
                if a_result[1] >= min_support:
                    frequent_dictionary[k].append(a_result[0])
                    support_dictionary[hash_candidate(a_result[0])] = a_result[1]
        else:
            for a_candidate_size_k in candidates_size_k:
                support = database.supportOf(a_candidate_size_k)
                if support >= min_support:
                    frequent_dictionary[k].append(a_candidate_size_k)
                    support_dictionary[hash_candidate(a_candidate_size_k)] = support
        # end = time.time()
        # print('Took ' + (str(end - start) + ' seconds'))
        k += 1
    # STEP 2: Rule Generation
    rules = rule_generation(frequent_dictionary, support_dictionary, min_confidence, parallel_rule_gen)
    return rules


def calculateSupport(a_candidate, database):
    return a_candidate, database.supportOf(a_candidate)


def findIndividualTFI(database, l_level, pj, lam):
    # Returns every Temporal Frequent Itemsets (of every length) TFI_j, for the j-th time period p_j of llevel-period.
    ptt_entry = database.getPTTValueFromLlevelAndPeriod(l_level, pj)
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
        for k_size_itemset in C_j:
            support = database.supportOf(k_size_itemset, l_level, pj)
            if support > lam:
                TFI_r.append(tuple(k_size_itemset))
                support_dictionary[hash_candidate(k_size_itemset)] = support
                frequent_dictionary[r].append(k_size_itemset)
        if len(TFI_r) > 0:
            TFI_j[r] = set(TFI_r)
        TFI_r = list()
        r += 1
    return {"TFI": TFI_j, "supportDict": support_dictionary}


def HTAR_BY_PG(database, min_rsup, min_rconf, lam, HTG=[24, 12, 4, 1]):
    """
    :param database:
    :param min_support:
    :param min_confidence:
    :return: a set of AssociationRules
    """
    # PHASE 1: FIND TEMPORAL FREQUENT ITEMSETS (l_level = 0)

    TFI_by_period_in_l_0 = {}
    support_dictionary_by_pg = {}
    HTFI_by_pg = {}

    for pi in range(HTG[0]):
        individualTFI = findIndividualTFI(database, 0, pi + 1, lam)
        if len(individualTFI["TFI"]) > 0:
            TFI_by_period_in_l_0[pi + 1] = individualTFI["TFI"]
            pgStringKey = stringifyPg(0, pi + 1)
            support_dictionary_by_pg[pgStringKey] = individualTFI["supportDict"]
            HTFI_by_pg[pgStringKey] = TFI_by_period_in_l_0[pi + 1]

    # PHASE 2: FIND ALL HIERARCHICAL TEMPORAL FREQUENT ITEMSETS

    HTFI = {}
    for l_level in range(len(HTG)):
        if l_level != 0:
            for period in range(HTG[l_level]):
                pgStringKey = stringifyPg(l_level, period + 1)
                support_dictionary_by_pg[pgStringKey] = {}
                level_0_periods_included = getPeriodsIncluded(l_level, period + 1)

                # Get a single merged TFI of 0-level periods involved
                possible_itemsets_in_pg = getTFIUnion(TFI_by_period_in_l_0, level_0_periods_included)

                for k in possible_itemsets_in_pg:
                    itemsets_length_k = possible_itemsets_in_pg[k]
                    frequent_itemsets_length_k = set()
                    for itemset in itemsets_length_k:
                        itemsetSupport = database.getItemsetRelativeSupport(itemset, level_0_periods_included)
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

    # PHASE 3: FIND ALL HIERARCHICAL TEMPORAL ASSOCIATION RULES
    HTFS_by_pg = {}
    for pg in HTFI_by_pg.keys():
        pg_rules = rule_generation(HTFI_by_pg[pg], support_dictionary_by_pg[pg], min_rconf)
        if len(pg_rules) > 0:
            HTFS_by_pg[pg] = pg_rules

    return HTFS_by_pg
