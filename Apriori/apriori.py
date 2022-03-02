import itertools
from itertools import combinations
from multiprocessing import freeze_support

from utility import getValidJoin, generateCanidadtesOfSizeK, getPeriodsIncluded, getTFIUnion, stringifyPg
from utility import allSubsetofSizeKMinus1
from utility import joinlistOfInts
from utility import flatten
import time
from DataStructures.AssociationRule import AssociationRule
import multiprocessing

def rule_generation(frequent_dictionary, support_dictionary, min_confidence):
    rules = []
    for key in frequent_dictionary:
        if key != 1:
            frequent_itemset_k = frequent_dictionary[key]
            for a_itemset_k in frequent_itemset_k:
                for idx, item in enumerate(a_itemset_k):

                    if type(a_itemset_k) == tuple:
                        a_itemset_k = list(a_itemset_k)

                    frequent_itemset_copy = a_itemset_k.copy()
                    consequent = [frequent_itemset_copy.pop(idx)]
                    antecedent = frequent_itemset_copy
                    support_antecedent = support_dictionary[joinlistOfInts(antecedent)]
                    support_all_items = support_dictionary[joinlistOfInts(a_itemset_k)]
                    confidence = support_all_items / support_antecedent

                    if confidence >= min_confidence:
                        association_rule = AssociationRule(antecedent, consequent, support_all_items, confidence)
                        rules.append(association_rule)
    return rules

def apriori(database, min_support, min_confidence):
    """
    :param database:
    :param min_support:
    :param min_confidence:
    :return: a set of AssociationRules
    """
    #STEP 1: Frequent itemset generation
    all_items = sorted(list(database.items_dic.keys()))
    k = 1
    support_dictionary = {}
    frequent_dictionary = {}
    candidates_size_k = []
    while (k==1 or frequent_dictionary[k - 1] != []) and k < len(all_items):
        candidates_size_k = generateCanidadtesOfSizeK(k, all_items, frequent_dictionary)
        #print('Candidates of size ' + str(k) + ' is ' + str(len(candidates_size_k)))
        #print('Calculating support of each candidate of size ' + str(k))
        #start = time.time()
        frequent_dictionary[k] = []
        for a_candidate_size_k in candidates_size_k:
            support = database.supportOf(a_candidate_size_k)
            if support >= min_support:
                frequent_dictionary[k].append(a_candidate_size_k)
                support_dictionary[joinlistOfInts(a_candidate_size_k)] = support
        #end = time.time()
        #print('Took ' + (str(end - start) + ' seconds'))
        k += 1
    #STEP 2: Rule Generation
    rules = rule_generation(frequent_dictionary, support_dictionary, min_confidence)
    return rules


def apriori_parallel_count(database, min_support, min_confidence):
    """
        :param database:
        :param min_support:
        :param min_confidence:
        :return: a set of AssociationRules
        """
    # STEP 1: Frequent itemset generation
    all_items = sorted(list(database.items_dic.keys()))
    k = 1
    support_dictionary = {}
    frequent_dictionary = {}
    while (k == 1 or frequent_dictionary[k - 1] != []) and k < len(all_items):
        candidates_size_k = []
        candidates_size_k = generateCanidadtesOfSizeK(k, all_items, frequent_dictionary)
        # print('Calculating support of each candidate of size ' + str(k))
        frequent_dictionary[k] = []
        start = time.time()
        pool = multiprocessing.Pool(8)
        results = pool.starmap(calculateSupport, zip(candidates_size_k, itertools.repeat(database)))
        for a_result in results:
            if a_result[1] >= min_support:
                frequent_dictionary[k].append(a_result[0])
                support_dictionary[joinlistOfInts(a_result[0])] = a_result[1]

        """for a_candidate_size_k in candidates_size_k:
            support = database.supportOf(a_candidate_size_k)
            if support >= min_support:
                frequent_dictionary[k].append(a_candidate_size_k)
                support_dictionary[joinlistOfInts(a_candidate_size_k)] = support
        """
        end = time.time()
        #print('Iteration ' + str(k) + ' took ' + (str(end - start) + ' seconds'))
        k += 1
    # STEP 2: Rule Generation
    rules = rule_generation(frequent_dictionary, support_dictionary, min_confidence)
    return rules

def calculateSupport(a_candidate, database):
    return (a_candidate, database.supportOf(a_candidate))


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
    while (r == 1 or frequent_dictionary[r - 1] != []) and r < len(allItems):
        frequent_dictionary[r] = []
        C_j = generateCanidadtesOfSizeK(r, C_j, frequent_dictionary)
        for k_size_itemset in C_j:
            support = database.supportOf(k_size_itemset, l_level, pj)
            if support > lam:
                TFI_r.append(tuple(k_size_itemset))
                support_dictionary[joinlistOfInts(k_size_itemset)] = support
                frequent_dictionary[r].append(k_size_itemset)
        if len(TFI_r) > 0:
            TFI_j[r] = set(TFI_r)
        TFI_r = list()
        r += 1
    return {"TFI": TFI_j, "supportDict": support_dictionary}


def HTAR_BY_PG(database, min_rsup, min_rconf, lam, HTG = [24, 12, 4, 1]):
    """
    :param database:
    :param min_support:
    :param min_confidence:
    :return: a set of AssociationRules
    """
    #PHASE 1: FIND TEMPORAL FREQUENT ITEMSETS (l_level = 0)

    TFI_by_period_in_l_0 = {}
    support_dictionary_by_pg = {}
    HTFI_by_pg = {}


    for pi in range(HTG[0]):
        individualTFI = findIndividualTFI(database, 0, pi+1, lam)
        if len(individualTFI["TFI"]) > 0:
            TFI_by_period_in_l_0[pi+1] = individualTFI["TFI"]
            pgStringKey = stringifyPg(0, pi+1)
            support_dictionary_by_pg[pgStringKey] = individualTFI["supportDict"]
            HTFI_by_pg[pgStringKey] = TFI_by_period_in_l_0[pi+1]

    #PHASE 2: FIND ALL HIERARCHICAL TEMPORAL FREQUENT ITEMSETS

    HTFI = {}
    for l_level in range(len(HTG)):
        if l_level != 0:
            for period in range(HTG[l_level]):
                pgStringKey = stringifyPg(l_level, period+1)
                support_dictionary_by_pg[pgStringKey] = {}
                level_0_periods_included = getPeriodsIncluded(l_level, period+1)

                #Get a single merged TFI of 0-level periods involved
                possible_itemsets_in_pg = getTFIUnion(TFI_by_period_in_l_0, level_0_periods_included)

                for k in possible_itemsets_in_pg:
                    itemsets_length_k = possible_itemsets_in_pg[k]
                    frequent_itemsets_length_k = set()
                    for itemset in itemsets_length_k:
                        itemsetSupports = database.getItemsetRelativeSupportLowerBound(itemset, level_0_periods_included)
                        if itemsetSupports is not None and (itemsetSupports["rslb"] > min_rsup or itemsetSupports["rsup"] > min_rsup):
                            frequent_itemsets_length_k.add(itemset)
                            support_dictionary_by_pg[pgStringKey][joinlistOfInts(itemset)] = max(itemsetSupports["rslb"], itemsetSupports["rsup"])

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
