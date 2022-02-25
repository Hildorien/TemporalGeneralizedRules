import itertools
from itertools import combinations
from multiprocessing import freeze_support

from utility import getValidJoin, generateCanidadtesOfSizeK
from utility import allSubsetofSizeKMinus1
from utility import joinlistOfInts
from utility import flatten
import time
from DataStructures.AssociationRule import AssociationRule
import multiprocessing

def rule_generation(frequent_dictionary, support_dictionary, min_confidence, database):
    rules = []
    for key in frequent_dictionary:
        if key != 1:
            frequent_itemset_k = frequent_dictionary[key]
            for a_itemset_k in frequent_itemset_k:
                for idx, item in enumerate(a_itemset_k):

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
    rules = rule_generation(frequent_dictionary, support_dictionary, min_confidence, database)
    return rules


def apriori_mapreduce(database, min_support, min_confidence):
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
        print('Iteration ' + str(k) + ' took ' + (str(end - start) + ' seconds'))
        k += 1
    # STEP 2: Rule Generation
    rules = rule_generation(frequent_dictionary, support_dictionary, min_confidence, database)
    return rules

def calculateSupport(a_candidate, database):
    return (a_candidate, database.supportOf(a_candidate))

#WIP
def findIndividualTFI(database, l_level, pj, lam):
    # Returns every Temporal Frequent Itemsets (of every length) TFI_j, for the j-th time period p_j of llevel-period.
    ptt_entry = database.getPTTValueFromLlevelAndPeriod(l_level, pj)
    TFI_j = {}
    totalTransactions = ptt_entry['totalTransactions']
    r = 1
    allItems = list(ptt_entry['itemsSet'])
    C_j = allItems
    C_j.sort()
    TFI_r = list()
    frequent_dictionary = {}
    while (len(C_j) > 0 or r == 1) and r < len(allItems):
        frequent_dictionary[r] = []
        C_j = generateCanidadtesOfSizeK(r, C_j, frequent_dictionary)
        for k_itemset in C_j:
            if database.supportOf(k_itemset, l_level, pj) > lam:
                TFI_r.append(k_itemset)
                frequent_dictionary[r].append(k_itemset)
        if len(TFI_r) > 0 :
            TFI_j[r] = TFI_r
        TFI_r = list()
        r+=1
    return TFI_j