import itertools
from itertools import combinations
from multiprocessing import freeze_support

from utility import getValidJoin
from utility import allSubsetofSizeKMinus1
from utility import joinlistOfInts
from utility import flatten
import time
from DataStructures.AssociationRule import AssociationRule
import multiprocessing

def apriori_gen(frequent_itemset_of_size_k_minus_1, frequent_dictionary):
    """
    :param frequent_itemset: An ORDERED set/list of itemsets in which each itemset is also ordered
    :param frequent_dictionary: Auxiliary structure for fast lookup of frequents of size k-1
    :return:
    """
    k = len(frequent_itemset_of_size_k_minus_1[0]) + 1
    # STEP 1: JOIN
    candidates_of_size_k = []
    n = len(frequent_itemset_of_size_k_minus_1)
    for i in range(n):
        j = i + 1
        while j < n:
            joined_itemset = getValidJoin(frequent_itemset_of_size_k_minus_1[i], frequent_itemset_of_size_k_minus_1[j])
            if joined_itemset is None:
                break
            else:
                candidates_of_size_k.append(joined_itemset)
            j += 1
    # STEP 2: PRUNE -> For each itemset in L_k check if all subsets are frequent
    for a_candidate_of_size_k in candidates_of_size_k:  # Iterating over a lists of lists
        subsets_of_size_k_minus_1 = allSubsetofSizeKMinus1(a_candidate_of_size_k, k - 1)  # a_candidate_of_size_k is a list
        for a_subset_of_size_k_minus_1 in subsets_of_size_k_minus_1:
            if not (a_subset_of_size_k_minus_1 in frequent_dictionary[k - 1]):
                candidates_of_size_k.remove(a_candidate_of_size_k)  # Prunes the entire candidate
                break

    return candidates_of_size_k

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
    while (k==1 or frequent_dictionary[k - 1] != []) and k < len(all_items):
        candidates_size_k = []
        if k == 1:
            candidates_size_k = list(map(lambda x: [x], all_items))
        elif k == 2:
            candidates_size_k = list(map(list, combinations(flatten(frequent_dictionary[1]), 2)))  # Treat k = 2 as a special case
        else:
            candidates_size_k = apriori_gen(frequent_dictionary[k - 1], frequent_dictionary)
        #print('Candidates of size ' + str(k) + ' is ' + str(len(candidates_size_k)))
        #print('Calculating support of each candidate of size ' + str(k))
        start = time.time()
        frequent_dictionary[k] = []
        for a_candidate_size_k in candidates_size_k:
            support = database.supportOf(a_candidate_size_k)
            if support >= min_support:
                frequent_dictionary[k].append(a_candidate_size_k)
                support_dictionary[joinlistOfInts(a_candidate_size_k)] = support
        end = time.time()
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
        if k == 1:
            candidates_size_k = list(map(lambda x: [x], all_items))
        elif k == 2:
            candidates_size_k = list(
                map(list, combinations(flatten(frequent_dictionary[1]), 2)))  # Treat k = 2 as a special case
        else:
            candidates_size_k = apriori_gen(frequent_dictionary[k - 1], frequent_dictionary)
        print('Candidates of size ' + str(k) + ' is ' + str(len(candidates_size_k)))
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
