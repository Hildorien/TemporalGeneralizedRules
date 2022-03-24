import itertools
import time

from rule_generator import rule_generation
from utility import generateCanidadtesOfSizeK, hash_candidate
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
    pool = None
    if parallel_count:
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
    while (k == 1 or frequent_dictionary[k - 1] != []) and k <= len(all_items):
        candidates_size_k = generateCanidadtesOfSizeK(k, all_items, frequent_dictionary)
        # print('Candidates of size ' + str(k) + ' is ' + str(len(candidates_size_k)))
        # print('Calculating support of each candidate of size ' + str(k))
        # start = time.time()
        frequent_dictionary[k] = []
        if parallel_count:
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