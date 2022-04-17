import itertools
import time

from rule_generator import rule_generation
from utility import generateCanidadtesOfSizeK, hash_candidate, append_tids, calculate_support_for_parallel
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
    start = time.time()
    all_items = sorted(list(database.items_dic.keys()))
    k = 1
    support_dictionary = {}
    frequent_dictionary = {}
    totalCandidates = 0
    total_transactions = database.tx_count
    items_dic = database.items_dic
    matrix_data_by_item = database.matrix_data_by_item
    pool = None
    if parallel_count:
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
    while (k == 1 or frequent_dictionary[k - 1] != []) and k <= len(all_items):
        candidates_size_k = generateCanidadtesOfSizeK(k, all_items, frequent_dictionary)
        totalCandidates += len(candidates_size_k)
        print('Candidates of size ' + str(k) + ' is ' + str(len(candidates_size_k)))
        print('Calculating support of each candidate of size ' + str(k))
        start = time.time()
        frequent_dictionary[k] = []
        if parallel_count:
            list_to_parallel = [append_tids(x, items_dic, matrix_data_by_item) for x in candidates_size_k]
            results = pool.starmap(calculate_support_for_parallel, list_to_parallel)
            for a_result in results:
                candidate_support = a_result[1] / total_transactions
                if candidate_support >= min_support:
                    frequent_dictionary[k].append(a_result[0])
                    support_dictionary[hash_candidate(a_result[0])] = candidate_support
        else:
            for a_candidate_size_k in candidates_size_k:
                support = database.supportOf(a_candidate_size_k)
                if support >= min_support:
                    frequent_dictionary[k].append(a_candidate_size_k)
                    support_dictionary[hash_candidate(a_candidate_size_k)] = support
        end = time.time()
        print('Took ' + (str(end - start) + ' seconds in k =' + str(k) + ' with this amount of candidates: ' + str(len(candidates_size_k))))
        k += 1
    end = time.time()
    print('Frequent phase took ' + (str(end - start)))
    print("Apriori evaluated " + str(totalCandidates) + ' candidates')
    totalFrecuent = 0
    for k in frequent_dictionary:
        totalFrecuent += len(frequent_dictionary[k])
    print("Total frequents: " + str(totalFrecuent))
    # STEP 2: Rule Generation
    start = time.time()
    rules = rule_generation(frequent_dictionary, support_dictionary, min_confidence, parallel_rule_gen)
    end = time.time()
    print('Rule phase took ' + (str(end - start)))
    return rules


def calculateSupport(a_candidate, database):
    return a_candidate, database.supportOf(a_candidate)