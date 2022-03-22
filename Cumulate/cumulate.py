import multiprocessing
import time
import itertools

from Cumulate.cumulate_utility import calculate_C1, calculate_C2, calculate_Ck, prune_candidates_in_same_family, \
    count_candidates_in_transaction, calculate_support
from rule_generator import rule_generation
from utility import hash_candidate

import logging

loggerA = logging.getLogger(__name__ + '_vanilla')
loggerB = logging.getLogger(__name__ + '_vertical')


def cumulate(horizontal_database, min_supp, min_conf, min_r):
    """
    :param horizontal_database: An HorizontalDatabase instance
    :param min_supp: User-defined minimum support
    :param min_conf: User-defined minimum confidence
    :param min_r: User-defined minimum R interesting measure
    :return: Set of association rules
    """
    start = time.time()
    # Instantiate local variables
    frequent_dictionary = {}
    k = 1
    transactions = horizontal_database.transactions
    transaction_count = horizontal_database.transaction_count
    taxonomy = horizontal_database.taxonomy  # Cumulate Optimization 2 in original paper
    all_items = sorted(list(horizontal_database.items_dic.keys()))
    support_dictionary = {}  # Auxiliary structure for rule-generation
    while k == 1 or frequent_dictionary[k - 1] != []:
        if k == 1:
            candidate_hashmap = calculate_C1(all_items, k)
        elif k == 2:
            candidate_hashmap = calculate_C2(frequent_dictionary, k)
        else:
            candidate_hashmap = calculate_Ck(frequent_dictionary, k)
        if k == 2:
            prune_candidates_in_same_family(candidate_hashmap, taxonomy)  # Cumulate Optimization 3 in original paper
        candidates_size_k = list(candidate_hashmap.values())
        frequent_dictionary[k] = []
        if len(candidates_size_k) == 0:
            break
        taxonomy_pruned = horizontal_database.prune_ancestors(
            candidates_size_k)  # Cumulate Optimization 1 in original paper

        candidate_support_dictionary = dict.fromkeys(candidate_hashmap.keys(),
                                                     0)  # Auxiliary structure for counting supports of size k
        # Count Candidates of size k in transactions
        for a_transaction in transactions:
            expanded_transaction = horizontal_database.expand_transaction(a_transaction, taxonomy_pruned)
            count_candidates_in_transaction(k, expanded_transaction, candidate_support_dictionary, candidate_hashmap)

        # Frequent itemset generation of size k
        for hashed_itemset in candidate_support_dictionary:
            support = candidate_support_dictionary[hashed_itemset] / transaction_count
            support_dictionary[hashed_itemset] = support
            if support >= min_supp:
                frequent_dictionary[k].append(candidate_hashmap[hashed_itemset])

        k += 1
    end = time.time()
    loggerA.info('FrequentPhase,' + str(end-start))
    # Generate Rules
    start = time.time()
    rules = rule_generation(frequent_dictionary, support_dictionary, min_conf,
                            False, taxonomy, min_r, horizontal_database)
    end = time.time()
    loggerA.info('RulePhase,' + str(end-start))
    return rules


def vertical_cumulate(vertical_database, min_supp, min_conf, min_r, parallel_count=False, parallel_rule_gen=False):
    """
    :param vertical_database: A Database instance
    :param min_supp: User-defined minimum support
    :param min_conf: User-defined minimum confidence
    :param min_r: User-defined minimum R interesting measure
    :param parallel_count: Optional parameter to set parallel counting for supports
    :param parallel_rule_gen: Optional parameter to set parallel rule gen
    :return: Set of association rules
    """
    start = time.time()
    # Instantiate local variables
    frequent_dictionary = {}
    support_dictionary = {}
    k = 1
    taxonomy = vertical_database.taxonomy  # Cumulate Optimization 2 in original paper
    all_items = sorted(list(vertical_database.items_dic.keys()))
    pool = None
    if parallel_count:
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
    while k == 1 or frequent_dictionary[k - 1] != []:
        # Candidate Generation
        if k == 1:
            candidate_hashmap = calculate_C1(all_items, k)
        elif k == 2:
            candidate_hashmap = calculate_C2(frequent_dictionary, k)
        else:
            candidate_hashmap = calculate_Ck(frequent_dictionary, k)
        if k == 2:
            prune_candidates_in_same_family(candidate_hashmap, taxonomy)  # Cumulate Optimization 3 in original paper
        candidates_size_k = list(candidate_hashmap.values())
        frequent_dictionary[k] = []
        if len(candidates_size_k) == 0:
            break
        if parallel_count:
            results = pool.starmap(calculate_support, zip(candidates_size_k, itertools.repeat(vertical_database)))
            for a_result in results:
                support_dictionary[hash_candidate(a_result[0])] = a_result[1]
                if a_result[1] >= min_supp:
                    frequent_dictionary[k].append(a_result[0])
        else:
            for a_candidate_size_k in candidates_size_k:
                support = vertical_database.supportOf(a_candidate_size_k)
                support_dictionary[hash_candidate(a_candidate_size_k)] = support
                if support >= min_supp:
                    frequent_dictionary[k].append(a_candidate_size_k)
        k += 1
    end = time.time()
    loggerB.info('FrequentPhase,' + str(end-start))
    # Generate Rules
    start = time.time()
    rules = rule_generation(frequent_dictionary, support_dictionary, min_conf,
                            parallel_rule_gen, taxonomy, min_r, vertical_database)
    end = time.time()
    loggerB.info('RulePhase,' + str(end-start))
    return rules
