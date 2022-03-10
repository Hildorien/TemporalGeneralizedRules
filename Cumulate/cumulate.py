import multiprocessing
import time
import itertools
from rule_generator import rule_generation
from utility import hash_candidate, flatten, getValidJoin, allSubsetofSizeKMinus1


def cumulate(horizontal_database, min_supp, min_conf, min_r):
    """
    :param horizontal_database: An HorizontalDatabase instance
    :param min_supp: User-defined minimum support
    :param min_conf: User-defined minimum confidence
    :param min_r: User-defined minimum R interesting measure
    :return: Set of association rules
    """
    # Instantiate local variables
    frequent_dictionary = {}
    k = 1
    transactions = horizontal_database.transactions
    transaction_count = horizontal_database.transaction_count
    taxonomy = horizontal_database.taxonomy  # Cumulate Optimization 2 in original paper
    all_items = sorted(list(horizontal_database.items_dic.keys()))
    support_dictionary = {}  # Auxiliary structure for rule-generation
    while (k == 1 or frequent_dictionary[k - 1] != []):
        # Candidate Generation
        # print('Iteration ' + str(k))
        # print('Candidate generation step...')
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
        # print('Pruning taxonomy...')
        start = time.time()
        taxonomy_pruned = horizontal_database.prune_ancestors(
            candidates_size_k)  # Cumulate Optimization 1 in original paper
        end = time.time()
        # print('Took ' + (str(end - start) + ' seconds to prune taxonomy'))
        candidate_support_dictionary = dict.fromkeys(candidate_hashmap.keys(),
                                                     0)  # Auxiliary structure for counting supports of size k
        # Count Candidates of size k in transactions
        # print('Counting candidates step. Passing over ' + str(len(transactions)) + ' transactions.')
        start = time.time()
        for a_transaction in transactions:
            expanded_transaction = horizontal_database.expand_transaction(a_transaction, taxonomy_pruned)
            count_candidates_in_transaction(k, expanded_transaction, candidate_support_dictionary, candidate_hashmap)

        end = time.time()
        # print('Took ' + (str(end - start) + ' seconds to count candidates of size ' + str(k) + ' in transactions'))
        # print('Frequent generation step...')
        start = time.time()
        # Frequent itemset generation of size k
        for hashed_itemset in candidate_support_dictionary:
            support = candidate_support_dictionary[hashed_itemset] / transaction_count
            support_dictionary[hashed_itemset] = support
            if support >= min_supp:
                frequent_dictionary[k].append(candidate_hashmap[hashed_itemset])
        end = time.time()
        # print('Took ' + (str(end - start) + ' seconds to generate ' + str(len(frequent_dictionary[k])) + ' frequents of size ' + str(k)))
        # print('--------------------------------------------------------------------------------')
        k += 1
    # Generate Rules
    start = time.time()
    # print('Generating rules...')
    rules = rule_generation(frequent_dictionary, support_dictionary, min_conf,
                            False, taxonomy, min_r, horizontal_database)
    end = time.time()
    # print('Took ' + (str(end - start) + ' seconds to generate ' + str(len(rules)) + ' rules'))
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
    # Instantiate local variables
    frequent_dictionary = {}
    support_dictionary = {}
    k = 1
    taxonomy = vertical_database.taxonomy  # Cumulate Optimization 2 in original paper
    all_items = sorted(list(vertical_database.items_dic.keys()))
    while k == 1 or frequent_dictionary[k - 1] != []:
        # Candidate Generation
        # print('Iteration ' + str(k))
        # print('Candidate generation step...')
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
        # print('Calculating support of each candidate of size ' + str(k))
        start = time.time()
        if parallel_count:
            pool = multiprocessing.Pool(multiprocessing.cpu_count())
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
        end = time.time()
        # print('Took ' + (str(end - start) + ' seconds'))
        k += 1
    # Generate Rules
    start = time.time()
    # print('Generating rules...')
    rules = rule_generation(frequent_dictionary, support_dictionary, min_conf,
                            parallel_rule_gen, taxonomy, min_r, vertical_database)
    end = time.time()
    # print('Took ' + (str(end - start) + ' seconds to generate ' + str(len(rules)) + ' rules'))
    return rules


def calculate_support(a_candidate, database):
    return a_candidate, database.supportOf(a_candidate)


def calculate_Ck(frequent_dictionary, k):
    start = time.time()
    candidate_hashmap = apriori_gen(frequent_dictionary[k - 1], frequent_dictionary)
    end = time.time()
    # print('Took ' + (str(end - start) + ' seconds to generate ' + str(len(list(candidate_hashmap.keys()))) + ' candidates of size ' + str(k) + ' using apriori-gen'))
    return candidate_hashmap


def calculate_C2(frequent_dictionary, k):
    candidate_hashmap = {}
    start = time.time()
    [add_candidate_to_hashmap(list(x), candidate_hashmap) for x in
     itertools.combinations(flatten(frequent_dictionary[1]), 2)]
    end = time.time()
    # print('Took ' + (str(end - start) + ' seconds to generate ' + str(len(list(candidate_hashmap.keys()))) + ' candidates of size ' + str(k) + ' using combinations'))
    return candidate_hashmap


def calculate_C1(all_items, k):
    candidate_hashmap = {}
    start = time.time()
    [add_candidate_to_hashmap(list(x), candidate_hashmap) for x in list(map(lambda x: [x], all_items))]
    end = time.time()
    # print('Took ' + (str(end - start) + ' seconds to generate ' + str(len(list(candidate_hashmap.keys()))) + ' candidates of size ' + str(k)))
    return candidate_hashmap


def prune_candidates_in_same_family(candidate_hashmap, taxonomy):
    for item in taxonomy:
        for ancestor in taxonomy[item]:
            hashed_itemset_1 = hash_candidate([item, ancestor])
            hashed_itemset_2 = hash_candidate([ancestor, item])
            if hashed_itemset_1 in candidate_hashmap:
                candidate_hashmap.pop(hashed_itemset_1)
            elif hashed_itemset_2 in candidate_hashmap:
                candidate_hashmap.pop(hashed_itemset_2)


def count_candidates_in_transaction(k, expanded_transaction, support_dictionary, candidate_hashmap):
    """
    Given an expanded transaction, increments the count of all candidates in candidates_size_k that are contained in expanded_transaction
    :param k: Number of iteration
    :param expanded_transaction: a list of ints representing a list of items
    :param support_dictionary: { 'hashed_candidate': support_count }
    :param candidate_hashmap: { 'hashed_candidate': candidate }
    :return:
    """
    if k < 3:  # If k is small candidate size is large
        all_subets = list(map(list, itertools.combinations(expanded_transaction, k)))
        for a_subset_size_k in all_subets:
            hashed_subset = hash_candidate(a_subset_size_k)
            if hashed_subset in candidate_hashmap:
                support_dictionary[hashed_subset] += 1
    else:  # If k is large candidates size is smaller
        for hashed_candidate in candidate_hashmap:
            candidate = candidate_hashmap[hashed_candidate]
            if set(candidate).issubset(set(expanded_transaction)):
                support_dictionary[hashed_candidate] += 1


def apriori_gen(frequent_itemset_of_size_k_minus_1, frequent_dictionary):
    """
    :param frequent_itemset_of_size_k_minus_1: An ORDERED set/list of itemsets in which each itemset is also ordered
    :param frequent_dictionary: Auxiliary structure for fast lookup of frequents of size k-1
    :return: candidate_hashmap: { 'hashed_candidate': [candidate] }
    """
    k = len(frequent_itemset_of_size_k_minus_1[0]) + 1
    # STEP 1: JOIN
    candidate_hashmap = {}
    n = len(frequent_itemset_of_size_k_minus_1)
    # print('Joining ' + str(n) + ' candidate of size ' + str(k - 1) + ' using apriori_gen')
    start = time.time()
    for i in range(n):
        j = i + 1
        while j < n:
            joined_itemset = getValidJoin(frequent_itemset_of_size_k_minus_1[i], frequent_itemset_of_size_k_minus_1[j])
            if joined_itemset is None:
                break
            else:
                candidate_hashmap[hash_candidate(joined_itemset)] = joined_itemset
            j += 1
    end = time.time()
    # print('Took ' + (str(end - start) + ' seconds to JOIN ' + str(n) + ' candidates of size ' + str(k - 1)))
    # STEP 2: PRUNE -> For each itemset in L_k check if all subsets are frequent
    if k > 2:
        candidates_size_k = list(candidate_hashmap.values()).copy()
        n = len(candidates_size_k)
        # print('Pruning ' + str(n) + ' candidates of size ' + str(k) + ' using apriori_gen')
        start = time.time()
        for i in range(n):
            subsets_of_size_k_minus_1 = allSubsetofSizeKMinus1(candidates_size_k[i],
                                                               k - 1)  # candidates_size_k[i] is a list
            for a_subset_of_size_k_minus_1 in subsets_of_size_k_minus_1:
                if not (a_subset_of_size_k_minus_1 in frequent_dictionary[k - 1]):
                    candidate_hashmap.pop(hash_candidate(candidates_size_k[i]), None)  # Prunes the entire candidate
                    break
        end = time.time()
        # print('Took ' + (str(end - start) + ' seconds to PRUNE ' + str(n) + ' candidates of size ' + str(k)))
        # print('Left with ' + str(len(list(candidate_hashmap.keys()))) + ' candidates')
    return candidate_hashmap


def add_candidate_to_hashmap(lst, candidate_hashmap):
    candidate_hashmap[hash_candidate(lst)] = lst




