import time
from itertools import combinations
from utility import flatten, allSubsetofSizeKMinus1, getValidJoin


def cumulate(horizontal_database, min_supp, min_conf, min_r):
    """
    :param horizontal_database: A horizontal database
    :param min_supp: User-defined minimum support
    :param min_conf: User-defined minimum confidence
    :param min_r: User-defined minimum R insteresting measure
    :return: Set of association rules
    """
    frequent_dictionary = {}
    # Calculate L1
    frequent_dictionary[1] = calculate_L1(horizontal_database, min_supp)
    k = 2
    # Instantiate local variables
    transactions = horizontal_database.transactions
    transaction_count = horizontal_database.transaction_count
    taxonomy = horizontal_database.taxonomy

    while frequent_dictionary[k - 1] != []:
        # Candidate Generation
        print('Iteration ' + str(k))
        print('Candidate generation step...')
        candidate_hashmap = {}
        if k == 2:
            start = time.time()
            [hash_candidate(list(x), candidate_hashmap) for x in combinations(flatten(frequent_dictionary[1]), 2)]
            end = time.time()
            print('Took ' + (str(end - start) + ' seconds to generate ' + str(len(list(candidate_hashmap.keys()))) +  ' candidates of size ' + str(k) + ' using combinations'))
        else:
            start = time.time()
            candidate_hashmap = apriori_gen(frequent_dictionary[k - 1], frequent_dictionary)
            end = time.time()
            print('Took ' + (str(end - start) + ' seconds to generate ' + str(len(list(candidate_hashmap.keys()))) + ' candidates of size ' + str(k) + ' using apriori-gen'))
        if k == 2:
            prune_candidates_in_same_family(candidate_hashmap, taxonomy)
        candidates_size_k = list(candidate_hashmap.values())

        print('Pruning taxonomy...')
        start = time.time()
        taxonomy_pruned = prune_ancestors(candidates_size_k, taxonomy)
        end = time.time()
        print('Took ' + (str(end - start) + ' seconds to prune taxonomy'))
        # Count Candidates of size k in transactions
        print('Counting candidates step...')
        frequent_dictionary[k] = []
        support_dictionary = dict.fromkeys(candidate_hashmap.keys(), 0) #Auxiliary structure for counting supports of size k
        start = time.time()
        for a_transaction in transactions:
            # print('Expanding transaction smartly...')
            # start = time.time()
            expanded_transaction = expand_transaction(a_transaction, taxonomy_pruned)
            # end = time.time()
            # print('Took ' + (str(end - start) + ' seconds to expand transaction'))
            count_candidates_in_transaction(k, expanded_transaction, support_dictionary, candidate_hashmap)
        end = time.time()
        print('Took ' + (str(end - start) + ' seconds to count candidates of size ' + str(k) + ' in transactions'))
        print('Frequent generation step...')
        start = time.time()
        # Frequent itemset generation of size k
        for hashed_itemset in support_dictionary:
            support = support_dictionary[hashed_itemset] / transaction_count
            if support >= min_supp:
                frequent_dictionary[k].append(candidate_hashmap[hashed_itemset])
        end = time.time()
        print('Took ' + (str(end - start) + ' seconds to generate ' + str(len(frequent_dictionary[k])) + ' frequents of size ' + str(k)))
        print('--------------------------------------------------------------------------------')
        k += 1
    return frequent_dictionary


def calculate_L1(horizontal_database, min_supp):
    """
    :param horizontal_database: A horizontal database
    :param min_supp: User-specified minimum support
    :return: A list of list of frequent items [ ['Apple'], ['Carrot'], ...]
    """
    support_dictionary = {}
    frequent_items = []
    transactions = horizontal_database.transactions
    transaction_count = horizontal_database.transaction_count
    taxonomy = horizontal_database.taxonomy

    for a_transaction in transactions:
        expanded_transaction = expand_transaction(a_transaction, taxonomy)
        # Work with expanded_transaction
        for item in set(expanded_transaction):
            if item in support_dictionary:
                support_dictionary[item] += 1
            else:
                support_dictionary[item] = 1

    for item in support_dictionary:
        support = support_dictionary[item] / transaction_count
        if support >= min_supp:
            frequent_items.append([item])
    return frequent_items


def expand_transaction(a_transaction, taxonomy):
    expanded_transaction = []
    for an_item in a_transaction.items:
        expanded_transaction.append(an_item)
        # Append ancestors of item to expanded_transaction
        if an_item in taxonomy:
            ancestors = taxonomy[an_item]
            for ancestor in ancestors:
                if ancestor not in expanded_transaction:
                    expanded_transaction.append(ancestor)
    return expanded_transaction


def prune_candidates_in_same_family(candidate_hashmap, taxonomy):
    for item in taxonomy:
        for ancestor in taxonomy[item]:
            hashed_itemset = str([item, ancestor])
            if hashed_itemset in candidate_hashmap:
                candidate_hashmap.pop(hashed_itemset)


def count_candidates_in_transaction(k, expanded_transaction, support_dictionary, candidate_hashmap):
    """
    Given an expanded transaction, increments the count of all candidates in candidates_size_k that are contained in expanded_transaction
    :param k: Number of iteration
    :param expanded_transaction: a list of strings representing a list of items
    :param support_dictionary: { 'hashed_candidate': support_count }
    :param candidate_hashmap: { 'hashed_candidate': candidate }
    :return:
    """
    all_subets = list(map(list, combinations(expanded_transaction, k)))
    for a_subset_size_k in all_subets:
        hashed_subset = str(a_subset_size_k)
        if hashed_subset in candidate_hashmap:
            support_dictionary[hashed_subset] += 1


def is_in_transaction(a_candidate, expanded_transaction):
    """
    Check if a candidate is in the list of items expanded_transaction
    :param a_candidate: a list of string representing candidate items
    :param expanded_transaction: list of strings representing items
    :return:
    """
    return set(a_candidate).issubset(set(expanded_transaction))


def prune_ancestors(candidates_size_k, taxonomy):
    taxonomy_pruned = {}
    for a_candidate in candidates_size_k:
        for an_item in a_candidate:
            if an_item in taxonomy:
                taxonomy_pruned[an_item] = taxonomy[an_item]
    return taxonomy_pruned


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
    for i in range(n):
        j = i + 1
        while j < n:
            joined_itemset = getValidJoin(frequent_itemset_of_size_k_minus_1[i], frequent_itemset_of_size_k_minus_1[j])
            if joined_itemset is None:
                break
            else:
                candidate_hashmap[str(joined_itemset)] = joined_itemset
            j += 1
    # STEP 2: PRUNE -> For each itemset in L_k check if all subsets are frequent
    if k > 2:
        for a_candidate_of_size_k in list(candidate_hashmap.values()):  # Iterating over a lists of lists
            subsets_of_size_k_minus_1 = allSubsetofSizeKMinus1(a_candidate_of_size_k,
                                                               k - 1)  # a_candidate_of_size_k is a list
            for a_subset_of_size_k_minus_1 in subsets_of_size_k_minus_1:
                if not (a_subset_of_size_k_minus_1 in frequent_dictionary[k - 1]):
                    candidate_hashmap.pop(str(a_candidate_of_size_k), None)  # Prunes the entire candidate
                    break
    return candidate_hashmap


def hash_candidate(list, candidate_hashmap):
    candidate_hashmap[str(list)] = list
