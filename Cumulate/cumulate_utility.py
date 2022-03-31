import itertools
import time

from utility import flatten, hash_candidate, getValidJoin, allSubsetofSizeKMinus1


def close_ancestor(item, taxonomy):
    """
    :param item:
    :param taxonomy:
    :return: Given an item returns the first ancestor i.e the closest in the family
    """
    if is_eldest(item, taxonomy):
        return item
    else:
        return taxonomy[item][0]


def is_eldest(item, taxonomy):
    return taxonomy[item] == []


def calculate_itemset_ancestor(itemset, taxonomy):
    """
    :param itemset: List of ints representing items
    :param taxonomy: Dictionary of <int, [int]> representing taxonomy of items
    :return: We call Z' an ancestor of Z if we can get Z' from Z by replacing one or more items in Z
    with their ancestors and Z and Z have the same number of items.

    E.g.: itemset = {x, y}
    x' is a close ancestor of x
    y' is a close ancestor of y
    {x',y'} is a close ancestor of {x, y}
    """
    itemset_ancestor = []
    for item in itemset:
        ancestor = close_ancestor(item, taxonomy)  # returns same item if item has no ancestor
        itemset_ancestor.append(ancestor)
    return sorted(list(set(itemset_ancestor))) # Remove duplicate ancestors in case taxonomy has parents repeated across levels


def calculate_support(a_candidate, database):
    return a_candidate, database.supportOf(a_candidate)


def calculate_Ck(frequent_dictionary, k):
    candidate_hashmap = apriori_gen(frequent_dictionary[k - 1], frequent_dictionary)
    return candidate_hashmap


def calculate_C2(frequent_dictionary, k):
    candidate_hashmap = {}
    [add_candidate_to_hashmap(list(x), candidate_hashmap) for x in
     itertools.combinations(flatten(frequent_dictionary[1]), 2)]
    return candidate_hashmap


def calculate_C1(all_items, k):
    candidate_hashmap = {}
    [add_candidate_to_hashmap(list(x), candidate_hashmap) for x in list(map(lambda x: [x], all_items))]
    return candidate_hashmap


def prune_candidates_in_same_family(candidates_size_k, taxonomy):
    candidate_hashmap = set(map(tuple, candidates_size_k))
    ancestor_set = set()
    new_candidates = []
    for item in taxonomy:
        ancestors = taxonomy[item]
        for ancestor in ancestors:
            hashed_itemset = hash_candidate(sorted([item, ancestor]))
            ancestor_set.add(hashed_itemset)
    return sorted(list(map(list, candidate_hashmap.difference(ancestor_set))))


def count_candidates_in_transaction(k, expanded_transaction, support_dictionary, candidate_hashmap):
    """
    Given an expanded transaction, increments the count of all candidates in candidates_size_k that are contained in expanded_transaction
    :param k: Number of iteration
    :param expanded_transaction: a list of ints representing a list of items
    :param support_dictionary: { 'hashed_candidate': support_count }
    :param candidate_hashmap: { 'hashed_candidate': candidate }
    :return:
    """
    if k < 4:  # If k is small candidate size is large
        all_subsets = itertools.combinations(expanded_transaction, k)
        for subset in all_subsets:
            if subset in candidate_hashmap:
                support_dictionary[subset] += 1
    else:  # If k is large candidates size is smaller
        for hashed_candidate in candidate_hashmap:
            if set(hashed_candidate).issubset(set(expanded_transaction)):
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
    for i in range(n):
        j = i + 1
        while j < n:
            joined_itemset = getValidJoin(frequent_itemset_of_size_k_minus_1[i], frequent_itemset_of_size_k_minus_1[j])
            if joined_itemset is None:
                break
            else:
                candidate_hashmap[hash_candidate(joined_itemset)] = joined_itemset
            j += 1
    # STEP 2: PRUNE -> For each itemset in L_k check if all subsets are frequent
    if k > 2:
        candidates_size_k = list(candidate_hashmap.values()).copy()
        n = len(candidates_size_k)
        for i in range(n):
            subsets_of_size_k_minus_1 = allSubsetofSizeKMinus1(candidates_size_k[i], k - 1)  # candidates_size_k[i] is a list
            frequent_dictionary_k_1 = frequent_dictionary[k - 1]
            frequent_dictionary_k_1_hashed = set(map(tuple, frequent_dictionary_k_1))
            for a_subset_of_size_k_minus_1 in subsets_of_size_k_minus_1:
                if not (tuple(a_subset_of_size_k_minus_1) in frequent_dictionary_k_1_hashed):
                    candidate_hashmap.pop(hash_candidate(candidates_size_k[i]), None)  # Prunes the entire candidate
                    break
    return candidate_hashmap


def add_candidate_to_hashmap(lst, candidate_hashmap):
    candidate_hashmap[hash_candidate(lst)] = lst
