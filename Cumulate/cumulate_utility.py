import itertools


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


def prune_candidates_in_same_family(candidates_size_k, ancestor_set):
    candidate_hashmap = set(map(tuple, candidates_size_k))
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

