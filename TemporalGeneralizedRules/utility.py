import time
from itertools import combinations


def maximal_time_period_interval(list_of_tuples, period1, period2):
    if period1 > period2 or len(list_of_tuples) < period2 or period1 < 0:
        print('ERROR GETTING MAXIMAL TIME PERIOD')
        return None

    pointer1 = period1
    pointer2 = period2
    lbTID = None
    ubTID = None

    while pointer1 <= pointer2 and (not (lbTID is not None and ubTID is not None)):
        if lbTID is None:
            if list_of_tuples[pointer1] is not None:
                lbTID = list_of_tuples[pointer1][0]
            else:
                pointer1 += 1
        if ubTID is None:
            if list_of_tuples[pointer2] is not None:
                ubTID = list_of_tuples[pointer2][1]
            else:
                pointer2 -= 1

    return [lbTID, ubTID]

def binary_search_or_first_higher_value(list, value, low, high):
    while low < high:

        mid = (low + high) // 2
        indexedValue = list[mid]
        if indexedValue == value:
            return mid
        elif indexedValue > value:
            high = mid - 1
        else:
            low = mid + 1

    if list[low] >= value:
        return low
    else:
        return low + 1


def findOrderedIntersection(arr1, arr2):
    """
        :param: two ordered int arrays
        :return: ordered int array
    """
    pointer_1 = 0
    pointer_2 = 0
    arr1_size = len(arr1)
    arr2_size = len(arr2)
    result_array = []
    while pointer_1 < arr1_size and pointer_2 < arr2_size:
        val_dif = arr1[pointer_1] - arr2[pointer_2]
        if val_dif == 0:
            result_array.append(arr1[pointer_1])
            pointer_1 += 1
            pointer_2 += 1
        elif val_dif > 0:
            pointer_2 += 1
        else:
            pointer_1 += 1

    return result_array

def getFilteredTIDSThatBelongToPeriod(arr, lb=None, ub=None):
    """
    :param arr: array to get sublist of
    :param lb: lowerbound value
    :param ub: upperbound value
    :return: sublist of arr delimeted between lb and ub
    """
    if lb is None or ub is None:
        return arr
    res = []

    #Pointer starts from first value apparition (or next), position found by binary search.
    pointer = binary_search_or_first_higher_value(arr, lb, 0, len(arr) - 1)
    while pointer < len(arr) and arr[pointer] <= ub:
        res.append(arr[pointer])
        pointer += 1

    return res


#Deprecated. Used for testing only.
def findOrderedIntersectionBetweenBoundaries(arr1, arr2, lb, ub):
    """
    :param: two ordered int arrays, lowerBoundary, upperBoundary
    :param: upper and lowerbound VALUES
    :return: ordered int array
    """
    cutted_arr_1 = getFilteredTIDSThatBelongToPeriod(arr1, lb, ub)
    cutted_arr_2 = getFilteredTIDSThatBelongToPeriod(arr2, lb, ub)
    if lb > ub:
        print('ERROR. UPPERBOUND IS NOT HIGHER OR EQUAL LOWERBOUND')
        return None
    arr1_size = len(cutted_arr_1)
    arr2_size = len(cutted_arr_2)
    pointer_1 = 0
    pointer_2 = 0
    result_array = []
    while pointer_1 < arr1_size and pointer_2 < arr2_size and cutted_arr_1[pointer_1] <= ub and cutted_arr_2[pointer_2] <= ub:
        val_dif = cutted_arr_1[pointer_1] - cutted_arr_2[pointer_2]
        if val_dif == 0:
            result_array.append(cutted_arr_1[pointer_1])
            pointer_1 += 1
            pointer_2 += 1
        elif val_dif > 0:
            pointer_2 += 1
        else:
            pointer_1 += 1
    return result_array


def getValidJoin(ordered_list_1, ordered_list_2):
    """
    :param ordered_list_1/2: An ORDERED set/list of itemsets of same length k-1 (both list should also be lexicographicly ordered: l1 < l2)
    :return: A new ordered list size k or None if not possible
    """
    if len(ordered_list_1) != len(ordered_list_2):
        print("LISTS HAVE DIFFERENT SIZES! UNABLE TO JOIN!")
        print(ordered_list_1)
        print(ordered_list_2)
        return None
    else:
        k = len(ordered_list_1)
        for i in range(k):
            if i != k - 1 and ordered_list_1[i] != ordered_list_2[i]:
                return None
            elif i == k - 1 and ordered_list_1[i] == ordered_list_2[i]:
                return None  # This may never happen since both lists must be exactly the same
        allItemsButLast = ordered_list_1.copy()
        allItemsButLast.append(ordered_list_2[k - 1])
        return allItemsButLast


def allSubsetofSizeKMinus1(a_candidate_of_size_k, k):
    return list(map(list, combinations(a_candidate_of_size_k, k)))


def stringifyPg(l_level, period):
    return str(l_level) + '-' + str(period)


def flatten_list(t):
    return [item for sublist in t for item in sublist]


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

    frequent_dictionary_k_1 = frequent_dictionary[k - 1]
    frequent_dictionary_k_1_hashed = set(map(tuple, frequent_dictionary_k_1))
    pruned_candidates = []
    for a_candidate_of_size_k in candidates_of_size_k:  # Iterating over a lists of lists
        subsets_of_size_k_minus_1 = allSubsetofSizeKMinus1(a_candidate_of_size_k,
                                                           k - 1)  # a_candidate_of_size_k is a list
        all_subsets_are_frequent = True
        for a_subset_of_size_k_minus_1 in subsets_of_size_k_minus_1:
            all_subsets_are_frequent = tuple(a_subset_of_size_k_minus_1) in frequent_dictionary_k_1_hashed
            if not all_subsets_are_frequent:
                break
        if all_subsets_are_frequent:
            pruned_candidates.append(a_candidate_of_size_k)

    return pruned_candidates


def generateCanidadtesOfSizeK(k, all_items, frequent_dictionary):
    if k == 1:
        return list(map(lambda x: [x], all_items))
    elif k == 2:
        return list(
            map(list, combinations(flatten_list(frequent_dictionary[1]), 2)))  # Treat k = 2 as a special case
    else:
        return apriori_gen(frequent_dictionary[k - 1], frequent_dictionary)


def hash_candidate(candidate):
    """
    Gets the tuple of the list of ints for hashing in dictionaries/sets
    :param candidate: a list of ints
    :return:
    """
    return tuple(candidate)


def create_ancestor_set(taxonomy):
    ancestor_set = set()
    for item in taxonomy:
        ancestors = taxonomy[item]
        for ancestor in ancestors:
            hashed_itemset = hash_candidate(sorted([item, ancestor]))
            ancestor_set.add(hashed_itemset)
    return ancestor_set


def calculate_support_for_parallel(candidate, candidate_tids):
    final_intersection = None
    for item in candidate_tids:
        tids = candidate_tids[item]
        if final_intersection is None:
            final_intersection = tids
        else:
            final_intersection = findOrderedIntersection(final_intersection, tids)
    return candidate, len(final_intersection)


def append_tids(candidate, items_dic, matrix_data_by_item):
    candidate_tids_dict = {}
    for item in candidate:
        item = items_dic[item]
        if item in matrix_data_by_item:
            candidate_tids_dict[item] = matrix_data_by_item[item]['tids']
        else:
            candidate_tids_dict[item] = []

    return candidate, candidate_tids_dict

def append_tids_for_HTAR(candidate, items_dic, matrix_data_by_item):
    candidate_tids_dict = {}
    for item in candidate:
        itemName = items_dic[item]
        if itemName in matrix_data_by_item:
            candidate_tids_dict[item] = matrix_data_by_item[itemName]['tids']
        else:
            candidate_tids_dict[item] = []

    return candidate_tids_dict

def calculate_tid_intersections_HTAR_with_boundaries(candidate, candidate_tids, tidLimits):
    start = time.time()
    lb = tidLimits[0]
    ub = tidLimits[1]
    final_intersection = None
    for item in candidate:
        tids = getFilteredTIDSThatBelongToPeriod(candidate_tids[item], lb, ub)
        if final_intersection is None:
            final_intersection = tids
        elif len(final_intersection) == 0 or len(tids) == 0:
            return candidate, 0
        else:
            final_intersection = findOrderedIntersection(final_intersection, tids)

    end = time.time()
    timeUsage = end-start
    return candidate, len(final_intersection), timeUsage

def calculate_tid_intersections_HTAR_with_boundaries_for_paralel(candidate, items_tids):
    start = time.time()
    final_intersection = None
    for item in candidate:
        tids = items_tids[item]
        if final_intersection is None:
            final_intersection = tids
        elif len(final_intersection) == 0 or len(tids) == 0:
            return candidate, 0
        else:
            final_intersection = findOrderedIntersection(final_intersection, tids)

    end = time.time()
    timeUsage = end-start
    return candidate, len(final_intersection), timeUsage

def create_minimal_items_dic(candidate, all_items_dic):
    res = {}
    for item in candidate:
        res[item] = all_items_dic[item]
    return candidate, res

def append_tids_for_HTAR_for_single_item(item, items_dic, matrix_data_by_item):
    itemName = items_dic[item]
    if itemName in matrix_data_by_item:
        return matrix_data_by_item[itemName]['tids']
    else:
        return []

