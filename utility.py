from itertools import combinations
from datetime import datetime


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


def joinlistOfInts(list):
    return ','.join(str(x) for x in list)


def flatten(t):
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
    for a_candidate_of_size_k in candidates_of_size_k:  # Iterating over a lists of lists
        subsets_of_size_k_minus_1 = allSubsetofSizeKMinus1(a_candidate_of_size_k, k - 1)  # a_candidate_of_size_k is a list
        for a_subset_of_size_k_minus_1 in subsets_of_size_k_minus_1:
            if not (a_subset_of_size_k_minus_1 in frequent_dictionary[k - 1]):
                candidates_of_size_k.remove(a_candidate_of_size_k)  # Prunes the entire candidate
                break

    return candidates_of_size_k

def generateCanidadtesOfSizeK(k, all_items, frequent_dictionary):
    if k == 1:
        return list(map(lambda x: [x], all_items))
    elif k == 2:
       return list(
            map(list, combinations(flatten(frequent_dictionary[1]), 2)))  # Treat k = 2 as a special case
    else:
        return apriori_gen(frequent_dictionary[k - 1], frequent_dictionary)

def getFortnight(day, month):
    firstHalf = day < 16
    fortnight = month * 2
    if (firstHalf):
        return fortnight - 1
    else:
        return fortnight

def getPeriodsIncluded(l_length, period):
    # Note: This method does only works for any period in level above 0 with hardcoded HTG = [24,12,4]
    if l_length == 1:
        later_fortnight = period * 2
        return [later_fortnight-1, later_fortnight]
    elif l_length == 2:
        later_fortnight = period*6
        return[later_fortnight - 5, later_fortnight]
    else:
        print("ERROR: L_LENGTH NOT AVAILABLE FOR DEFAULT HTG")

def getTFIUnion(TFI_by_period, bounderies):
    """
    :param TFI_by_period: A dictionary of TFI's indexed by period
    :param bounderies: the lower and upper boundery of periods involved in the final merge.
    :return:
    """
    finalTFI = {}
    new_items_found = True
    k = 1
    while new_items_found:
        new_items_found = False
        finalTFI_k = set()
        for p_i in range(bounderies[0], bounderies[1]+1):
            if p_i in TFI_by_period and k in TFI_by_period[p_i]:
                finalTFI_k = set.union(finalTFI_k, TFI_by_period[p_i][k])

        if len(finalTFI_k) > 0:
            finalTFI[k] = finalTFI_k
            new_items_found = True
        k+=1

    return finalTFI


def getPeriodStampFromTimestamp(timestamp):
    # Hierarchy of Time Granules is represented by an array of numbers, where each column represents the l-level and
    # the number the max. amount of periods within each level. For the purpose of this thesis, HTG will be [24,12,
    # 4] which stands for 24 possible fortnights, 12 months and 4 quarters. Notice how the dates 25/3/95 and 25/3/15
    # will have the same periodStamp, since the year is omitted.

    dt = datetime.fromtimestamp(timestamp)
    return [getFortnight(dt.day, dt.month), dt.month, ((dt.month // 4) + 1)]


# This method gets the maximal time period between all HTG with their respective l-length
def getMTPFromHTGArrays(faps):
    l_length = 3  # Hardcoded HTG length
    res = []
    for i in range(l_length):
        lth_level_values = map(lambda fap: fap[i], faps)
        max_period = max(lth_level_values)
        res.append(max_period)
    return res

#TODO: After LAP is implemented
#def getMTPFromFAPandLAP