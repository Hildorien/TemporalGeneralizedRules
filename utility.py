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
            if i != k-1 and ordered_list_1[i] != ordered_list_2[i]:
                return None
            elif i == k-1 and ordered_list_1[i] == ordered_list_2[i]:
                return None #This may never happen since both lists must be exactly the same
        allItemsButLast = ordered_list_1.copy()
        allItemsButLast.append(ordered_list_2[k - 1])
        return allItemsButLast

def allSubsetofSizeKMinus1(a_candidate_of_size_k, k):
    return list(map(list, combinations(a_candidate_of_size_k, k)))

def joinlistOfInts(list):
    return ','.join(str(x) for x in list)

def flatten(t):
    return [item for sublist in t for item in sublist]

def getFortnight(day,month):
    firstHalf = day < 16
    fortnight = month*2
    if(firstHalf):
        return fortnight - 1
    else:
        return fortnight

def getPeriodStampFromTimestamp(timestamp):
    #Hierarchy of Time Granules is represented by an array of numbers, where each column represents the l-level and the number the max. amount of periods within each level.
    #For the purpose of this thesis, HTG will be [24,12,4] which stands for 24 possible fortnights, 12 months and 4 quarters.
    #Notice how the dates 25/3/95 and 25/3/15 will have the same periodStamp, since the year is omitted.

    dt = datetime.fromtimestamp(timestamp)
    return [getFortnight(dt.day, dt.month), dt.month, ((dt.month//4)+1)]
