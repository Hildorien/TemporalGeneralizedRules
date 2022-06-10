from datetime import datetime


def getFortnight(day, month):
    firstHalf = day < 16
    fortnight = month * 2
    if firstHalf:
        return fortnight - 1
    else:
        return fortnight

def getPeriodStampFromTimestamp(timestamp):
    # Hierarchy of Time Granules is represented by an array of numbers, where each column represents the l-level and
    # the number the max. amount of periods within each level. For the purpose of this thesis, HTG will be [24,12,
    # 4] which stands for 24 possible fortnights, 12 months and 4 quarters. Notice how the dates 25/3/95 and 25/3/15
    # will have the same periodStamp, since the year is omitted. Last "1" represents the entire HTG.

    dt = datetime.fromtimestamp(timestamp)
    return [getFortnight(dt.day, dt.month), dt.month, ((dt.month // 4) + 1), 1]


def getPeriodStampFromTimestampHONG(timestamp):
    # Only for HTG = [10,5,1]
    fromDate = 852076800 # 1/1/1997
    untilDate = 883612800 # 1/1/1998 (assuming they only use one year), else use 1/1/1999(915148800)
    totalWidth = untilDate - fromDate
    basketSize = totalWidth // 10
    leafbasket = (timestamp - fromDate) // basketSize
    upperLeaf = leafbasket // 2
    return [leafbasket + 1, upperLeaf + 1, 1]



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
        for p_i in range(bounderies[0], bounderies[1] + 1):
            if p_i in TFI_by_period and k in TFI_by_period[p_i]:
                finalTFI_k = set.union(finalTFI_k, TFI_by_period[p_i][k])

        if len(finalTFI_k) > 0:
            finalTFI[k] = finalTFI_k
            new_items_found = True
        k += 1

    return finalTFI

def getPeriodsIncluded(l_length, period):
    # Note: This method does only works with hardcoded HTG = [24,12,4, 1]
    if l_length == 0:
        return [period, period]
    elif l_length == 1:
        later_fortnight = period * 2
        return [later_fortnight - 1, later_fortnight]
    elif l_length == 2:
        later_fortnight = period * 6
        return [later_fortnight - 5, later_fortnight]
    elif l_length == 3:
        return [1, 24]
    else:
        print("ERROR: L_LENGTH NOT AVAILABLE FOR DEFAULT HTG")
