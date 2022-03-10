
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
    return sorted(itemset_ancestor)