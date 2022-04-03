import itertools
import math
import multiprocessing
import time

from Cumulate.cumulate_utility import calculate_itemset_ancestor, close_ancestor
from DataStructures.AssociationRule import AssociationRule
from utility import hash_candidate


def rule_generation(frequent_dictionary, support_dictionary, min_confidence,
                    parallel_rule_gen=False, taxonomy=None, min_r=None, database=None):
    rules = []
    if parallel_rule_gen:
        all_frequents = []
        for key in frequent_dictionary:
            if key != 1:
                all_frequents.extend(frequent_dictionary[key])
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        if taxonomy is None:
            list_to_parallel = [append_data_for_rule(x, support_dictionary, min_confidence) for x in all_frequents]
            results = pool.starmap(calculate_rule_for_parallel, list_to_parallel)
        else:
            list_to_parallel = [
                append_data_for_generalized_rule(x, support_dictionary, database, taxonomy, min_confidence, min_r) for x
                in all_frequents]
            results = pool.starmap(calculate_generalized_rule_for_parallel, list_to_parallel)

        for result in results:
            rules.extend(result)
        pool.close()  # Close pools after using them
        pool.join()  # Main process waits after last pool closes
    else:
        for key in frequent_dictionary:
            if key != 1:
                frequent_itemset_k = frequent_dictionary[key]
                for a_itemset_k in frequent_itemset_k:
                    if taxonomy is None:
                        rules_from_itemset = create_rules(a_itemset_k, support_dictionary, min_confidence)
                    else:
                        rules_from_itemset = create_generalized_rules(a_itemset_k, support_dictionary, database,
                                                                      taxonomy, min_confidence, min_r)
                    rules.extend(rules_from_itemset)
    return rules


def create_rules(a_itemset, support_dictionary, min_conf):
    rules = []
    for idx, item in enumerate(a_itemset):

        if type(a_itemset) == tuple:
            a_itemset = list(a_itemset)

        consequent = [item]
        antecedent = [x for i, x in enumerate(a_itemset) if i != idx]
        support_antecedent = support_dictionary[hash_candidate(antecedent)]
        support_all_items = support_dictionary[hash_candidate(a_itemset)]
        confidence = support_all_items / support_antecedent
        if confidence >= min_conf:
            association_rule = AssociationRule(antecedent, consequent, support_all_items, confidence)
            rules.append(association_rule)
    return rules


def create_generalized_rules(a_itemset, support_dictionary, database, taxonomy, min_conf, min_r):
    rules = []
    for idx, item in enumerate(a_itemset):
        consequent = [item]
        antecedent = [x for i, x in enumerate(a_itemset) if i != idx]
        support_antecedent = support_dictionary[hash_candidate(antecedent)]
        support_all_items = support_dictionary[hash_candidate(a_itemset)]
        confidence = support_all_items / support_antecedent
        if confidence >= min_conf and support_all_items >= min_r * expected_value(min_r, a_itemset, support_dictionary,
                                                                                  taxonomy, database):
            association_rule = AssociationRule(antecedent, consequent, support_all_items, confidence)
            rules.append(association_rule)
    return rules


def expected_value(min_r, itemset, support_dictionary, taxonomy, database):
    """
    :param min_r: Minimum interesting measure
    :param itemset: And itemset Z = XUY that represents the rule X=>Y
    :param support_dictionary: Auxiliary structure to obtain the supports of individual items in the itemset
    :param taxonomy: Auxiliary structure to get the close ancestor of items
    :param database: Database (horizontal or vertical) to count the support of Z'
    :return: The expected value of itemset Z based on Z' where Z' is an ancestor of Z
    """
    if min_r == 0:
        return 0
    itemset_ancestor = calculate_itemset_ancestor(itemset, taxonomy)  # This is Z'
    product_list = []  # This is P(z_1) / P(z'_1) * ...
    for item in itemset:
        item_ancestor = close_ancestor(item, taxonomy)
        numerator = support_dictionary[
            hash_candidate([item])]  # P(z_j). support_dictionary has all supports of 1-itemsets
        denominator = support_dictionary[hash_candidate([item_ancestor])]  # P(z'_j)
        if denominator != 0:
            fraction = numerator / denominator
        elif numerator == 0 and denominator == 0:
            fraction = 0
        else:
            raise Exception("Division by zero in expected_value function")
        product_list.append(fraction)
    hashed_ancestor = hash_candidate(itemset_ancestor)
    if hashed_ancestor in support_dictionary:
        product_list.append(support_dictionary[hashed_ancestor])
    else:
        product_list.append(database.supportOf(itemset_ancestor))  # P(Z') needs to be calculated
    return math.prod(product_list)


def append_data_for_generalized_rule(itemset, support_dictionary, database, taxonomy, min_conf, min_r):
    support_dictionary_for_itemset = {}  # Build a smaller dictionary only containing the necessary data
    support_dictionary_for_itemset[hash_candidate(itemset)] = support_dictionary[hash_candidate(itemset)]
    close_ancestor_dict = {}
    for idx, item in enumerate(itemset):
        item_ancestor = close_ancestor(item, taxonomy)
        close_ancestor_dict[hash_candidate([item])] = item_ancestor
        antecedent = [x for i, x in enumerate(itemset) if i != idx]
        support_dictionary_for_itemset[hash_candidate(antecedent)] = support_dictionary[hash_candidate(antecedent)]
        support_dictionary_for_itemset[hash_candidate([item])] = support_dictionary[
            hash_candidate([item])]  # Save support of every singular item
        support_dictionary_for_itemset[hash_candidate([item_ancestor])] = support_dictionary[
            hash_candidate([item_ancestor])]  # Save support of every ancestor of itemset

    itemset_ancestor = calculate_itemset_ancestor(itemset, taxonomy)
    hashed_ancestor = hash_candidate(itemset_ancestor)
    if hashed_ancestor in support_dictionary:
        support_dictionary_for_itemset[hashed_ancestor] = support_dictionary[
            hashed_ancestor]  # Save support of ancestor itemset if previously calculated

    support_of_ancestor = database.supportOf(itemset_ancestor)  # Also save support of ancestor

    return itemset, support_dictionary_for_itemset, itemset_ancestor, support_of_ancestor, close_ancestor_dict, min_conf, min_r


def calculate_generalized_rule_for_parallel(itemset, support_dictionary_for_itemset, itemset_ancestor,
                                            support_of_ancestor, close_ancestor_dict, min_conf, min_r):
    rules = []
    for idx, item in enumerate(itemset):
        consequent = [item]
        antecedent = [x for i, x in enumerate(itemset) if i != idx]
        support_antecedent = support_dictionary_for_itemset[hash_candidate(antecedent)]
        support_all_items = support_dictionary_for_itemset[hash_candidate(itemset)]
        confidence = support_all_items / support_antecedent
        if confidence >= min_conf and support_all_items >= min_r * calculate_expected_value_for_parallel(itemset,
                                                                                                         support_dictionary_for_itemset,
                                                                                                         itemset_ancestor,
                                                                                                         support_of_ancestor,
                                                                                                         close_ancestor_dict,
                                                                                                         min_r):
            association_rule = AssociationRule(antecedent, consequent, support_all_items, confidence)
            rules.append(association_rule)
    return rules


def calculate_expected_value_for_parallel(itemset, support_dictionary_for_itemset, itemset_ancestor,
                                          support_of_ancestor, close_ancestor_dict, min_r):
    if min_r == 0:
        return 0
    product_list = []  # This is P(z_1) / P(z'_1) * ...
    for item in itemset:
        item_ancestor = close_ancestor_dict[hash_candidate([item])]
        numerator = support_dictionary_for_itemset[
            hash_candidate([item])]  # P(z_j). support_dictionary has all supports of 1-itemsets
        denominator = support_dictionary_for_itemset[hash_candidate(item_ancestor)]  # P(z'_j)
        if denominator != 0:
            fraction = numerator / denominator
        elif numerator == 0 and denominator == 0:
            fraction = 0
        else:
            raise Exception("Division by zero in expected_value function")
        product_list.append(fraction)
    hashed_ancestor = hash_candidate(itemset_ancestor)
    if hashed_ancestor in support_dictionary_for_itemset:
        product_list.append(support_dictionary_for_itemset[hashed_ancestor])
    else:
        product_list.append(support_of_ancestor)  # P(Z') needs to be calculated
    return math.prod(product_list)


def append_data_for_rule(itemset, support_dictionary, min_conf):
    support_dictionary_for_itemset = {}  # Build a smaller dictionary only containing the necessary data
    support_dictionary_for_itemset[hash_candidate(itemset)] = support_dictionary[hash_candidate(itemset)]
    for idx, item in enumerate(itemset):
        antecedent = [x for i, x in enumerate(itemset) if i != idx]
        support_dictionary_for_itemset[hash_candidate(antecedent)] = support_dictionary[hash_candidate(antecedent)]
        support_dictionary_for_itemset[hash_candidate([item])] = support_dictionary[
            hash_candidate([item])]  # Save support of every singular item
    return itemset, support_dictionary_for_itemset, min_conf


def calculate_rule_for_parallel(itemset, support_dictionary_for_itemset, min_conf):
    rules = []
    for idx, item in enumerate(itemset):

        if type(itemset) == tuple:
            itemset = list(itemset)

        consequent = [item]
        antecedent = [x for i, x in enumerate(itemset) if i != idx]
        support_antecedent = support_dictionary_for_itemset[hash_candidate(antecedent)]
        support_all_items = support_dictionary_for_itemset[hash_candidate(itemset)]
        confidence = support_all_items / support_antecedent
        if confidence >= min_conf:
            association_rule = AssociationRule(antecedent, consequent, support_all_items, confidence)
            rules.append(association_rule)
    return rules
