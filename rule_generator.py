import itertools
import math
import multiprocessing

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
            results = pool.starmap(create_rules, zip(all_frequents, itertools.repeat(support_dictionary),
                                                     len(all_frequents) * [min_confidence]))
        else:
            results = pool.starmap(create_generalized_rules,
                                   zip(all_frequents, itertools.repeat(support_dictionary), itertools.repeat(database),
                                       itertools.repeat(taxonomy),
                                       len(all_frequents) * [min_confidence],
                                       len(all_frequents) * [min_r]))

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

        frequent_itemset_copy = a_itemset.copy()
        consequent = [frequent_itemset_copy.pop(idx)]
        antecedent = frequent_itemset_copy
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
        frequent_itemset_copy = a_itemset.copy()
        consequent = [frequent_itemset_copy.pop(idx)]
        antecedent = frequent_itemset_copy
        support_antecedent = support_dictionary[hash_candidate(antecedent)]
        support_all_items = support_dictionary[hash_candidate(a_itemset)]
        confidence = support_all_items / support_antecedent
        if confidence >= min_conf and support_all_items >= min_r * expected_value(a_itemset, support_dictionary,
                                                                                  taxonomy, database):
            association_rule = AssociationRule(antecedent, consequent, support_all_items, confidence)
            rules.append(association_rule)
    return rules


def expected_value(itemset, support_dictionary, taxonomy, database):
    """
    :param itemset: And itemset Z = XUY that represents the rule X=>Y
    :param support_dictionary: Auxiliary structure to obtain the supports of individual items in the itemset
    :param taxonomy: Auxiliary structure to get the close ancestor of items
    :param database: Database (horizontal or vertical) to count the support of Z'
    :return: The expected value of itemset Z based on Z' where Z' is an ancestor of Z
    """
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
