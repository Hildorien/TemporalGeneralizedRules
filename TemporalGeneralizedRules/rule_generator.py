import math
import multiprocessing
import os
from .cumulate_utility import calculate_itemset_ancestor, close_ancestor
from .AssociationRule import AssociationRule
from .utility import hash_candidate


def rule_generation(frequent_dictionary, support_dictionary, min_confidence,
                    parallel_rule_gen=False, min_r=None, database=None, pg=None):
    rules = []
    potential_rules_dict = {}
    if min_r is None:
        min_r = 0
    frequent_itemset_values = list(frequent_dictionary.values())
    frequent_itemsets = [frequent_itemset_values[i] for i in range(1, len(frequent_itemset_values))]
    [explode_frequent_itemset_in_potential_rule(itemset, support_dictionary, min_confidence, potential_rules_dict) for
     sublist in frequent_itemsets for itemset in sublist]
    if parallel_rule_gen:
        pool = multiprocessing.Pool(os.cpu_count())
        list_to_parallel = [(key, potential_rules_dict[key][3], potential_rules_dict[key][4]) for key in
                            potential_rules_dict]
        results = pool.starmap(check_rule_for_parallel, list_to_parallel)
        rules = [generate_rule_for_parallel(r, potential_rules_dict) for r in results if r is not None]
        pool.close()  # Close pools after using them
        pool.join()  # Main process waits after last pool closes
        if database is not None and database.taxonomy is not None:
            rules = filter_interesting_rules(rules, min_confidence, min_r, support_dictionary, database)
    else:
        for key in potential_rules_dict:
            antecedent, consequent, rule_support, rule_confidence, min_conf = potential_rules_dict[key]  # Unpack data
            if database is not None and database.taxonomy is not None:
                itemset = sorted(antecedent + consequent)  # Reconstruct itemset
                if rule_confidence >= min_conf and rule_support >= min_r * expected_value(min_r, itemset,
                                                                                          support_dictionary,
                                                                                          database.taxonomy,
                                                                                          database, pg):
                    rules.append(AssociationRule(antecedent, consequent, rule_support, rule_confidence))
            else:
                if rule_confidence >= min_conf:
                    rules.append(AssociationRule(antecedent, consequent, rule_support, rule_confidence))
    return rules


def explode_frequent_itemset_in_potential_rule(frequent_itemset, support_dictionary, min_conf, potential_rule_dict):
    """
    Given a frequent_itemset explodes in n potential rules and stores it in potential_rule_dict.
    Eg: {A,B,C} -> potential_rules { {AB=>C} , {AC=>B}, {BC=>A} }
    potential_rule_dict[hash(AB=>C)] = (antecedent: AB, consequent: C, support_all_items: sup(ABC), confidence: conf(AB=>C), min_conf)
    :param frequent_itemset:
    :param support_dictionary:
    :param min_conf:
    :param potential_rule_dict:
    :return:
    """
    for idx, item in enumerate(frequent_itemset):
        consequent = tuple([item])
        antecedent = tuple([x for i, x in enumerate(frequent_itemset) if i != idx])
        support_antecedent = support_dictionary[hash_candidate(antecedent)]
        support_all_items = support_dictionary[hash_candidate(frequent_itemset)]
        confidence = support_all_items / support_antecedent
        rule_data = (antecedent, consequent, support_all_items, confidence, min_conf)
        hashed_rule = hash((rule_data[0], rule_data[1]))
        potential_rule_dict[hashed_rule] = rule_data


def expected_value(min_r, itemset, support_dictionary, taxonomy, database, pg=None):
    """
    :param min_r: Minimum interesting measure
    :param itemset: And itemset Z = XUY that represents the rule X=>Y
    :param support_dictionary: Auxiliary structure to obtain the supports of individual items in the itemset
    :param taxonomy: Auxiliary structure to get the close ancestor of items
    :param database: Database (horizontal or vertical) to count the support of Z'
    :param pg: String that represents the time granule
    :return: The expected value of itemset Z based on Z' where Z' is an ancestor of Z
    """
    if min_r == 0:
        return 0
    itemset_ancestor = calculate_itemset_ancestor(itemset, taxonomy)  # This is Z'

    hashed_ancestor = hash_candidate(itemset_ancestor)
    if hashed_ancestor == hash_candidate(itemset):  # Edge Case: Itemset has no ancestor, therefore is interesting
        return 0

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

    if hashed_ancestor in support_dictionary:
        product_list.append(support_dictionary[hashed_ancestor])
    else:
        if pg is None:
            product_list.append(database.supportOf(itemset_ancestor))  # P(Z') needs to be calculated
        else:
            l_level, period = pg.split('-')
            product_list.append(database.supportOf(itemset_ancestor, int(l_level), int(period)))
    return math.prod(product_list)


def check_rule_for_parallel(key, potential_rule_confidence, min_conf):
    if potential_rule_confidence >= min_conf:
        return key


def filter_interesting_rules(rules, min_confidence, min_r, support_dictionary, database):
    interesting_rules = []
    for rule in rules:
        itemset = sorted(list(rule.lhs) + list(rule.rhs))  # Reconstruct itemset
        if rule.confidence >= min_confidence and rule.support >= min_r * expected_value(min_r, itemset,
                                                                                        support_dictionary,
                                                                                        database.taxonomy,
                                                                                        database):
            interesting_rules.append(rule)
    return interesting_rules


def generate_rule_for_parallel(key, dict):
    antecedent, consequent, support_all_items, confidence, min_conf = dict[key]  # Unpack data
    return AssociationRule(antecedent, consequent, support_all_items, confidence)
