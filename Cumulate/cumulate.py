import time
from itertools import combinations

from DataStructures.AssociationRule import AssociationRule
from utility import flatten, allSubsetofSizeKMinus1, getValidJoin


def cumulate(horizontal_database, min_supp, min_conf, min_r):
    """
    :param horizontal_database: A horizontal database
    :param min_supp: User-defined minimum support
    :param min_conf: User-defined minimum confidence
    :param min_r: User-defined minimum R insteresting measure
    :return: Set of association rules
    """
    #Instantiate local variables
    frequent_dictionary = {}
    k = 1
    transactions = horizontal_database.transactions
    transaction_count = horizontal_database.transaction_count
    taxonomy = horizontal_database.taxonomy #Cumulate Optimization 2 in original paper
    all_items = sorted(list(horizontal_database.items_dic.keys()))
    frequent_support = {} #Auxiliary structure for rule-generation
    while (k == 1 or frequent_dictionary[k - 1] != []):
        # Candidate Generation
        print('Iteration ' + str(k))
        print('Candidate generation step...')
        if k == 1:
            candidate_hashmap = calculate_C1(all_items, k)
        elif k == 2:
            candidate_hashmap = calculate_C2(frequent_dictionary, k)
        else:
            candidate_hashmap = calculate_Ck(frequent_dictionary, k)
        if k == 2:
            prune_candidates_in_same_family(candidate_hashmap, taxonomy) #Cumulate Optimization 3 in original paper
        candidates_size_k = list(candidate_hashmap.values())
        frequent_dictionary[k] = []
        if len(candidates_size_k) == 0:
            break
        print('Pruning taxonomy...')
        start = time.time()
        taxonomy_pruned = prune_ancestors(candidates_size_k, taxonomy) #Cumulate Optimization 1 in original paper
        end = time.time()
        print('Took ' + (str(end - start) + ' seconds to prune taxonomy'))
        support_dictionary = dict.fromkeys(candidate_hashmap.keys(), 0) #Auxiliary structure for counting supports of size k
        # Count Candidates of size k in transactions
        print('Counting candidates step...')
        start = time.time()
        for a_transaction in transactions:
            expanded_transaction = expand_transaction(a_transaction, taxonomy_pruned)
            count_candidates_in_transaction(k, expanded_transaction, support_dictionary, candidate_hashmap)
        end = time.time()
        print('Took ' + (str(end - start) + ' seconds to count candidates of size ' + str(k) + ' in transactions'))
        print('Frequent generation step...')
        start = time.time()
        # Frequent itemset generation of size k
        for hashed_itemset in support_dictionary:
            support = support_dictionary[hashed_itemset] / transaction_count
            if support >= min_supp:
                frequent_dictionary[k].append(candidate_hashmap[hashed_itemset])
                frequent_support[hashed_itemset] = support
        end = time.time()
        print('Took ' + (str(end - start) + ' seconds to generate ' + str(len(frequent_dictionary[k])) + ' frequents of size ' + str(k)))
        print('--------------------------------------------------------------------------------')
        k += 1
    #Generate Rules
    rules = rule_generation(frequent_dictionary, frequent_support, min_conf)
    return rules


def calculate_Ck(frequent_dictionary, k):
    start = time.time()
    candidate_hashmap = apriori_gen(frequent_dictionary[k - 1], frequent_dictionary)
    end = time.time()
    print('Took ' + (str(end - start) + ' seconds to generate ' + str(
        len(list(candidate_hashmap.keys()))) + ' candidates of size ' + str(k) + ' using apriori-gen'))
    return candidate_hashmap


def calculate_C2(frequent_dictionary, k):
    candidate_hashmap = {}
    start = time.time()
    [add_candidate_to_hashmap(list(x), candidate_hashmap) for x in combinations(flatten(frequent_dictionary[1]), 2)]
    end = time.time()
    print('Took ' + (str(end - start) + ' seconds to generate ' + str(
        len(list(candidate_hashmap.keys()))) + ' candidates of size ' + str(k) + ' using combinations'))
    return candidate_hashmap


def calculate_C1(all_items, k):
    candidate_hashmap = {}
    start = time.time()
    [add_candidate_to_hashmap(list(x), candidate_hashmap) for x in list(map(lambda x: [x], all_items))]
    end = time.time()
    print('Took ' + (str(end - start) + ' seconds to generate ' + str(
        len(list(candidate_hashmap.keys()))) + ' candidates of size ' + str(k)))
    return candidate_hashmap


def expand_transaction(a_transaction, taxonomy):
    expanded_transaction = []
    for an_item in a_transaction.items:
        expanded_transaction.append(an_item)
        # Append ancestors of item to expanded_transaction
        if an_item in taxonomy:
            ancestors = taxonomy[an_item]
            for ancestor in ancestors:
                if ancestor not in expanded_transaction:
                    expanded_transaction.append(ancestor)
    return sorted(expanded_transaction)




def prune_candidates_in_same_family(candidate_hashmap, taxonomy):
    for item in taxonomy:
        for ancestor in taxonomy[item]:
            hashed_itemset_1 = hash_candidate([item, ancestor])
            hashed_itemset_2 = hash_candidate([ancestor, item])
            if hashed_itemset_1 in candidate_hashmap:
                candidate_hashmap.pop(hashed_itemset_1)
            elif hashed_itemset_2 in candidate_hashmap:
                candidate_hashmap.pop(hashed_itemset_2)


def count_candidates_in_transaction(k, expanded_transaction, support_dictionary, candidate_hashmap):
    """
    Given an expanded transaction, increments the count of all candidates in candidates_size_k that are contained in expanded_transaction
    :param k: Number of iteration
    :param expanded_transaction: a list of strings representing a list of items
    :param support_dictionary: { 'hashed_candidate': support_count }
    :param candidate_hashmap: { 'hashed_candidate': candidate }
    :return:
    """
    all_subets = list(map(list, combinations(expanded_transaction, k)))
    for a_subset_size_k in all_subets:
        hashed_subset = hash_candidate(a_subset_size_k)
        if hashed_subset in candidate_hashmap:
            support_dictionary[hashed_subset] += 1


def prune_ancestors(candidates_size_k, taxonomy):
    """
    Delete any ancestors in taxonomy that are not present in any of the candidates in candidates_size_k
    :param candidates_size_k:
    :param taxonomy:
    :return:
    """
    taxonomy_pruned = taxonomy.copy()
    checked = set()
    for item in taxonomy_pruned:
        ancestors = taxonomy_pruned[item]
        for an_ancestor in ancestors:
            if an_ancestor not in checked:
                checked.add(an_ancestor)
                contained_in_a_candidate = an_ancestor in (item for sublist in candidates_size_k for item in sublist)
                if not contained_in_a_candidate:
                    ancestors.remove(an_ancestor)
    return taxonomy_pruned


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
    print('Joining ' + str(n) + ' candidate of size ' + str(k-1) + ' using apriori_gen')
    start = time.time()
    for i in range(n):
        j = i + 1
        while j < n:
            joined_itemset = getValidJoin(frequent_itemset_of_size_k_minus_1[i], frequent_itemset_of_size_k_minus_1[j])
            if joined_itemset is None:
                break
            else:
                candidate_hashmap[hash_candidate(joined_itemset)] = joined_itemset
            j += 1
    end = time.time()
    print('Took ' + (str(end - start) + ' seconds to JOIN ' + str(n) + ' candidates of size ' + str(k-1)))
    # STEP 2: PRUNE -> For each itemset in L_k check if all subsets are frequent
    if k > 2:
        candidates_size_k = list(candidate_hashmap.values()).copy()
        n = len(candidates_size_k)
        print('Pruning ' + str(n) + ' candidates of size ' + str(k) + ' using apriori_gen')
        start = time.time()
        sum_time_cost = 0
        for i in range(n):
            start_loop = time.time()
            subsets_of_size_k_minus_1 = allSubsetofSizeKMinus1(candidates_size_k[i], k - 1)  # candidates_size_k[i] is a list
            for a_subset_of_size_k_minus_1 in subsets_of_size_k_minus_1:
                if not (a_subset_of_size_k_minus_1 in frequent_dictionary[k - 1]):
                    candidate_hashmap.pop(hash_candidate(candidates_size_k[i]), None)  # Prunes the entire candidate
                    break
            end_loop = time.time()
            sum_time_cost += end_loop-start_loop
        end = time.time()
        total_loop_cost = end - start
        if total_loop_cost != 0:
            avg_loop_cost = sum_time_cost / total_loop_cost
        else:
            avg_loop_cost = 0
        print('Took ' + (str(total_loop_cost) + ' seconds to PRUNE ' + str(n) + ' candidates of size ' + str(k)) +
              '.\n Left with ' + str(len(list(candidate_hashmap.keys()))) + ' candidates. Avarage loop time cost: ' + str(avg_loop_cost))
    return candidate_hashmap


def add_candidate_to_hashmap(lst, candidate_hashmap):
    candidate_hashmap[hash_candidate(lst)] = lst

def hash_candidate(candidate):
    """
    Gets a unique string based on candidate list
    :param candidate: a list of ints
    :return:
    """
    return str(candidate)

def rule_generation(frequent_dictionary, support_dictionary, min_confidence):
    rules = []
    for key in frequent_dictionary:
        if key != 1:
            frequent_itemset_k = frequent_dictionary[key]
            for a_itemset_k in frequent_itemset_k:
                for idx, item in enumerate(a_itemset_k):

                    frequent_itemset_copy = a_itemset_k.copy()
                    consequent = [frequent_itemset_copy.pop(idx)]
                    antecedent = frequent_itemset_copy
                    support_antecedent = support_dictionary[hash_candidate(antecedent)]
                    support_all_items = support_dictionary[hash_candidate(a_itemset_k)]
                    confidence = support_all_items / support_antecedent

                    if confidence >= min_confidence:
                        association_rule = AssociationRule(antecedent, consequent, support_all_items, confidence)
                        rules.append(association_rule)
    return rules
