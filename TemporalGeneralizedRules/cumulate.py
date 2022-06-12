import multiprocessing
from .cumulate_utility import prune_candidates_in_same_family
from .rule_generator import rule_generation
from .utility import hash_candidate, generateCanidadtesOfSizeK, append_tids, calculate_support_for_parallel


def _vertical_cumulate(vertical_database, min_supp, min_conf, min_r, parallel_count=False, parallel_rule_gen=False):
    """
    :param vertical_database: A Database instance
    :param min_supp: User-defined minimum support
    :param min_conf: User-defined minimum confidence
    :param min_r: User-defined minimum R interesting measure
    :param parallel_count: Optional parameter to set parallel counting for supports
    :param parallel_rule_gen: Optional parameter to set parallel rule gen
    :return: Set of association rules
    """
    frequent_dictionary, support_dictionary, taxonomy = vertical_cumulate_frequents(vertical_database, min_supp,
                                                                                    parallel_count)
    rules = rule_generation(frequent_dictionary, support_dictionary, min_conf, parallel_rule_gen, min_r,
                            vertical_database)
    return rules


def vertical_cumulate_frequents(vertical_database, min_supp, parallel_count=False):
    # Instantiate local variables
    frequent_dictionary = {}
    support_dictionary = {}
    k = 1
    taxonomy = vertical_database.taxonomy  # Cumulate Optimization 2 in original paper
    all_items = sorted(list(vertical_database.items_dic.keys()))
    pool = None
    total_transactions = vertical_database.tx_count
    items_dic = vertical_database.items_dic
    matrix_data_by_item = vertical_database.matrix_data_by_item
    if parallel_count:
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
    while k == 1 or frequent_dictionary[k - 1] != []:
        # Candidate Generation
        candidates_size_k = generateCanidadtesOfSizeK(k, all_items, frequent_dictionary)
        if k == 2:
            # Cumulate Optimization 3 in original paper
            candidates_size_k = prune_candidates_in_same_family(candidates_size_k, vertical_database.ancestor_set)
        frequent_dictionary[k] = []
        if len(candidates_size_k) == 0:
            break
        if parallel_count:
            list_to_parallel = [append_tids(x, items_dic, matrix_data_by_item) for x in candidates_size_k]
            results = pool.starmap(calculate_support_for_parallel, list_to_parallel)
            for a_result in results:
                candidate_support = a_result[1] / total_transactions
                support_dictionary[hash_candidate(a_result[0])] = candidate_support
                if candidate_support >= min_supp:
                    frequent_dictionary[k].append(a_result[0])
        else:
            for a_candidate_size_k in candidates_size_k:
                support = vertical_database.supportOf(a_candidate_size_k)
                support_dictionary[hash_candidate(a_candidate_size_k)] = support
                if support >= min_supp:
                    frequent_dictionary[k].append(a_candidate_size_k)
        k += 1

    return frequent_dictionary, support_dictionary, taxonomy

