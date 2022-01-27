from itertools import combinations

def apriori_gen(frequent_itemset_of_size_k_minus_1, frequent_dictionary):
    """
    :param frequent_itemset: An ORDERED set/list of itemsets in which each itemset is also ordered
    :param frequent_dictionary: Auxiliary structure for fast lookup of frequents of size k-1
    :return:
    """
    k = len(frequent_itemset_of_size_k_minus_1[0])+1
    #STEP 1: JOIN
    candidates_of_size_k = []
    n = len(frequent_itemset_of_size_k_minus_1)
    for i in range(n):
        j = i+1
        while j < n-1:
            joined_itemset = getValidJoin(frequent_itemset_of_size_k_minus_1[i], frequent_itemset_of_size_k_minus_1[j])
            if joined_itemset is None:
                break
            else:
                candidates_of_size_k.append(joined_itemset)
            j += 1
    #STEP 2: PRUNE -> For each itemset in L_k check if all subsets are frequent
    for a_candidate_of_size_k in candidates_of_size_k: #Iterating over a lists of lists
        subsets_of_size_k_minus_1 = allSubsetofSizeKMinus1(a_candidate_of_size_k, k-1) #a_candidate_of_size_k is a list
        for a_subset_of_size_k_minus_1 in subsets_of_size_k_minus_1:
            if not (joinlistOfInts(a_subset_of_size_k_minus_1) in frequent_dictionary[k-1]):
                candidates_of_size_k.remove(a_candidate_of_size_k) #Prunes the entire candidate
                break

    return candidates_of_size_k


def apriori(database, min_support, min_confidence):
    """
    :param database:
    :param min_support:
    :param min_confidence:
    :return: a set of AssociationRules
    """

    frequent_size_1 = database.items_dic.keys()
    # Pre-prune frequents_size_1 where support does not meet the min_support
    frequent_size_1 = set(filter(lambda x: database.supportOf({x}) > min_support, frequent_size_1))

    k = 1
    frecuent_itemsets_by_k = {k: frequent_size_1}
    while len(frecuent_itemsets_by_k[k]) == 0 or k != len(frequent_size_1):
        k += 1
        pre_pruned_itemset_tuples = set() #abcd, acde
        #  Uno de a tuplas las posibles combinaciones entre los conjuntos de tamaño k-1
        k_frecuent_itemset = set()
        for tuple in list(combinations(frecuent_itemsets_by_k[k - 1], 2)):
            if k == 2:
                #k=2 does not need pruning and has direct int instead of tuples/sets
                if database.supportOf(set(tuple)) >= min_support:
                    k_frecuent_itemset.add(set(tuple))
            else:
                auxiliar_set = set()
                # Por cada tupla generada anteriormente, "rompo+pongo en set+saco combinatoria tamaño k" e inserto en set de conjuntos candidatos
                auxiliar_set = set.union(auxiliar_set,set(tuple[0]))
                auxiliar_set = set.union(auxiliar_set, set(tuple[1]))

                 # agrego al itemset candidatos pre-pruneados de tamaño K (ItemsetCandidatos tamaño K)
                for combo in list(combinations(auxiliar_set, k)):
                    pre_pruned_itemset_tuples.add(combo)

        # Pruneo: Itero los items del set de conjuntos candidatos
            if k != 2:
                items_candidate_and_pruned = set()
                for pre_pruned_item in pre_pruned_itemset_tuples:
                    # Por cada uno, busco las combinaciones k-1 de ese candidato
                    for kminus1item in list(combinations(pre_pruned_item, k-1)):
                        # Por cada uno, chequeo si está en los frecuentes de tamaño K-1
                        if set(kminus1item) not in k_frecuent_itemset[k-1]:
                            # Si no: no (pruneado x principio de monotonia)
                            break
                    #agregar diccionario para no volver a chequear lo ya chequeado
                    # Si está: entonces lo agrego al itemset candidato de tamaño K
                    items_candidate_and_pruned.add(set(pre_pruned_item))
                for itemset in items_candidate_and_pruned:
                    if database.supportOf(itemset) >= min_support:
                        k_frecuent_itemset.add(itemset)

    # ITEMSET_FRECUENTE_TAMAÑO_K = Filtrar conjunto de itemsetCandidatos por minSupport
        frecuent_itemsets_by_k[k] = k_frecuent_itemset

    # Si K != len(frequent_size_1) o len(ITEMSET_FRECUENTE_TAMAÑO_K) = 0:

def apriori2(database, min_support, min_confidence):
    """
    :param database:
    :param min_support:
    :param min_confidence:
    :return: a set of AssociationRules
    """
    all_items = sorted(list(database.items_dic.keys()))
    # Pre-prune frequents_size_1 where support does not meet the min_support
    frequent_size_1 = list(filter(lambda x: database.supportOf({x}) > min_support, all_items))
    print(len(frequent_size_1))
    candidates_size_2 = list(map(list, combinations(frequent_size_1, 2)))
    print(len(candidates_size_2))
    #frequent_size_2 = list(filter(lambda x: database.supportOf(x) > min_support, candidates_size_2))


