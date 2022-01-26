from itertools import combinations


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
    # STOP

