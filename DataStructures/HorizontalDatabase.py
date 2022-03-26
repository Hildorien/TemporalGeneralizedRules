class HorizontalDatabase:
    transactions = []
    taxonomy = {}
    transaction_count = 0
    items_dic = {}
    index_items_dic = {}
    is_horizontal = True

    def __init__(self, transactions, taxonomy, items_dic, index_items_dic):
        self.transactions = transactions
        self.taxonomy = taxonomy
        self.transaction_count = len(transactions)
        self.items_dic = items_dic
        self.index_items_dic = index_items_dic

    def printAssociationRule(self, association_rule):
        consequent_name = self.items_dic[association_rule.rhs[0]]
        antecedent_names = ','.join(list(map(lambda x: self.items_dic[x], association_rule.lhs)))
        return '{' + antecedent_names + "} => " + '{' + consequent_name + "} , support: " + str(
            association_rule.support) + " ,confidence: " + str(association_rule.confidence)

    def expand_transaction(self, a_transaction, taxonomy):
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

    def supportOf(self, itemset, l_level=None, period=None):
        count = 0
        taxonomy_pruned = self.prune_ancestors([itemset])
        for a_transaction in self.transactions:
            expanded_transaction = self.expand_transaction(a_transaction, taxonomy_pruned)
            if set(itemset).issubset(set(expanded_transaction)):
                count += 1
        return count / len(self.transactions)

    def prune_ancestors(self, candidates_size_k):
        """
        Delete any ancestors in taxonomy that are not present in any of the candidates in candidates_size_k
        :param candidates_size_k:
        :param taxonomy:
        :return:
        """
        taxonomy_pruned = self.taxonomy.copy()
        checked = set()
        # Check in linear time if an ancestor is contained in a candidate
        setOfCandidates = set(map(tuple, candidates_size_k))
        for item in taxonomy_pruned:
            ancestors = taxonomy_pruned[item]
            for an_ancestor in ancestors:
                if an_ancestor not in checked:
                    checked.add(an_ancestor)
                    contained_in_a_candidate = False
                    for a_candidate in setOfCandidates:
                        if an_ancestor in a_candidate:
                            contained_in_a_candidate = True
                            break
                    if not contained_in_a_candidate:
                        ancestors.remove(an_ancestor)
        return taxonomy_pruned
