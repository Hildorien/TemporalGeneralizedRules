class HorizontalDatabase:
    transactions = []
    taxonomy = {}
    transaction_count = 0
    items_dic = {}
    index_items_dic = {}

    def __init__(self, transactions, taxonomy, items_dic, index_items_dic):
        self.transactions = transactions
        self.taxonomy = taxonomy
        self.transaction_count = len(transactions)
        self.items_dic = items_dic
        self.index_items_dic = index_items_dic

    def printAssociationRule(self, association_rule):
        consequent_name = self.items_dic[association_rule.rhs[0]]
        antecedent_names = ','.join(list(map(lambda x: self.items_dic[x], association_rule.lhs)))
        return '{' + antecedent_names + "} => " + '{' + consequent_name + "} , support: " + str(association_rule.support) + " ,confidence: " + str(association_rule.confidence)
