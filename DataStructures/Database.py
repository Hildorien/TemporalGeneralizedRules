from utility import findOrderedIntersection


class Database:
    timestamp_mapping = {} #{transactionIndex(int) : timestamp(long)}
    items_dic = {} #{itemIndex(int) : itemName(string)}
    row_count = 0

    #{product : transaccions_where_it_appears(int[])}
    matrix_data_by_item = {}

    def __init__(self, var1, var2, var3, var4):
        self.matrix_data_by_item = var1
        self.timestamp_mapping = var2
        self.items_dic = var3
        self.row_count = var4

    def supportOf(self, itemset):
        """
        :param itemset: set/list of integers
        :return: float
        """
        final_intersection = []
        for itemColumnIndex in itemset:

           item_valid_indexes = self.matrix_data_by_item[self.items_dic[itemColumnIndex]]

           if len(final_intersection) == 0:
                final_intersection = item_valid_indexes
           else:
                final_intersection = findOrderedIntersection(final_intersection, item_valid_indexes)

        return len(final_intersection)/self.row_count

    def confidenceOf(self, lhs, rhs):
        """
        :param lhs: a list of items
        :param rhs: a list of one item
        :return: float
        """
        return self.supportOf(lhs) / self.supportOf(rhs)

    def printAssociationRule(self, association_rule):
        consequent_name = self.items_dic[association_rule.rhs[0]]
        antecedent_names = ','.join(list(map(lambda x: self.items_dic[x], association_rule.lhs)))
        return antecedent_names + " => " + consequent_name + " support: " + str(association_rule.support) + " confidence: " + str(association_rule.confidence)