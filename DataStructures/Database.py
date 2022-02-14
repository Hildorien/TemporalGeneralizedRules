from utility import findOrderedIntersection


class Database:
    timestamp_mapping = {} #{transactionIndex(int) : timestamp(long)}
    items_dic = {} #{itemIndex(int) : itemName(string)}
    row_count = 0


    #{item : {tids:int[], fap:HTG}}
    #tids: Array of transaction id, in order, where the item i appears
    #fap: First Appereance Periods from the transaction where the item i was first discovered
    matrix_data_by_item = {}

    #{ 'product_name' : [ancestor] }
    taxonomy = {}

    def __init__(self, matrix_data_by_item,
                 timestamp_dict, items_dict, row_count, taxonomy_dict):
        self.matrix_data_by_item = matrix_data_by_item
        self.timestamp_mapping = timestamp_dict
        self.items_dic = items_dict
        self.row_count = row_count
        self.taxonomy = taxonomy_dict

    def supportOf(self, itemset):
        """
        :param itemset: set/list of integers
        :return: float
        """
        final_intersection = []
        for itemColumnIndex in itemset:

           item_valid_indexes = self.matrix_data_by_item[self.items_dic[itemColumnIndex]]['tids']

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