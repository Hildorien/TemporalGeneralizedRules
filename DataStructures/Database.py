from utility import findOrderedIntersection

class Database:
    sparse_dataframe = None
    timestamp_mapping = {} #{transactionIndex(int) : timestamp(long)}
    items_dic = {} #{itemIndex(int) : itemName(string)}
    row_count = 0

    def __init__(self, var1, var2, var3):
        self.sparse_dataframe = var1
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
            item_valid_indexes = self.sparse_dataframe[self.items_dic[itemColumnIndex]].to_numpy().nonzero()[0]
            if len(final_intersection) == 0:
                final_intersection = item_valid_indexes
            else:
                final_intersection = findOrderedIntersection(final_intersection, item_valid_indexes)

        return len(final_intersection)/self.row_count

    def confidenceOf(self, lhs, rhs):
        """
        :param lhs: a set of items
        :param rhs: a set of one item
        :return: float
        """
        return self.supportOf(lhs) / self.supportOf(rhs)



