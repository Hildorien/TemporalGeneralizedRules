import pandas as pd
import DataStructures.Parser
from scipy.sparse import csr_matrix

class Database:
    sparse_dataframe = None
    timestamp_mapping = {}
    row_count = 0

    def __init__(self, var1, var2):
        self.sparse_dataframe = var1
        self.timestamp_mapping = var2
        self.row_count = len(self.sparse_dataframe.index)

    def supportOf(self, itemset):
        """
        :param itemset: set of integers
        :return:
        """

        list_of_list = [[]]
        for item in itemset:
            list_of_list.append(list(self.sparse_dataframe.iloc[:, item]))

            #Calculo la intersecion de dos listas
            #set_2 = frozenset(list_2)
            #intersection = [x for x in list_1 if x in set_2]




