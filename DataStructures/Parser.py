import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix

from DataStructures.Transaction import Transaction
from DataStructures.Database import Database

class Parser:
    def __init__(self):
        return None

    @staticmethod
    def parse(filepath):
        """
        Parse csv file into list of Transaction
        :param filepath: csv header must be: | orderid | timestamp | product_name |
        :return: List of Transaction
        """
        dataframe = pd.read_csv(filepath,
                                dtype={'order_id': int, 'timestamp': int, 'product_name': "string"}).dropna()
        orderdict = {}
        for index, row in dataframe.iterrows():
            if row['order_id'] in orderdict:
                orderdict[row['order_id']].addItem(row['product_name'])
            else:
                orderdict[row[0]] = Transaction(index, row['timestamp'], {row['product_name']})
        return list(orderdict.values())

    def parseAndSparse(self, filepath):
        df = pd.read_csv(filepath,
                         dtype={'order_id': int, 'timestamp': int, 'product_name': "string"})
        df['product_name'].replace(',', '.', inplace=True)
        dfG = df.groupby(['order_id', 'timestamp'])['product_name'].apply(lambda x: list(x)).reset_index()
        timestamps = dfG['timestamp']
        dataset = list(dfG['product_name'])
        sparse_matrix = self.fit(dataset).transform(dataset)
        sparse_df = pd.DataFrame.sparse.from_spmatrix(sparse_matrix, columns=self.columns_)
        return Database(sparse_df, timestamps.to_dict())

    def fit(self, X):
        unique_items = set()
        for transaction in X:
            for item in transaction:
                unique_items.add(item)
        self.columns_ = sorted(unique_items)
        columns_mapping = {}
        for col_idx, item in enumerate(self.columns_):
            columns_mapping[item] = col_idx
        self.columns_mapping_ = columns_mapping
        return self

    def transform(self, X):
        indptr = [0]
        indices = []
        for transaction in X:
            for item in set(transaction):
                col_idx = self.columns_mapping_[item]
                indices.append(col_idx)
            indptr.append(len(indices))
        non_sparse_values = [True] * len(indices)
        array = csr_matrix((non_sparse_values, indices, indptr),
                           dtype=bool)
        return array

    def fit_transform(self, X):
        return self.fit(X).transform(X)
