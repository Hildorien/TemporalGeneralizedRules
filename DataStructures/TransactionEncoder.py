import numpy as np
from scipy.sparse import csr_matrix
from sklearn.base import BaseEstimator, TransformerMixin


class TransactionEncoder(BaseEstimator, TransformerMixin):

    #FALTA INCLUIR EL CONCEPTO DE TIMESTAMPS AL CODIFICADOR DE TRANSACCIONES

    def __init__(self):
        return None

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

    def inverse_transform(self, array):
        return [[self.columns_[idx]
                 for idx, cell in enumerate(row) if cell]
                for row in array]

    def fit_transform(self, X):
        return self.fit(X).transform(X)
