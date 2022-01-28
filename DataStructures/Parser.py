import pandas as pd
from DataStructures.Database import Database

class Parser:

    def parse(self, filepath):
        df = pd.read_csv(filepath,
                         dtype={'order_id': int, 'timestamp': int, 'product_name': "string"})
        df['product_name'].replace(',', '.', inplace=True)
        dfG = df.groupby(['order_id', 'timestamp'])['product_name'].apply(lambda x: list(x)).reset_index()
        timestamps = dfG['timestamp']
        dataset = list(dfG['product_name'])
        sparse_matrix = self.fit(dataset).transform(dataset)
        sparse_df = pd.DataFrame.sparse.from_spmatrix(sparse_matrix, columns=self.columns_)
        return Database(sparse_df, timestamps.to_dict(), self.items_dic)

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
