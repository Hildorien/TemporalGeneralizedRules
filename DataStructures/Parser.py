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
        matrix_dictionary = self.fit(dataset).create_matrix_dictionary(dataset)
        return Database(matrix_dictionary, timestamps.to_dict(), self.item_name_by_index, len(dataset))

    def parseBasketFile(self, filepath):
        dataset = []
        with open(filepath) as file:
            lines = file.readlines()
        for line in lines:
            a_transaction = []
            string_split = line.rstrip().split(",")
            for item in string_split:
                a_transaction.append(item)
            dataset.append(a_transaction)
        matrix_dictionary = self.fit(dataset).create_matrix_dictionary(dataset)
        return Database(matrix_dictionary, {}, self.item_name_by_index, len(dataset))

    def parseTaxonomy(self, taxonomy_filepath):
        taxonomy = {}
        with open(taxonomy_filepath) as file:
            lines = file.readlines()[1:] #skips header
        for line in lines:
            a_hierarchy = []
            string_split = line.rstrip().split(",")
            product = string_split[0]
            taxonomy[product] = []
            for i, ancestor in enumerate(string_split):
                if i != 0:
                    taxonomy[product].append(ancestor)
        return taxonomy

    def create_matrix_dictionary(self, dataset):
        matrix_dictionary = {}
        for tid, transaction in enumerate(dataset):

            for item in set(transaction):
                if item in matrix_dictionary:
                    matrix_dictionary[item].append(tid)
                else:
                    matrix_dictionary[item] = [tid]
        return matrix_dictionary

    def fit(self, X):
        self.item_name_by_index = {}
        unique_items = set()
        for transaction in X:
            for item in transaction:
                unique_items.add(item)
        self.item_names = sorted(unique_items)
        self.item_index_by_name = {}
        for col_idx, item in enumerate(self.item_names):
            self.item_index_by_name[item] = col_idx
            self.item_name_by_index[col_idx] = item
        return self
