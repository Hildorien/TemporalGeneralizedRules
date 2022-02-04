import pandas as pd
from DataStructures.Database import Database

class Parser:

    def parse(self, filepath, csv_format='single', taxonomy_filepath=''):
        if csv_format == 'basket' and taxonomy_filepath == '':
            return self.parse_basket_file(filepath)
        elif csv_format == 'single' and taxonomy_filepath == '':
            return self.parse_single_file(filepath)
        elif csv_format == 'basket' and taxonomy_filepath != '':
            return self.parse_basket_with_taxonomy(filepath, taxonomy_filepath)
        elif csv_format == 'single' and taxonomy_filepath != '':
            return self.parse_single_with_taxonomy(filepath, taxonomy_filepath)

    def parse_single_file(self, filepath):
        dataset, timestamps = self.build_dataset_timestamp_from_file(filepath)
        return self.fit_database(dataset, timestamps.to_dict())

    def build_dataset_timestamp_from_file(self, filepath):
        df = pd.read_csv(filepath,
                         dtype={'order_id': int, 'timestamp': int, 'product_name': "string"})
        df['product_name'].replace(',', '.', inplace=True)
        dfG = df.groupby(['order_id', 'timestamp'])['product_name'].apply(lambda x: list(x)).reset_index()
        timestamps = dfG['timestamp']
        dataset = list(dfG['product_name'])
        return (dataset, timestamps)

    def parse_basket_file(self, filepath):
        return self.fit_database(self.build_dataset_from_basket(filepath), {})

    def build_dataset_from_basket(self, filepath):
        dataset = []
        with open(filepath) as file:
            lines = file.readlines()
        for line in lines:
            a_transaction = []
            string_split = line.rstrip().split(",")
            for item in string_split:
                a_transaction.append(item)
            dataset.append(a_transaction)
        return dataset

    def fit_database(self, dataset, timestamps, taxonomy=None):
        if taxonomy is None:
            matrix_dictionary = self.fit(dataset).create_matrix_dictionary(dataset)
            return Database(matrix_dictionary, timestamps, self.item_name_by_index,
                            len(dataset), {})
        else:
            matrix_dictionary_with_tax = self.fit_with_taxonomy(dataset, taxonomy).create_matrix_dictionary_with_taxonomy(dataset, taxonomy)
            return Database(matrix_dictionary_with_tax, timestamps, self.item_name_by_index,
                            len(dataset), taxonomy)

    def parse_taxonomy(self, taxonomy_filepath):
        taxonomy = {}
        with open(taxonomy_filepath) as file:
            lines = file.readlines()[1:] #skips header
        for line in lines:
            a_hierarchy = []
            string_split = line.rstrip().split(",")
            product = string_split[0]
            taxonomy[product] = []
            for i, ancestor in enumerate(string_split):
                if i != 0: #append only ancestors
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

    def fit(self, dataset):
        self.item_name_by_index = {}
        unique_items = set()
        for transaction in dataset:
            for item in transaction:
                unique_items.add(item)
        self.item_names = sorted(unique_items)
        self.item_index_by_name = {}
        for col_idx, item in enumerate(self.item_names):
            self.item_index_by_name[item] = col_idx
            self.item_name_by_index[col_idx] = item
        return self

    def fit_with_taxonomy(self, dataset, taxonomy):
        """
        :param dataset: [['product_name']]
        :param taxonomy: { 'product_name': ['ancestor'] }
        :return:
        """
        self.item_name_by_index = {}
        unique_items = set()
        for transaction in dataset:
            for item in transaction:
                unique_items.add(item)
        #Add ancestors to unique items
        for key in taxonomy:
            for ancestor in taxonomy[key]:
                unique_items.add(ancestor)

        self.item_names = sorted(unique_items)
        self.item_index_by_name = {}
        for col_idx, item in enumerate(self.item_names):
            self.item_index_by_name[item] = col_idx
            self.item_name_by_index[col_idx] = item
        return self

    def create_matrix_dictionary_with_taxonomy(self, dataset, taxonomy):
        """
        :param dataset: [['product_name']]
        :param taxonomy: { 'product_name': ['ancestor'] }
        :return:
        """
        matrix_dictionary = {}
        for tid, transaction in enumerate(dataset):
            #Expand transaction
            expanded_transaction = []
            for item in transaction:
                expanded_transaction.append(item)
                #Append ancestors of item to expanded_transaction
                ancestors = taxonomy[item]
                for ancestor in ancestors:
                    if ancestor not in expanded_transaction:
                        expanded_transaction.append(ancestor)
            #Work with expanded_transaction
            for item in set(expanded_transaction):
                if item in matrix_dictionary:
                    matrix_dictionary[item].append(tid)
                else:
                    matrix_dictionary[item] = [tid]

        return matrix_dictionary

    def parse_single_with_taxonomy(self, dataset_filepath, taxonomy_filepath):
        dataset, timestamps = self.build_dataset_timestamp_from_file(dataset_filepath)
        taxonomy = self.parse_taxonomy(taxonomy_filepath)
        return self.fit_database(dataset, timestamps.to_dict(), taxonomy)

    def parse_basket_with_taxonomy(self, dataset_filepath, taxonomy_filepath):
        dataset = self.build_dataset_from_basket(dataset_filepath)
        taxonomy = self.parse_taxonomy(taxonomy_filepath)
        return self.fit_database(dataset, {}, taxonomy)
