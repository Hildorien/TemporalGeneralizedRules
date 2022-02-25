import pandas as pd
from DataStructures.Database import Database
from DataStructures.HorizontalDatabase import HorizontalDatabase
from DataStructures.PTT import PTT
from DataStructures.Transaction import Transaction
from utility import getPeriodStampFromTimestamp


class Parser:

    def parse(self, filepath, csv_format='single', taxonomy_filepath=None, usingTimestamp=False):
        if csv_format == 'basket' and taxonomy_filepath is None:
            return self.parse_basket_file(filepath)
        elif csv_format == 'single' and taxonomy_filepath is None:
            return self.parse_single_file(filepath, usingTimestamp)
        elif csv_format == 'basket' and taxonomy_filepath is not None:
            return self.parse_basket_with_taxonomy(filepath, taxonomy_filepath)
        elif csv_format == 'single' and taxonomy_filepath is not None:
            return self.parse_single_with_taxonomy(filepath, taxonomy_filepath)

    def parse_single_file(self, filepath, usingTimestamp=False):
        dataset, timestamps = self.build_dataset_timestamp_from_file(filepath)
        return self.fit_database(dataset, timestamps.to_dict(), None, usingTimestamp)

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

    def fit_database(self, dataset, timestamps, taxonomy=None, usingTimestamps=False):
        if taxonomy is not None:
            matrix_dictionary_with_tax = self.fit_with_taxonomy(dataset,
                                                                taxonomy).create_matrix_dictionary_with_taxonomy(
                dataset, taxonomy)
            indexed_taxonomy = self.create_indexed_taxonomy(self.item_index_by_name, taxonomy)
            return Database(matrix_dictionary_with_tax, timestamps, self.item_name_by_index,
                            len(dataset), indexed_taxonomy)

        elif usingTimestamps:
            matrix_dictionary_and_ptt = self.fit(dataset).create_matrix_dictionary_using_timestamps(dataset, timestamps)
            return Database(matrix_dictionary_and_ptt["md"], timestamps, self.item_name_by_index,
                            len(dataset), {}, matrix_dictionary_and_ptt["ptt"])
        else:
            matrix_dictionary = self.fit(dataset).create_matrix_dictionary(dataset)["md"]
            return Database(matrix_dictionary, timestamps, self.item_name_by_index,
                            len(dataset), {})

    def parse_taxonomy(self, taxonomy_filepath):
        taxonomy = {}
        with open(taxonomy_filepath) as file:
            lines = file.readlines()[1:]  # skips header
        for line in lines:
            products = line.rstrip().split(",")
            for i, a_product in enumerate(products):
                if a_product not in taxonomy:
                    taxonomy[a_product] = []
                    taxonomy[a_product].extend(products[i + 1:len(products)])
        return taxonomy

    def create_matrix_dictionary(self, dataset):
        matrix_dictionary = {}
        for tid, transaction in enumerate(dataset):
            for item in set(transaction):
                if item in matrix_dictionary:
                    matrix_dictionary[item]['tids'].append(tid)
                else:
                    matrix_dictionary[item] = {}
                    matrix_dictionary[item]['tids'] = [tid]
        return {"md": matrix_dictionary}

    def create_matrix_dictionary_using_timestamps(self, dataset, timestamps):
        matrix_dictionary = {}
        ptt = PTT()
        for tid, transaction in enumerate(dataset):
            transactionHTG = getPeriodStampFromTimestamp(timestamps[tid])
            itemsToAddToPTT = set(map(lambda x: self.item_index_by_name[x], set(transaction)))
            ptt.addItemPeriodToPtt(transactionHTG, itemsToAddToPTT)
            for item in set(transaction):
                if item in matrix_dictionary:
                    matrix_dictionary[item]['tids'].append(tid)
                else:
                    matrix_dictionary[item] = {}
                    matrix_dictionary[item]['tids'] = [tid]
                    matrix_dictionary[item]['fap'] = transactionHTG

        return {"md": matrix_dictionary, "ptt": ptt}

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

    def fit_with_timestamps(self, dataset):
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
        # Add ancestors to unique items
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
            # Expand transaction
            expanded_transaction = []
            for item in transaction:
                expanded_transaction.append(item)
                # Append ancestors of item to expanded_transaction
                ancestors = taxonomy[item]
                for ancestor in ancestors:
                    if ancestor not in expanded_transaction:
                        expanded_transaction.append(ancestor)
            # Work with expanded_transaction
            for item in set(expanded_transaction):
                if item in matrix_dictionary:
                    matrix_dictionary[item]['tids'].append(tid)
                else:
                    matrix_dictionary[item] = {}
                    matrix_dictionary[item]['tids'] = [tid]

        return matrix_dictionary

    def parse_single_with_taxonomy(self, dataset_filepath, taxonomy_filepath):
        dataset, timestamps = self.build_dataset_timestamp_from_file(dataset_filepath)
        taxonomy = self.parse_taxonomy(taxonomy_filepath)
        return self.fit_database(dataset, timestamps.to_dict(), taxonomy)

    def parse_basket_with_taxonomy(self, dataset_filepath, taxonomy_filepath):
        dataset = self.build_dataset_from_basket(dataset_filepath)
        taxonomy = self.parse_taxonomy(taxonomy_filepath)
        return self.fit_database(dataset, {}, taxonomy)

    def fit_horizontal_database(self, dataset, timestamps, taxonomy):
        transactions = []
        unique_items = set()
        items_dic = {}
        index_items_dic = {}
        for a_transaction in dataset:
            for item in a_transaction:
                unique_items.add(item)
        # Add ancestors to unique items
        for key in taxonomy:
            for ancestor in taxonomy[key]:
                unique_items.add(ancestor)
        sorted_items = sorted(unique_items)
        for id_item, item in enumerate(sorted_items):
            items_dic[id_item] = item
            index_items_dic[item] = id_item

        for tid, a_transaction in enumerate(dataset):
            indexed_transaction = []
            for an_item in a_transaction:
                indexed_transaction.append(index_items_dic[an_item])
            transactions.append(Transaction(tid, timestamps.get(tid, 0), sorted(indexed_transaction)))

        indexed_taxonomy = self.create_indexed_taxonomy(index_items_dic, taxonomy)

        return HorizontalDatabase(transactions, indexed_taxonomy, items_dic, index_items_dic)

    def create_indexed_taxonomy(self, index_items_dic, taxonomy):
        indexed_taxonomy = {}
        for an_item in taxonomy:
            ancestors = taxonomy[an_item]
            if an_item in index_items_dic:
                indexed_taxonomy[index_items_dic[an_item]] = []
                for an_ancestor in ancestors:
                    indexed_taxonomy[index_items_dic[an_item]].append(index_items_dic[an_ancestor])
        return indexed_taxonomy

    def parse_single_file_for_horizontal_database(self, dataset_filepath, taxonomy_filepath):
        dataset, timestamps = self.build_dataset_timestamp_from_file(dataset_filepath)
        taxonomy = self.parse_taxonomy(taxonomy_filepath)
        return self.fit_horizontal_database(dataset, timestamps.to_dict(), taxonomy)

    def parse_basket_file_for_horizontal_database(self, dataset_filepath, taxonomy_filepath):
        dataset = self.build_dataset_from_basket(dataset_filepath)
        taxonomy = self.parse_taxonomy(taxonomy_filepath)
        return self.fit_horizontal_database(dataset, {}, taxonomy)

    def parse_horizontal_database(self, dataset_filepath, taxonomy_filepath, csv_format='single'):
        if csv_format == 'single':
            return self.parse_single_file_for_horizontal_database(dataset_filepath, taxonomy_filepath)
        elif csv_format == 'basket':
            return self.parse_basket_file_for_horizontal_database(dataset_filepath, taxonomy_filepath)
