import numpy as np
import pandas as pd
from DataStructures.Database import Database
from DataStructures.HorizontalDatabase import HorizontalDatabase
from HTAR.HTAR_utility import getPeriodStampFromTimestamp, getPeriodStampFromTimestampHONG
from HTAR.PTT import PTT
from DataStructures.Transaction import Transaction

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

    #DELETE WHEN FINISH TESTING FOR HONG
    def parseHONG(self, filepath, csv_format='single', usingTimestamp = True):
        #self.parse_single_file(filepath, usingTimestamp)
        dataset, timestamps = self.build_dataset_timestamp_from_file(filepath)
        matrix_dictionary_and_ptt = self.fit(dataset).create_matrix_dictionary_using_timestamps_HONG(dataset, timestamps)
        return Database(matrix_dictionary_and_ptt["md"], timestamps, self.item_name_by_index,
                        len(dataset), {}, matrix_dictionary_and_ptt["ptt"])

    def parse_single_file(self, filepath, usingTimestamp=False):
        dataset, timestamps = self.build_dataset_timestamp_from_file(filepath)
        return self.fit_database(dataset, timestamps, None, usingTimestamp)

    def build_dataset_timestamp_from_file(self, filepath):
        header_with_timestamp = {'order_id', 'timestamp', 'product_name'}
        header_without_timestamp = {'order_id', 'product_name'}
        with open(filepath) as f:
            firstline = set(f.readline().rstrip().split(','))

        if firstline == header_with_timestamp:
            df = pd.read_csv(filepath, dtype={'order_id': int, 'timestamp': int, 'product_name': "string"})
            df = df.sort_values(by=['timestamp', 'order_id'])
            df['product_name'].replace(',', '.', inplace=True)
            dfG = df.groupby(['timestamp', 'order_id'])['product_name'].apply(lambda x: list(x)).reset_index()
            timestamps = dfG['timestamp']
            dataset = list(dfG['product_name'])
            return dataset, timestamps
        elif firstline == header_without_timestamp:
            df = pd.read_csv(filepath, dtype={'order_id': int, 'product_name': "string"})
            df['product_name'].replace(',', '.', inplace=True)
            dataset, timestamps =self.groupby_multikey_dataframe_fast(df, ['order_id'])
            return dataset, timestamps
        elif len(firstline) == 2:   # No header but we assume format is order_id,product_name
            df = pd.read_csv(filepath, names=['order_id', 'product_name'], header=None,
                             dtype={'order_id': int, 'product_name': "string"})
            df['product_name'].replace(',', '.', inplace=True)
            dataset, timestamps =self.groupby_multikey_dataframe_fast(df, ['order_id'])
            return dataset, timestamps.to_dict()

    def groupby_multikey_dataframe_fast(self, df, key_cols):
        #pandas.groupby is slow.
        #Explanation: https://stackoverflow.com/questions/60028836/combination-of-columns-for-aggregation-after-groupby/60029108#60029108
        #Performance can be improved using numpy arrays https://stackoverflow.com/a/56525582
        if not isinstance(key_cols, list):
            key_cols = [key_cols]

        values = df.sort_values(key_cols).values.T

        col_idcs = [df.columns.get_loc(cn) for cn in key_cols]
        other_col_names = [name for idx, name in enumerate(df.columns) if idx not in col_idcs]
        other_col_idcs = [df.columns.get_loc(cn) for cn in other_col_names]

        # split df into indexing colums(=keys) and data colums(=vals)
        keys = values[col_idcs, :]
        vals = values[other_col_idcs, :]

        # list of tuple of key pairs
        multikeys = list(zip(*keys))
        # remember unique key pairs and ther indices
        ukeys, index = np.unique(multikeys, return_index=True, axis=0)

        # split data columns according to those indices
        arrays = np.split(vals, index[1:], axis=1)

        # resulting list of subarrays has same number of subarrays as unique key pairs
        # each subarray has the following shape:
        #    rows = number of non-grouped data columns
        #    cols = number of data points grouped into that unique key pair

        # prepare multi index
        idx = pd.MultiIndex.from_arrays(ukeys.T, names=key_cols)

        list_agg_vals = dict()
        for tup in zip(*arrays, other_col_names):
            col_vals = tup[:-1]  # first entries are the subarrays from above
            col_name = tup[-1]  # last entry is data-column name

            list_agg_vals[col_name] = col_vals


        #df2 = pd.DataFrame(data=list_agg_vals, index=idx)
        if len(key_cols) > 1:
            timestamps = pd.Series(data=list(dict.fromkeys(list(keys[1, :]))))
        else:
            timestamps = pd.DataFrame(columns=['timestamp'])  # Empty pandas series -> no timestamps used
        dataset = [x.tolist() for x in list(list(list_agg_vals.values())[0])]
        return dataset, timestamps

    def build_dataset_timestamp_from_file_without_pandas(self, filepath):
        header_with_timestamp = {'order_id', 'timestamp', 'product_name'}
        header_without_timestamp = {'order_id', 'product_name'}
        dataset = [] # [[product_name]]
        timestamp = {} # dict { order_id: timestamp }
        with open(filepath) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            pos = csv_file.tell()
            line = csv_file.readline()
            csv_file.seek(pos)
            firstline = set(line.rstrip().split(','))
            if firstline == header_with_timestamp:
                csv_file.readline() # skips header
                pos = csv_file.tell()
                line = csv_file.readline()
                csv_file.seek(pos)
                csv_order_id = int(line[0])
                csv_last_order_id = int(line[0])
                tx_id = 0
                order = []
                for row in csv_reader:

                    csv_last_order_id = csv_order_id
                    csv_order_id = int(row[0])

                    if csv_last_order_id == csv_order_id:
                        order.append(row[2])
                        timestamp.setdefault(tx_id, int(row[1]))
                    else:
                        dataset.append(order) # append last order
                        order = [] # start a new one
                        order.append(row[2])
                        timestamp.setdefault(tx_id, int(row[1]))
                        tx_id += 1

            elif firstline == header_without_timestamp:
                csv_file.readline()  # skips header
                pos = csv_file.tell()
                line = csv_file.readline()
                csv_file.seek(pos)
                csv_order_id = int(line[0])
                csv_last_order_id = int(line[0])
                tx_id = 0
                order = []
                for row in csv_reader:

                    csv_last_order_id = csv_order_id
                    csv_order_id = int(row[0])

                    if csv_last_order_id == csv_order_id:
                        order.append(row[1])
                    else:
                        dataset.append(order)  # append last order
                        order = []  # start a new one
                        order.append(row[1])
                        tx_id += 1

            elif len(firstline) == 2:  # No header but we assume format is order_id,product_name
                pos = csv_file.tell()
                line = csv_file.readline()
                csv_file.seek(pos)
                csv_order_id = int(line[0])
                csv_last_order_id = int(line[0])
                tx_id = 0
                order = []
                for row in csv_reader:

                    csv_last_order_id = csv_order_id
                    csv_order_id = int(row[0])

                    if csv_last_order_id == csv_order_id:
                        order.append(row[1])
                    else:
                        dataset.append(order)  # append last order
                        order = []  # start a new one
                        order.append(row[1])
                        tx_id += 1
            return dataset, timestamp

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

    def parse_taxonomy_basket(self, taxonomy_filepath):
        taxonomy = {}
        with open(taxonomy_filepath) as file:
            lines = file.readlines()
        for line in lines:
            products = line.rstrip().split(",")
            for i, a_product in enumerate(products):
                if a_product not in taxonomy:
                    taxonomy[a_product] = []
                    taxonomy[a_product].extend(products[i + 1:len(products)])
        return taxonomy

    def parse_taxonomy_single(self, taxonomy_filepath):
        taxonomy = {}
        with open(taxonomy_filepath) as file: #Learn all close ancestors of taxonomy
            lines = file.readlines()
            for line in lines:
                products = line.rstrip().split(",")
                child = products[0]
                parent = products[1]
                if child != parent:
                    if parent not in taxonomy:
                        taxonomy[parent] = []
                    if child not in taxonomy:
                        taxonomy[child] = []
                    taxonomy[child].append(parent)

        old_lens = [len(x) for x in list(taxonomy.values())]
        new_lens = []
        while old_lens != new_lens: #Loop until values don't change
            for key in taxonomy:
                ancestors = taxonomy[key]
                if ancestors:
                    last_ancestor = ancestors[-1]
                    taxonomy[key].extend(taxonomy[last_ancestor])
                ancestors_without_dups = list(dict.fromkeys(taxonomy[key]))
                taxonomy[key] = ancestors_without_dups

            old_lens = new_lens
            new_lens = [len(x) for x in list(taxonomy.values())]
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
            transactionHTGLeafGranule = getPeriodStampFromTimestamp(timestamps[tid])[0]
            itemsToAddToPTT = set(map(lambda x: self.item_index_by_name[x], set(transaction)))
            ptt.addItemPeriodToPtt(transactionHTGLeafGranule, tid, itemsToAddToPTT)
            for item in set(transaction):
                if item in matrix_dictionary:
                    matrix_dictionary[item]['tids'].append(tid)
                else:
                    matrix_dictionary[item] = {}
                    matrix_dictionary[item]['tids'] = [tid]

        return {"md": matrix_dictionary, "ptt": ptt}


    #DELETE WHEN FINISH TESTING
    def create_matrix_dictionary_using_timestamps_HONG(self, dataset, timestamps):
        matrix_dictionary = {}
        ptt = PTT(True)
        for tid, transaction in enumerate(dataset):
            transactionHTGLeafGranule = getPeriodStampFromTimestampHONG(timestamps[tid])[0]
            itemsToAddToPTT = set(map(lambda x: self.item_index_by_name[x], set(transaction)))
            ptt.addItemPeriodToPtt(transactionHTGLeafGranule, tid, itemsToAddToPTT)
            for item in set(transaction):
                if item in matrix_dictionary:
                    matrix_dictionary[item]['tids'].append(tid)
                else:
                    matrix_dictionary[item] = {}
                    matrix_dictionary[item]['tids'] = [tid]

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
        taxonomy = self.parse_taxonomy_single(taxonomy_filepath)
        return self.fit_database(dataset, timestamps, taxonomy)

    def parse_basket_with_taxonomy(self, dataset_filepath, taxonomy_filepath):
        dataset = self.build_dataset_from_basket(dataset_filepath)
        taxonomy = self.parse_taxonomy_basket(taxonomy_filepath)
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
        taxonomy = self.parse_taxonomy_single(taxonomy_filepath)
        return self.fit_horizontal_database(dataset, timestamps, taxonomy)

    def parse_basket_file_for_horizontal_database(self, dataset_filepath, taxonomy_filepath):
        dataset = self.build_dataset_from_basket(dataset_filepath)
        taxonomy = self.parse_taxonomy_basket(taxonomy_filepath)
        return self.fit_horizontal_database(dataset, {}, taxonomy)

    def parse_horizontal_database(self, dataset_filepath, taxonomy_filepath, csv_format='single'):
        if csv_format == 'single':
            return self.parse_single_file_for_horizontal_database(dataset_filepath, taxonomy_filepath)
        elif csv_format == 'basket':
            return self.parse_basket_file_for_horizontal_database(dataset_filepath, taxonomy_filepath)
