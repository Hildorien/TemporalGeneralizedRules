import csv
from .Database import Database
from .HTAR_utility import getPeriodStampFromTimestamp
from .PTT import PTT


class Parser:

    def parse(self, filepath, taxonomy_filepath=None, usingTimestamp=False):
        if taxonomy_filepath is None:
            return self.parse_file(filepath, usingTimestamp)
        else:
            return self.parse_file_with_taxonomy(filepath, taxonomy_filepath, usingTimestamp)

    def parse_file(self, filepath, usingTimestamp=False):
        dataset, timestamps = self.build_dataset_timestamp_from_file(filepath)
        return self.fit_database(dataset, timestamps, None, usingTimestamp)

    def build_dataset_timestamp_from_file(self, filepath):
        header_with_timestamp = {'order_id', 'timestamp', 'product_name'}
        header_without_timestamp = {'order_id', 'product_name'}
        with open(filepath) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            pos = csv_file.tell()
            line = csv_file.readline()
            csv_file.seek(pos)
            firstline = set(map(lambda x: x.strip(), line.split(',')))
            if firstline == header_with_timestamp:
                csv_file.readline()  # skips header
                return self.read_csv_and_build_dataset_timestamps(csv_reader, True)
            elif firstline == header_without_timestamp:
                csv_file.readline()  # skips header
                return self.read_csv_and_build_dataset_timestamps(csv_reader)
            elif len(firstline) == 2:  # No header but we assume format is order_id,product_name
                return self.read_csv_and_build_dataset_timestamps(csv_reader)

    def read_csv_and_build_dataset_timestamps(self, csv_reader, parse_timestamp=False):
        dataset = []  # [[product_name]]
        timestamp = {}  # dict { order_id: timestamp }
        transactions = {}
        for row in csv_reader:
            id = int(row[0])
            if id not in transactions:
                if parse_timestamp:
                    transactions[id] = {'timestamp': int(row[1]), 'products': [row[2]]}
                else:
                    transactions[id] = {'products': [row[1]]}
            else:
                if parse_timestamp:
                    if int(row[1]) != transactions[id]['timestamp']:
                        print('Different timestamps in the same order')
                    transactions[id]['products'].append(row[2])
                else:
                    transactions[id]['products'].append(row[1])
        orders = transactions.keys()
        for idx, order_id in enumerate(orders):
            if parse_timestamp:
                timestamp[idx] = transactions[order_id]['timestamp']
            dataset.append(transactions[order_id]['products'])

        return dataset, timestamp

    def fit_database(self, dataset, timestamps, taxonomy=None, usingTimestamps=False):
        if taxonomy is not None and not usingTimestamps:
            matrix_dictionary_with_tax = self.fit_with_taxonomy(dataset,
                                                                taxonomy).create_matrix_dictionary_with_taxonomy(
                dataset, taxonomy)
            indexed_taxonomy = self.create_indexed_taxonomy(self.item_index_by_name, taxonomy)
            return Database(matrix_dictionary_with_tax, timestamps, self.item_name_by_index,
                            len(dataset), indexed_taxonomy)

        elif usingTimestamps and taxonomy is None:
            matrix_dictionary_and_ptt = self.fit(dataset).create_matrix_dictionary_using_timestamps(dataset, timestamps)
            return Database(matrix_dictionary_and_ptt["md"], timestamps, self.item_name_by_index,
                            len(dataset), {}, matrix_dictionary_and_ptt["ptt"])
        elif usingTimestamps and taxonomy is not None:
            matrix_dictionary_and_ptt = self.fit_with_taxonomy(dataset,
                                                               taxonomy).create_matrix_dictionary_with_taxonomy_and_timestamps(
                dataset, taxonomy, timestamps)
            indexed_taxonomy = self.create_indexed_taxonomy(self.item_index_by_name, taxonomy)
            return Database(matrix_dictionary_and_ptt["md"], timestamps, self.item_name_by_index,
                            len(dataset), indexed_taxonomy, matrix_dictionary_and_ptt["ptt"], self.only_ancestors)
        else:
            matrix_dictionary = self.fit(dataset).create_matrix_dictionary(dataset)["md"]
            return Database(matrix_dictionary, timestamps, self.item_name_by_index,
                            len(dataset), {})

    def parse_taxonomy_single(self, taxonomy_filepath):
        taxonomy = {}
        with open(taxonomy_filepath) as file:  # Learn all close ancestors of taxonomy
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
        while old_lens != new_lens:  # Loop until values don't change
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
        self.only_ancestors = set()
        unique_items = set()
        for transaction in dataset:
            for item in transaction:
                unique_items.add(item)
        # Add ancestors to unique items
        for key in taxonomy:
            for ancestor in taxonomy[key]:
                unique_items.add(ancestor)
                self.only_ancestors.add(ancestor)

        self.item_names = sorted(unique_items)
        self.item_index_by_name = {}
        for col_idx, item in enumerate(self.item_names):
            self.item_index_by_name[item] = col_idx
            self.item_name_by_index[col_idx] = item

        return self

    def fit_with_taxonomy_and_timestamps(self, dataset, taxonomy):
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

    def create_matrix_dictionary_with_taxonomy_and_timestamps(self, dataset, taxonomy, timestamps):
        """
               :param dataset: [['product_name']]
               :param taxonomy: { 'product_name': ['ancestor'] }
               :return:
               """
        matrix_dictionary = {}
        ptt = PTT()
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
            transactionHTGLeafGranule = getPeriodStampFromTimestamp(timestamps[tid])[0]
            itemsToAddToPTT = set(map(lambda x: self.item_index_by_name[x], set(expanded_transaction)))
            ptt.addItemPeriodToPtt(transactionHTGLeafGranule, tid, itemsToAddToPTT)
            for item in set(expanded_transaction):
                if item in matrix_dictionary:
                    matrix_dictionary[item]['tids'].append(tid)
                else:
                    matrix_dictionary[item] = {}
                    matrix_dictionary[item]['tids'] = [tid]

        return {"md": matrix_dictionary, "ptt": ptt}

    def parse_file_with_taxonomy(self, dataset_filepath, taxonomy_filepath, usingTimestamp):
        dataset, timestamps = self.build_dataset_timestamp_from_file(dataset_filepath)
        taxonomy = self.parse_taxonomy_single(taxonomy_filepath)
        return self.fit_database(dataset, timestamps, taxonomy, usingTimestamp)

    def create_indexed_taxonomy(self, index_items_dic, taxonomy):
        indexed_taxonomy = {}
        for an_item in taxonomy:
            ancestors = taxonomy[an_item]
            if an_item in index_items_dic:
                indexed_taxonomy[index_items_dic[an_item]] = []
                for an_ancestor in ancestors:
                    indexed_taxonomy[index_items_dic[an_item]].append(index_items_dic[an_ancestor])
        return indexed_taxonomy
