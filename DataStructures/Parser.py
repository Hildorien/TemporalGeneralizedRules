import pandas as pd

from DataStructures.Transaction import Transaction


class Parser:
    @staticmethod
    def parse(filepath):
        """
        Parse csv file into list of Transaction
        :param filepath: csv header must be: | orderid | timestamp | product_name |
        :return: List of Transaction
        """
        dataframe = pd.read_csv(filepath,
                                dtype={'order_id': int, 'timestamp': "string", 'product_name': "string"}).dropna()
        orderdict = {}
        for index, row in dataframe.iterrows():
            if row['order_id'] in orderdict:
                orderdict[row['order_id']].addItem(row['product_name'])
            else:
                orderdict[row[0]] = Transaction(index, row['timestamp'], {row['product_name']})
        return list(orderdict.values())
