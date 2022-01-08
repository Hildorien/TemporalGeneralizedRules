import pandas as pd

from DataStructures.Transaction import Transaction


class Parser:
    @staticmethod
    def parse(filepath):
        """
        Parse csv file into list of Transaction
        :param filepath: csv header must be: | OrderID | Timestamp | Product Name |
        :return: List of Transaction
        """
        dataframe = pd.read_csv(filepath).dropna()
        orderdict = {}
        for index, row in dataframe.iterrows():
            if row[0] in orderdict:
                orderdict[row[0]].addItem(row[2])
            else:
                orderdict[row[0]] = Transaction(index, row[1], {row[2]})
        return list(orderdict.values())
