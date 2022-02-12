class HorizontalDatabase:
    transactions = []
    taxonomy = {}
    transaction_count = 0
    items_dic = {}
    index_items_dic = {}

    def __init__(self, transactions, taxonomy, items_dic, index_items_dic):
        self.transactions = transactions
        self.taxonomy = taxonomy
        self.transaction_count = len(transactions)
        self.items_dic = items_dic
        self.index_items_dic = index_items_dic
