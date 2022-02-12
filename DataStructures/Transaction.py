class Transaction:
    id = 0
    timestamp = 0
    items = {}

    def __init__(self, id, timestamp, items):
        self.id = id
        self.timestamp = timestamp
        self.items = items
