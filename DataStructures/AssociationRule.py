class AssociationRule:
    lhs = {}
    rhs = {}
    support = 0
    confidence = 0

    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def print(self):
        return str(self.lhs) + " => " + str(self.rhs) + " support: " + str(self.support) + " confidence: " + str(self.confidence)
