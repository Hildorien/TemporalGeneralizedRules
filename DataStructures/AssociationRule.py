class AssociationRule:
    lhs = {}
    rhs = {}

    def __init__(self, lhs, rhs):
        self.lhs = lhs
        self.rhs = rhs

    def print(self):
        return str(self.lhs) + " => " + str(self.rhs)
