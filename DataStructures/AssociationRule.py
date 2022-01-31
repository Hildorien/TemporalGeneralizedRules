
class AssociationRule:
    lhs = []
    rhs = []
    support = 0
    confidence = 0

    def __init__(self, lhs, rhs, support, confidence):
        self.lhs = lhs
        self.rhs = rhs
        self.support = support
        self.confidence = confidence

