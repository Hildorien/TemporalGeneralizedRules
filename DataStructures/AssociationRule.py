
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

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, AssociationRule):
            return self.lhs == other.lhs and self.rhs == other.rhs \
                   and self.support == other.support and self.confidence == other.confidence
        return False